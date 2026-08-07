/* Copyright 2022 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Methods for tokenizing and parsing SQL statements. They are tokenized and parsed using
//! [sqlparser] as it is already used by Apache DataFusion. Only public functions return
//! [`ModelarDbStorageError`] to simplify use of traits from [sqlparser] and Apache DataFusion.
//!
//! [sqlparser]: https://crates.io/crates/sqlparser

use std::collections::HashMap;
use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::datatypes::TimeUnit;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use datafusion::common::{DFSchema, DataFusionError, ToDFSchema};
use datafusion::config::ConfigOptions;
use datafusion::execution::context::ExecutionProps;
use datafusion::functions;
use datafusion::logical_expr::{AggregateUDF, Expr as DFExpr, ScalarUDF, TableSource, WindowUDF};
use datafusion::physical_expr::planner;
use datafusion::sql::TableReference;
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use modelardb_types::functions::normalize_name; // Fully imported to not conflict.
use modelardb_types::types::{
    ArrowTimestamp, ArrowValue, ErrorBound, GeneratedColumn, TimeSeriesTableMetadata,
};
use sqlparser::ast::{
    CascadeOption, ColumnDef, ColumnOption, ColumnOptionDef, CreateTable, DataType as SQLDataType,
    Expr, GeneratedAs, HiveDistributionStyle, HiveFormat, Ident, ObjectName, ObjectNamePart,
    ObjectType, Query, Setting, Statement, TableEngine, TimezoneInfo, TruncateIdentityOption,
    TruncateTableTarget, Value,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::keywords::{ALL_KEYWORDS, Keyword};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Span, Token};

use crate::error::{ModelarDbStorageError, Result};

/// A top-level statement (CREATE, INSERT, SELECT, TRUNCATE, DROP, VACUUM etc.) that has been
/// tokenized, parsed, and for which semantic checks have verified that it is compatible with
/// ModelarDB.
#[derive(Debug)]
pub enum ModelarDbStatement {
    /// CREATE TABLE.
    CreateNormalTable { name: String, schema: Schema },
    /// CREATE TIME SERIES TABLE.
    CreateTimeSeriesTable(Arc<TimeSeriesTableMetadata>),
    /// INSERT, EXPLAIN, SELECT.
    Statement(Statement),
    /// INCLUDE addresses SELECT.
    IncludeSelect(Statement, Vec<String>),
    /// DROP TABLE.
    DropTable(Vec<String>),
    /// TRUNCATE TABLE.
    TruncateTable(Vec<String>),
    /// VACUUM.
    Vacuum(Vec<String>),
}

/// Tokenizes and parses the SQL statement in `sql` and returns its parsed representation in the form
/// of a [`ModelarDbStatement`]. Returns a [`ModelarDbStorageError`] if `sql` is empty, contains
/// multiple statements, or the statement is unsupported. Currently, CREATE TABLE, CREATE TIME SERIES
/// TABLE, INSERT, EXPLAIN, INCLUDE, SELECT, TRUNCATE TABLE, DROP TABLE, and VACUUM are supported.
pub fn tokenize_and_parse_sql_statement(sql_statement: &str) -> Result<ModelarDbStatement> {
    let mut statements = Parser::parse_sql(&ModelarDbDialect::new(), sql_statement)?;

    // Check that the sql contained a parseable statement.
    if statements.is_empty() {
        Err(ModelarDbStorageError::InvalidArgument(
            "An empty string cannot be tokenized and parsed.".to_owned(),
        ))
    } else if statements.len() > 1 {
        Err(ModelarDbStorageError::InvalidArgument(
            "Multiple SQL statements are not supported.".to_owned(),
        ))
    } else {
        let statement = statements.remove(0);
        match statement {
            Statement::CreateTable(create_table) => semantic_checks_for_create_table(create_table),
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
            } => {
                let table_names = semantic_checks_for_drop(
                    object_type,
                    if_exists,
                    names,
                    cascade,
                    restrict,
                    purge,
                    temporary,
                )?;
                Ok(ModelarDbStatement::DropTable(table_names))
            }
            Statement::Truncate {
                table_names,
                partitions,
                table,
                only,
                identity,
                cascade,
                on_cluster,
            } => {
                let table_names = semantic_checks_for_truncate(
                    table_names,
                    partitions,
                    table,
                    only,
                    identity,
                    cascade,
                    on_cluster,
                )?;
                Ok(ModelarDbStatement::TruncateTable(table_names))
            }
            // ShowVariable is used as a substitute for VACUUM since Statement does not have a
            // Vacuum enum variant.
            Statement::ShowVariable { variable } => Ok(ModelarDbStatement::Vacuum(
                variable.into_iter().map(|ident| ident.value).collect(),
            )),
            Statement::Explain { .. } => Ok(ModelarDbStatement::Statement(statement)),
            Statement::Query(ref boxed_query) => {
                if let Some(addresses) = extract_include_addresses(boxed_query) {
                    Ok(ModelarDbStatement::IncludeSelect(statement, addresses))
                } else {
                    Ok(ModelarDbStatement::Statement(statement))
                }
            }
            Statement::Insert(ref _insert) => Ok(ModelarDbStatement::Statement(statement)),
            _ => Err(ModelarDbStorageError::InvalidArgument(
                "Only CREATE, DROP, TRUNCATE, EXPLAIN, INCLUDE, SELECT, INSERT, and VACUUM are supported."
                    .to_owned(),
            )),
        }
    }
}

/// Parse `sql_expression` into a [`DFExpr`] if it is a correct SQL expression that only references
/// columns in [`DFSchema`], otherwise [`ModelarDbStorageError`] is returned.
pub fn tokenize_and_parse_sql_expression(
    sql_expression: &str,
    df_schema: &DFSchema,
) -> Result<DFExpr> {
    let context_provider = ParserContextProvider::new();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let mut planner_context = PlannerContext::new();

    let dialect = ModelarDbDialect::new();
    let mut parser = Parser::new(&dialect).try_with_sql(sql_expression)?;
    let parsed_sql_expr = parser.parse_expr()?;
    sql_to_rel
        .sql_to_expr(parsed_sql_expr, df_schema, &mut planner_context)
        .map_err(|error| error.into())
}

/// SQL dialect that extends `sqlparsers's` [`GenericDialect`] with support for parsing CREATE TIME
/// SERIES TABLE table_name DDL statements, INCLUDE 'address'\[, 'address'\]+ DQL statements, and
/// VACUUM \[table_name\[, table_name\]+\] statements.
#[derive(Debug)]
struct ModelarDbDialect {
    /// Dialect to use for identifying identifiers.
    dialect: GenericDialect,
}

impl ModelarDbDialect {
    fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    /// Return [`true`] if the token stream starts with CREATE TIME SERIES TABLE, otherwise
    /// [`false`] is returned. The method does not consume tokens.
    fn next_tokens_are_create_time_series_table(&self, parser: &Parser) -> bool {
        // CREATE.
        if let Token::Word(word) = parser.peek_nth_token(0).token
            && word.keyword == Keyword::CREATE
        {
            // TIME.
            if let Token::Word(word) = parser.peek_nth_token(1).token
                && word.value.to_uppercase() == "TIME"
            {
                // SERIES.
                if let Token::Word(word) = parser.peek_nth_token(2).token
                    && word.value.to_uppercase() == "SERIES"
                {
                    // TABLE.
                    if let Token::Word(word) = parser.peek_nth_token(3).token
                        && word.keyword == Keyword::TABLE
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Parse CREATE TIME SERIES TABLE table_name DDL statements to a [`Statement::CreateTable`]. A
    /// [`ParserError`] is returned if the column names and the column types cannot be parsed.
    fn parse_create_time_series_table(
        &self,
        parser: &mut Parser,
    ) -> StdResult<Statement, ParserError> {
        // CREATE TIME SERIES TABLE.
        parser.expect_keyword(Keyword::CREATE)?;
        self.expect_word_value(parser, "TIME")?;
        self.expect_word_value(parser, "SERIES")?;
        parser.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_word_value(parser)?;

        // (column name and column type*).
        let columns = self.parse_columns(parser)?;

        // Return Statement::CreateTable with the extracted information.
        let name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(table_name))]);
        Ok(Self::new_create_time_series_table_statement(name, columns))
    }

    /// Parse (column name and type*) to a [`Vec<ColumnDef>`]. A [`ParserError`] is returned if the
    /// column names and the column types cannot be parsed.
    fn parse_columns(&self, parser: &mut Parser) -> StdResult<Vec<ColumnDef>, ParserError> {
        let mut columns = vec![];
        let mut parsed_all_columns = false;
        parser.expect_token(&Token::LParen)?;

        while !parsed_all_columns {
            let name = self.parse_word_value(parser)?;
            let data_type = self.parse_word_value(parser)?;
            let mut options = vec![];

            let data_type = match data_type.to_uppercase().as_str() {
                "TIMESTAMP" => SQLDataType::Timestamp(None, TimezoneInfo::None),
                "FIELD" => {
                    if parser.peek_token() == Token::LParen {
                        // An error bound is given for the field column.
                        parser.expect_token(&Token::LParen)?;
                        let error_bound = self.parse_positive_literal_f32(parser)?;
                        let is_relative = parser.consume_token(&Token::Mod);
                        parser.expect_token(&Token::RParen)?;

                        // The error bound is zero by default so there is no need to store zero.
                        if error_bound > 0.0 {
                            options.push(Self::new_error_bound_column_option_def(
                                error_bound,
                                is_relative,
                            ));
                        }
                    } else if let Token::Word(_) = parser.peek_nth_token(0).token {
                        // An expression to generate the field is given.
                        self.expect_word_value(parser, "AS")?;

                        parser.expect_token(&Token::LParen)?;
                        let option = ColumnOption::Generated {
                            generated_as: GeneratedAs::Always,
                            sequence_options: None,
                            generation_expr: Some(parser.parse_expr()?),
                            generation_expr_mode: None,
                            generated_keyword: false,
                        };
                        parser.expect_token(&Token::RParen)?;

                        options.push(ColumnOptionDef { name: None, option });
                    }

                    SQLDataType::Real
                }
                "TAG" => SQLDataType::Text,
                column_type => {
                    return Err(ParserError::ParserError(format!(
                        "Expected TIMESTAMP, FIELD, or TAG, found: {column_type}."
                    )));
                }
            };

            columns.push(Self::new_column_def(name.as_str(), data_type, options));

            if parser.peek_token() == Token::RParen {
                parser.expect_token(&Token::RParen)?;
                parsed_all_columns = true;
            } else {
                parser.expect_token(&Token::Comma)?;
            }
        }

        Ok(columns)
    }

    /// Return [`Ok`] if the next [`Token`] is a [`Token::Word`] with the value `expected`,
    /// otherwise a [`ParserError`] is returned.
    fn expect_word_value(&self, parser: &mut Parser, expected: &str) -> StdResult<(), ParserError> {
        if let Ok(string) = self.parse_word_value(parser)
            && string.to_uppercase() == expected.to_uppercase()
        {
            return Ok(());
        }
        parser.expected(expected, parser.peek_token())
    }

    /// Return its value as a [`String`] if the next [`Token`] is a [`Token::SingleQuotedString`],
    /// otherwise a [`ParserError`] is returned.
    fn parse_single_quoted_string(&self, parser: &mut Parser) -> StdResult<String, ParserError> {
        let token_with_location = parser.next_token();
        match token_with_location.token {
            Token::SingleQuotedString(string) => Ok(string),
            _ => parser.expected("single quoted string", token_with_location),
        }
    }

    /// Return its value as a [`String`] if the next [`Token`] is a
    /// [`Token::Word`], otherwise a [`ParserError`] is returned.
    fn parse_word_value(&self, parser: &mut Parser) -> StdResult<String, ParserError> {
        let token_with_location = parser.next_token();
        match token_with_location.token {
            Token::Word(word) => Ok(word.value),
            _ => parser.expected("word", token_with_location),
        }
    }

    /// Return its value as a [`f32`] if the next [`Token`] is a [`Token::Number`], otherwise a
    /// [`ParserError`] is returned.
    fn parse_positive_literal_f32(&self, parser: &mut Parser) -> StdResult<f32, ParserError> {
        let token_with_location = parser.next_token();
        match token_with_location.token {
            Token::Number(maybe_f32, _) => maybe_f32.parse::<f32>().map_err(|error| {
                ParserError::ParserError(format!(
                    "Failed to parse '{maybe_f32}' into a positive f32 due to: {error}"
                ))
            }),
            _ => parser.expected("literal float", token_with_location),
        }
    }

    /// Create a new [`ColumnDef`] with the provided `column_name`, `data_type`, and `options`.
    fn new_column_def(
        column_name: &str,
        data_type: SQLDataType,
        options: Vec<ColumnOptionDef>,
    ) -> ColumnDef {
        ColumnDef {
            name: Ident::new(column_name),
            data_type,
            options,
        }
    }

    /// Create a new [`ColumnOptionDef`] with the provided `error_bound`.
    fn new_error_bound_column_option_def(error_bound: f32, is_relative: bool) -> ColumnOptionDef {
        // An error bound column option does not exist so ColumnOption::DialectSpecific and
        // Token::Number is used. Token::Number's bool should be the number when cast to a bool.
        ColumnOptionDef {
            name: None,
            option: ColumnOption::DialectSpecific(vec![Token::Number(
                error_bound.to_string(),
                is_relative,
            )]),
        }
    }

    /// Create a new [`Statement::CreateTable`] with the provided `table_name` and `columns`, and
    /// with `engine` set to "TimeSeriesTable".
    fn new_create_time_series_table_statement(
        table_name: ObjectName,
        columns: Vec<ColumnDef>,
    ) -> Statement {
        // Designed to match the Statement::CreateTable created by sqlparser for CREATE TABLE as
        // closely as possible so semantic checks can be shared.
        Statement::CreateTable(CreateTable {
            or_replace: false,
            temporary: false,
            external: false,
            global: None,
            if_not_exists: false,
            transient: false,
            volatile: false,
            iceberg: false,
            name: table_name,
            columns,
            constraints: vec![],
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: Some(HiveFormat {
                row_format: None,
                serde_properties: None,
                storage: None,
                location: None,
            }),
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            clone: None,
            engine: Some(TableEngine {
                name: "TimeSeriesTable".to_owned(),
                parameters: None,
            }),
            comment: None,
            auto_increment_offset: None,
            default_charset: None,
            collation: None,
            on_commit: None,
            on_cluster: None,
            primary_key: None,
            order_by: None,
            partition_by: None,
            cluster_by: None,
            clustered_by: None,
            options: None,
            strict: false,
            copy_grants: false,
            enable_schema_evolution: None,
            change_tracking: None,
            data_retention_time_in_days: None,
            max_data_extension_time_in_days: None,
            default_ddl_collation: None,
            with_aggregation_policy: None,
            with_row_access_policy: None,
            with_tags: None,
            external_volume: None,
            base_location: None,
            catalog: None,
            catalog_sync: None,
            storage_serialization_policy: None,
        })
    }

    /// Return [`true`] if the token stream starts with INCLUDE, otherwise [`false`] is returned.
    /// The method does not consume tokens.
    fn next_token_is_include(&self, parser: &Parser) -> bool {
        // INCLUDE.
        if let Token::Word(word) = parser.peek_nth_token(0).token {
            word.keyword == Keyword::INCLUDE
        } else {
            false
        }
    }

    /// Parse INCLUDE address to a [`Value`] containing the address. A [`ParserError`] is returned
    /// if INCLUDE is typed incorrectly or the address cannot be extracted.
    fn parse_include_query(&self, parser: &mut Parser) -> StdResult<Statement, ParserError> {
        // INCLUDE.
        parser.expect_keyword(Keyword::INCLUDE)?;

        let mut addresses = vec![];
        loop {
            match self.parse_single_quoted_string(parser) {
                Ok(address) => {
                    addresses.push(new_setting(address, None, Span::empty(), Value::Null));
                    if let Token::Comma = parser.peek_nth_token(0).token {
                        parser.next_token();
                    } else {
                        break;
                    };
                }
                Err(error) => return Err(error),
            }
        }

        // SELECT.
        let mut boxed_query = parser.parse_query()?;

        // ClickHouse's SETTINGS is not supported by ModelarDB, so it is repurposed.
        boxed_query.settings = Some(addresses);
        let statement = Statement::Query(boxed_query);

        Ok(statement)
    }

    /// Return [`true`] if the token stream starts with VACUUM, otherwise [`false`] is returned.
    /// The method does not consume tokens.
    fn next_token_is_vacuum(&self, parser: &Parser) -> bool {
        // VACUUM.
        if let Token::Word(word) = parser.peek_nth_token(0).token {
            word.keyword == Keyword::VACUUM
        } else {
            false
        }
    }

    /// Parse VACUUM \[table_name\[, table_name\]+\] to a [`Statement::ShowVariable`] with the
    /// table names in the `variable` field. Note that [`Statement::ShowVariable`] is used since
    /// [`Statement`] does not have a `Vacuum` variant. A [`ParserError`] is returned if VACUUM is
    /// not the first word or the table names cannot be extracted.
    fn parse_vacuum(&self, parser: &mut Parser) -> StdResult<Statement, ParserError> {
        // VACUUM.
        parser.expect_keyword(Keyword::VACUUM)?;

        let mut table_names = vec![];

        if Token::EOF != parser.peek_nth_token(0).token {
            loop {
                match self.parse_word_value(parser) {
                    Ok(table_name) => {
                        table_names.push(Ident::new(table_name));
                        if Token::Comma == parser.peek_nth_token(0).token {
                            parser.next_token();
                        } else {
                            break;
                        };
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        // Return Statement::ShowVariable as a substitute for Vacuum.
        Ok(Statement::ShowVariable {
            variable: table_names,
        })
    }
}

/// Create a [`Setting`] with `key`, `quote_style`, and `value`.
fn new_setting(key: String, quote_style: Option<char>, span: Span, value: Value) -> Setting {
    let key = Ident {
        value: key,
        quote_style,
        span,
    };

    Setting { key, value }
}

impl Dialect for ModelarDbDialect {
    /// Return [`true`] if a character is a valid start character for an unquoted identifier,
    /// otherwise [`false`] is returned.
    fn is_identifier_start(&self, c: char) -> bool {
        self.dialect.is_identifier_start(c)
    }

    /// Return [`true`] if a character is a valid unquoted identifier character, otherwise [`false`]
    /// is returned.
    fn is_identifier_part(&self, c: char) -> bool {
        self.dialect.is_identifier_part(c)
    }

    /// Check if the next tokens are CREATE TIME SERIES TABLE, if so, attempt to parse the token stream
    /// as a CREATE TIME SERIES TABLE DDL statement. If not, check if the next token is INCLUDE, if so,
    /// attempt to parse the token stream as an INCLUDE 'address'\[, 'address'\]+ DQL statement.
    /// If not, check if the next token is VACUUM, if so, attempt to parse the token stream as a
    /// VACUUM \[table_name\[, table_name\]+\] statement. If all checks fail, [`None`] is returned
    /// so [`sqlparser`] uses its parsing methods for all other statements. If parsing succeeds, a
    /// [`Statement`] is returned, and if not, a [`ParserError`] is returned.
    fn parse_statement(&self, parser: &mut Parser) -> Option<StdResult<Statement, ParserError>> {
        if self.next_tokens_are_create_time_series_table(parser) {
            Some(self.parse_create_time_series_table(parser))
        } else if self.next_token_is_include(parser) {
            Some(self.parse_include_query(parser))
        } else if self.next_token_is_vacuum(parser) {
            Some(self.parse_vacuum(parser))
        } else {
            None
        }
    }
}

/// Context used when converting [`Expr`](sqlparser::ast::Expr) to [`DFExpr`], e.g., when validating
/// generation expressions. It is empty except for the functions included in Apache DataFusion. It
/// is an extended version of the empty context provider in [rewrite_expr.rs] in the Apache
/// DataFusion GitHub repository which was released under version 2.0 of the Apache License.
///
/// [rewrite_expr.rs]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/rewrite_expr.rs
struct ParserContextProvider {
    /// The default options for Apache DataFusion.
    options: ConfigOptions,
    /// The functions included in Apache DataFusion.
    udfs: HashMap<String, Arc<ScalarUDF>>,
}

impl ParserContextProvider {
    fn new() -> Self {
        let mut functions = functions::all_default_functions();

        let udfs = functions
            .drain(0..)
            .map(|udf| (udf.name().to_owned(), udf))
            .collect::<HashMap<String, Arc<ScalarUDF>>>();

        ParserContextProvider {
            options: ConfigOptions::default(),
            udfs,
        }
    }
}

impl ContextProvider for ParserContextProvider {
    fn get_table_source(
        &self,
        _name: TableReference,
    ) -> StdResult<Arc<dyn TableSource>, DataFusionError> {
        Err(DataFusionError::Plan(
            "The table was not found.".to_string(),
        ))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.udfs.get(name).cloned()
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        self.udfs.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        vec![]
    }

    fn udwf_names(&self) -> Vec<String> {
        vec![]
    }
}

/// Perform semantic checks to ensure that the CREATE TABLE and CREATE TIME SERIES TABLE statement in
/// `create_table` was correct. A [`ModelarDbStorageError`] is returned if a semantic check fails.
/// If all semantic checks are successful a [`ModelarDbStatement`] is returned.
fn semantic_checks_for_create_table(create_table: CreateTable) -> Result<ModelarDbStatement> {
    // Ensure it is a create table and only supported features are enabled.
    check_unsupported_features_are_disabled(&create_table)?;

    // let is used so a compile error is raised if CreateTable is changed.
    let CreateTable {
        name,
        columns,
        engine,
        ..
    } = create_table;

    // Extract the table name from the Statement::CreateTable.
    if name.0.len() > 1 {
        let message = "Multi-part table names are not supported.";
        return Err(ModelarDbStorageError::InvalidArgument(message.to_owned()));
    }

    // Check if the table name contains whitespace, e.g., spaces or tabs.
    // unwrap() is safe as ObjectNamePart is an enum with only one variant.
    let normalized_name = normalize_name(&name.0[0].as_ident().unwrap().value);
    if normalized_name.contains(char::is_whitespace) {
        let message = "Table name cannot contain whitespace.";
        return Err(ModelarDbStorageError::InvalidArgument(message.to_owned()));
    }

    // Check if the table name is a restricted keyword.
    let table_name_uppercase = normalized_name.to_uppercase();
    for keyword in ALL_KEYWORDS {
        if &table_name_uppercase == keyword {
            return Err(ModelarDbStorageError::InvalidArgument(format!(
                "Reserved keyword '{name}' cannot be used as a table name."
            )));
        }
    }

    // Check if the table name is a valid object_store path and database table name.
    object_store::path::Path::parse(&normalized_name)?;

    // Create a ModelarDbStatement with the information for creating the table of the specified
    // type.
    if let Some(_expected_engine) = engine {
        // Create a time series table for time series that only supports TIMESTAMP, FIELD, and TAG.
        let time_series_table_metadata =
            semantic_checks_for_create_time_series_table(normalized_name, columns)?;

        Ok(ModelarDbStatement::CreateTimeSeriesTable(Arc::new(
            time_series_table_metadata,
        )))
    } else {
        // Create a table that supports all columns types supported by Apache DataFusion.
        let context_provider = ParserContextProvider::new();
        let sql_to_rel = SqlToRel::new(&context_provider);
        let schema = sql_to_rel
            .build_schema(columns)
            .map_err(|error| ParserError::ParserError(error.to_string()))?;

        // SqlToRel.build() is hard coded to create Timestamp(TimeUnit::Nanosecond, TimeZone)
        // but Delta Lake currently only supports Timestamp(TimeUnit::Microsecond, TimeZone).
        let supported_fields = schema
            .flattened_fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Timestamp(_time_unit, timezone) => {
                    let data_type = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
                    Field::new(field.name(), data_type, field.is_nullable())
                }
                _data_type => (*field).clone(),
            })
            .collect::<Vec<Field>>();

        Ok(ModelarDbStatement::CreateNormalTable {
            name: normalized_name,
            schema: Schema::new(supported_fields),
        })
    }
}

/// Perform additional semantic checks to ensure that the CREATE TIME SERIES TABLE statement from which
/// `name` and `column_defs` was extracted was correct. A [`ParserError`] is returned if any of the
/// additional semantic checks fails.
fn semantic_checks_for_create_time_series_table(
    name: String,
    column_defs: Vec<ColumnDef>,
) -> StdResult<TimeSeriesTableMetadata, ParserError> {
    // Extract the error bounds for all columns. It is here to keep the parser types in the parser.
    let error_bounds = extract_error_bounds_for_all_columns(&column_defs)?;

    // Extract the expressions for all columns. It is here to keep the parser types in the parser.
    let generated_columns = extract_generation_exprs_for_all_columns(&column_defs)
        .map_err(|error| ParserError::ParserError(error.to_string()))?;

    // Convert column definitions to a schema.
    let query_schema = column_defs_to_time_series_table_query_schema(column_defs)
        .map_err(|error| ParserError::ParserError(error.to_string()))?;

    // Return the metadata required to create a time series table.
    let time_series_table_metadata = TimeSeriesTableMetadata::try_new(
        name,
        Arc::new(query_schema),
        error_bounds,
        generated_columns,
    )
    .map_err(|error| ParserError::ParserError(error.to_string()))?;

    Ok(time_series_table_metadata)
}

/// Return [`ParserError`] if [`Statement`] is not a [`CreateTable`] or if an unsupported
/// feature is set.
fn check_unsupported_features_are_disabled(
    create_table: &CreateTable,
) -> StdResult<(), ParserError> {
    // let is used so a compile error is raised if CreateTable is changed.
    let CreateTable {
        or_replace,
        temporary,
        external,
        global,
        if_not_exists,
        transient,
        volatile,
        iceberg,
        name: _name,
        columns: _columns,
        constraints,
        hive_distribution,
        hive_formats,
        table_properties,
        with_options,
        file_format,
        location,
        query,
        without_rowid,
        like,
        clone,
        engine: _engine,
        comment,
        auto_increment_offset,
        default_charset,
        collation,
        on_commit,
        on_cluster,
        primary_key,
        order_by,
        partition_by,
        cluster_by,
        clustered_by,
        options,
        strict,
        copy_grants,
        enable_schema_evolution,
        change_tracking,
        data_retention_time_in_days,
        max_data_extension_time_in_days,
        default_ddl_collation,
        with_aggregation_policy,
        with_row_access_policy,
        with_tags,
        external_volume,
        base_location,
        catalog,
        catalog_sync,
        storage_serialization_policy,
    } = create_table;

    check_unsupported_feature_is_disabled(*or_replace, "OR REPLACE")?;
    check_unsupported_feature_is_disabled(*temporary, "TEMPORARY")?;
    check_unsupported_feature_is_disabled(*external, "EXTERNAL")?;
    check_unsupported_feature_is_disabled(global.is_some(), "GLOBAL")?;
    check_unsupported_feature_is_disabled(*if_not_exists, "IF NOT EXISTS")?;
    check_unsupported_feature_is_disabled(*transient, "TRANSIENT")?;
    check_unsupported_feature_is_disabled(*iceberg, "ICEBERG")?;
    check_unsupported_feature_is_disabled(*volatile, "VOLATILE")?;
    check_unsupported_feature_is_disabled(!constraints.is_empty(), "CONSTRAINTS")?;
    check_unsupported_feature_is_disabled(
        hive_distribution != &HiveDistributionStyle::NONE,
        "Hive distribution",
    )?;
    check_unsupported_feature_is_disabled(
        *hive_formats
            != Some(HiveFormat {
                row_format: None,
                serde_properties: None,
                storage: None,
                location: None,
            }),
        "Hive formats",
    )?;
    check_unsupported_feature_is_disabled(!table_properties.is_empty(), "Table properties")?;
    check_unsupported_feature_is_disabled(!with_options.is_empty(), "OPTIONS")?;
    check_unsupported_feature_is_disabled(file_format.is_some(), "File format")?;
    check_unsupported_feature_is_disabled(location.is_some(), "Location")?;
    check_unsupported_feature_is_disabled(query.is_some(), "Query")?;
    check_unsupported_feature_is_disabled(*without_rowid, "Without ROWID")?;
    check_unsupported_feature_is_disabled(like.is_some(), "LIKE")?;
    check_unsupported_feature_is_disabled(clone.is_some(), "CLONE")?;
    check_unsupported_feature_is_disabled(comment.is_some(), "Comment")?;
    check_unsupported_feature_is_disabled(auto_increment_offset.is_some(), "AUTO INCREMENT")?;
    check_unsupported_feature_is_disabled(default_charset.is_some(), "Charset")?;
    check_unsupported_feature_is_disabled(collation.is_some(), "Collation")?;
    check_unsupported_feature_is_disabled(on_commit.is_some(), "ON COMMIT")?;
    check_unsupported_feature_is_disabled(on_cluster.is_some(), "ON CLUSTER")?;
    check_unsupported_feature_is_disabled(primary_key.is_some(), "PRIMARY_KEY")?;
    check_unsupported_feature_is_disabled(order_by.is_some(), "ORDER BY")?;
    check_unsupported_feature_is_disabled(partition_by.is_some(), "PARTITION BY")?;
    check_unsupported_feature_is_disabled(cluster_by.is_some(), "CLUSTER BY")?;
    check_unsupported_feature_is_disabled(clustered_by.is_some(), "CLUSTERED BY")?;
    check_unsupported_feature_is_disabled(options.is_some(), "NAME=VALUE")?;
    check_unsupported_feature_is_disabled(*strict, "STRICT")?;
    check_unsupported_feature_is_disabled(*copy_grants, "COPY_GRANTS")?;
    check_unsupported_feature_is_disabled(
        enable_schema_evolution.is_some(),
        "ENABLE_SCHEMA_EVOLUTION",
    )?;
    check_unsupported_feature_is_disabled(change_tracking.is_some(), "CHANGE_TRACKING")?;
    check_unsupported_feature_is_disabled(
        data_retention_time_in_days.is_some(),
        "DATA_RETENTION_TIME_IN_DAYS",
    )?;
    check_unsupported_feature_is_disabled(
        max_data_extension_time_in_days.is_some(),
        "MAX_DATA_EXTENSION_TIME_IN_DAYS",
    )?;
    check_unsupported_feature_is_disabled(
        default_ddl_collation.is_some(),
        "DEFAULT_DDL_COLLATION",
    )?;
    check_unsupported_feature_is_disabled(
        with_aggregation_policy.is_some(),
        "WITH_AGGREGATION_POLICY",
    )?;
    check_unsupported_feature_is_disabled(
        with_row_access_policy.is_some(),
        "WITH_ROW_ACCESS_POLICY",
    )?;
    check_unsupported_feature_is_disabled(with_tags.is_some(), "WITH_TAGS")?;
    check_unsupported_feature_is_disabled(external_volume.is_some(), "EXTERNAL_VOLUME")?;
    check_unsupported_feature_is_disabled(base_location.is_some(), "BASE_LOCATION")?;
    check_unsupported_feature_is_disabled(catalog.is_some(), "CATALOG")?;
    check_unsupported_feature_is_disabled(catalog_sync.is_some(), "CATALOG_SYNC")?;
    check_unsupported_feature_is_disabled(
        storage_serialization_policy.is_some(),
        "STORAGE_SERIALIZATION_POLICY",
    )?;
    Ok(())
}

/// Return [`ParserError`] specifying that the functionality with the name `feature` is not
/// supported if `enabled` is [`true`].
fn check_unsupported_feature_is_disabled(
    enabled: bool,
    feature: &str,
) -> StdResult<(), ParserError> {
    if enabled {
        let message = format!("{feature} is not supported.");
        Err(ParserError::ParserError(message))
    } else {
        Ok(())
    }
}

/// Return [`Schema`] if the types of the `column_defs` are supported by time series tables,
/// otherwise a [`DataFusionError`] is returned.
fn column_defs_to_time_series_table_query_schema(
    column_defs: Vec<ColumnDef>,
) -> StdResult<Schema, DataFusionError> {
    let mut fields = Vec::with_capacity(column_defs.len());

    // Manually convert TIMESTAMP, FIELD, and TAG columns to Apache DataFusion types.
    for column_def in column_defs {
        let normalized_name = normalize_name(&column_def.name.value);

        let field = match column_def.data_type {
            SQLDataType::Timestamp(None, TimezoneInfo::None) => {
                Field::new(normalized_name, ArrowTimestamp::DATA_TYPE, false)
            }
            SQLDataType::Real => {
                let mut metadata: HashMap<String, String> = HashMap::with_capacity(2);
                for column_option_def in column_def.options {
                    match column_option_def.option {
                        ColumnOption::DialectSpecific(dialect_specific_tokens) => {
                            let (error_bound, is_relative) =
                                tokens_to_error_bound(&dialect_specific_tokens)?;
                            let suffix = if is_relative { "%" } else { "" };
                            metadata
                                .insert("Error Bound".to_owned(), error_bound.to_string() + suffix);
                        }
                        ColumnOption::Generated {
                            generated_as: _,
                            sequence_options: _,
                            generation_expr,
                            generation_expr_mode: _,
                            generated_keyword: _,
                        } => {
                            metadata.insert(
                                "Generated As".to_owned(),
                                generation_expr.unwrap().to_string(),
                            );
                        }
                        option => {
                            return Err(DataFusionError::SQL(
                                ParserError::ParserError(format!(
                                    "{option} is not supported in time series tables."
                                )),
                                None,
                            ));
                        }
                    }
                }

                Field::new(normalized_name, ArrowValue::DATA_TYPE, false).with_metadata(metadata)
            }
            SQLDataType::Text => Field::new(normalized_name, DataType::Utf8, false),
            data_type => {
                return Err(DataFusionError::SQL(
                    ParserError::ParserError(format!(
                        "{data_type} is not supported in time series tables."
                    )),
                    None,
                ));
            }
        };

        fields.push(field);
    }

    Ok(Schema::new(fields))
}

/// Extract the error bounds from the columns in `column_defs`. The error bound for the timestamp
/// and tag columns will be zero so the error bound of each column can be accessed using its index.
fn extract_error_bounds_for_all_columns(
    column_defs: &[ColumnDef],
) -> StdResult<Vec<ErrorBound>, ParserError> {
    let mut error_bounds = Vec::with_capacity(column_defs.len());

    for column_def in column_defs {
        let mut error_bound_value = 0.0;
        let mut is_relative = false;

        for column_def_option in &column_def.options {
            if let ColumnOption::DialectSpecific(dialect_specific_tokens) =
                &column_def_option.option
            {
                (error_bound_value, is_relative) = tokens_to_error_bound(dialect_specific_tokens)?;
            }
        }

        let error_bound = if !is_relative {
            ErrorBound::try_new_absolute(error_bound_value)
        } else {
            ErrorBound::try_new_relative(error_bound_value)
        }
        .map_err(|error| ParserError::ParserError(error.to_string()))?;

        error_bounds.push(error_bound);
    }

    Ok(error_bounds)
}

/// Return the value of an error bound and a [`bool`] indicating if it is relative if it is the only
/// token in `dialect_specific_tokens`, otherwise [`ParserError`] is returned. Assumes the tokens
/// have been extracted from a [`ColumnOption::DialectSpecific`].
fn tokens_to_error_bound(dialect_specific_tokens: &[Token]) -> StdResult<(f32, bool), ParserError> {
    if dialect_specific_tokens.len() != 1 {
        return Err(ParserError::ParserError(
            "Error bounds are currently the only supported dialect specific options.".to_owned(),
        ));
    }

    if let Token::Number(error_bound_string, is_relative) = &dialect_specific_tokens[0] {
        let error_bound_value = error_bound_string
            .parse::<f32>()
            .map_err(|_error| ParserError::ParserError("Error bound is not a float".to_owned()))?;
        Ok((error_bound_value, *is_relative))
    } else {
        Err(ParserError::ParserError(
            "Dialect specific tokens must be an error bound.".to_owned(),
        ))
    }
}

/// Extract the [`GeneratedColumn`] from the field columns in `column_defs`. The [`GeneratedColumn`]
/// for the timestamp columns, stored field columns, tag columns will be [`None`] so the
/// [`GeneratedColumn`] of each generated field column can be accessed using its column index.
fn extract_generation_exprs_for_all_columns(
    column_defs: &[ColumnDef],
) -> StdResult<Vec<Option<GeneratedColumn>>, DataFusionError> {
    let context_provider = ParserContextProvider::new();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let schema = sql_to_rel.build_schema(column_defs.to_vec())?;
    let df_schema = schema.clone().to_dfschema()?;
    let mut planner_context = PlannerContext::new();
    let execution_props = ExecutionProps::new();

    let mut generated_columns = Vec::with_capacity(column_defs.len());

    for column_def in column_defs {
        let mut generated_column = None;

        for column_def_option in &column_def.options {
            if let ColumnOption::Generated {
                generated_as: _,
                sequence_options: _,
                generation_expr,
                generation_expr_mode: _,
                generated_keyword: _,
            } = &column_def_option.option
            {
                let sql_expr = generation_expr.as_ref().unwrap();

                // Ensure that the parsed sqlparser expression can be converted to a logical Apache
                // Arrow DataFusion expression within the context of schema to check it for errors.
                let expr =
                    sql_to_rel.sql_to_expr(sql_expr.clone(), &df_schema, &mut planner_context)?;

                // Ensure the logical Apache DataFusion expression can be converted to a physical
                // Apache DataFusion expression within the context of schema. This is to improve
                // error messages if the user-defined expression has semantic errors.
                let _physical_expr =
                    planner::create_physical_expr(&expr, &df_schema, &execution_props)?;

                // unwrap() is safe as sql_to_expr() validates that the expression only
                // references columns in the schema.
                generated_column = Some(GeneratedColumn::try_from_expr(expr, &df_schema).unwrap());
            }
        }

        generated_columns.push(generated_column);
    }

    Ok(generated_columns)
}

/// Extract the addresses specified in an INCLUDE clause if at least one is specified in `query`,
/// otherwise [`None`] is returned.
pub fn extract_include_addresses(query: &Query) -> Option<Vec<String>> {
    query.settings.as_ref().map(|settings| {
        settings
            .iter()
            .map(|setting| setting.key.value.to_owned())
            .collect()
    })
}

/// Perform semantic checks to ensure that the DROP statement from which the arguments were extracted
/// was correct. A [`ParserError`] is returned if any of the additional semantic checks fails.
fn semantic_checks_for_drop(
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
    restrict: bool,
    purge: bool,
    temporary: bool,
) -> StdResult<Vec<String>, ParserError> {
    if object_type != ObjectType::Table || if_exists || cascade || restrict || purge || temporary {
        Err(ParserError::ParserError(
            "Only DROP TABLE is supported.".to_owned(),
        ))
    } else {
        let mut table_names = Vec::with_capacity(names.len());

        for parts in names {
            let table_name = parts.0.iter().fold(String::new(), |name, part| {
                // unwrap() is safe as ObjectNamePart is an enum with only one variant.
                name + &part.as_ident().unwrap().value
            });

            table_names.push(table_name);
        }

        Ok(table_names)
    }
}

/// Perform semantic checks to ensure that the TRUNCATE statement from which the arguments were
/// extracted was correct. A [`ParserError`] is returned if any of the additional semantic checks
/// fails.
fn semantic_checks_for_truncate(
    names: Vec<TruncateTableTarget>,
    partitions: Option<Vec<Expr>>,
    table: bool,
    only: bool,
    identity: Option<TruncateIdentityOption>,
    cascade: Option<CascadeOption>,
    on_cluster: Option<Ident>,
) -> StdResult<Vec<String>, ParserError> {
    if partitions.is_some()
        || !table
        || only
        || identity.is_some()
        || cascade.is_some()
        || on_cluster.is_some()
    {
        Err(ParserError::ParserError(
            "Only TRUNCATE TABLE is supported.".to_owned(),
        ))
    } else {
        let mut table_names = Vec::with_capacity(names.len());

        for parts in names {
            // unwrap() is safe as ObjectNamePart is an enum with only one variant.
            let table_name = parts.name.0.iter().fold(String::new(), |name, part| {
                name + &part.as_ident().unwrap().value
            });

            table_names.push(table_name);
        }

        Ok(table_names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use sqlparser::dialect::ClickHouseDialect;

    // Tests for tokenize_and_parse_sql_statement().
    #[test]
    fn test_tokenize_and_parse_empty_sql() {
        assert!(tokenize_and_parse_sql_statement("").is_err());
    }

    #[test]
    fn test_tokenize_parse_semantic_check_create_time_series_table() {
        let sql = "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP,
                   field_one FIELD, field_two FIELD(10.5), field_three FIELD(1%),
                   field_four FIELD AS (CAST(SIN(CAST(field_one AS DOUBLE) * PI() / 180.0) AS REAL)),
                   tag TAG)";

        let modelardb_statement = tokenize_and_parse_sql_statement(sql).unwrap();
        if let ModelarDbStatement::CreateTimeSeriesTable(time_series_table_metadata) =
            &modelardb_statement
        {
            assert_eq!(time_series_table_metadata.name, "table_name");
            let expected_schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("field_one", DataType::Float32, false),
                Field::new("field_two", DataType::Float32, false).with_metadata({
                    let mut metadata = HashMap::new();
                    metadata.insert("Error Bound".to_owned(), "10.5".to_owned());
                    metadata
                }),
                Field::new("field_three", DataType::Float32, false).with_metadata({
                    let mut metadata = HashMap::new();
                    metadata.insert("Error Bound".to_owned(), "1%".to_owned());
                    metadata
                }),
                Field::new("field_four", DataType::Float32, false).with_metadata({
                    let mut metadata = HashMap::new();
                    metadata.insert(
                        "Generated As".to_owned(),
                        "CAST(SIN(CAST(field_one AS DOUBLE) * PI() / 180.0) AS REAL)".to_owned(),
                    );
                    metadata
                }),
                Field::new("tag", DataType::Utf8, false),
            ]));
            assert_eq!(time_series_table_metadata.query_schema, expected_schema);

            assert_eq!(
                time_series_table_metadata.error_bounds[2],
                ErrorBound::try_new_absolute(10.5).unwrap()
            );
            assert_eq!(
                time_series_table_metadata.error_bounds[3],
                ErrorBound::try_new_relative(1.0).unwrap()
            );
            assert!(time_series_table_metadata.generated_columns[4].is_some())
        } else {
            panic!(
                "CREATE TABLE DDL did not parse to a ModelarDbStatement::CreateTimeSeriesTable."
            );
        }
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_create() {
        assert!(
            tokenize_and_parse_sql_statement(
                "TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_create_time_space() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATETIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_time() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_time_series_space() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIMESERIES TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_series() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_series_table_space() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIESTABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_time_and_series() {
        // Tracks if sqlparser at some point can parse fields/tags in a TABLE.
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TABLE table_name(timestamp TIMESTAMP, field FIELD,
             field_one FIELD(10.5), field_two FIELD(1%), tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_table_name() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE(timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_table_table_name_space() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLEtable_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_start_parentheses() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name timestamp TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_option() {
        assert!(tokenize_and_parse_sql_statement(
            "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP PRIMARY KEY, field FIELD, tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_sql_types() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field REAL, tag VARCHAR)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_column_name() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(TIMESTAMP, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_generated_timestamps() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP AS (37), field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_generated_tags() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG AS (37))",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_generated_fields_without_parentheses()
    {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD AS 37, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_generated_fields_without_start_parentheses()
     {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD AS 37), tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_generated_fields_without_end_parentheses()
     {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD AS (37, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_generated_fields_with_absolute_error_bound()
     {
        assert!(tokenize_and_parse_sql_statement(
            "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD(1.0) AS (37), tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_generated_fields_with_relative_error_bound()
     {
        assert!(tokenize_and_parse_sql_statement(
            "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD(1.0%) AS (37), tag TAG)",
        )
        .is_err());
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_column_type() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp, field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_comma() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP field FIELD, tag TAG)",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_without_end_parentheses() {
        assert!(
            tokenize_and_parse_sql_statement(
                "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_two_create_time_series_table_statements() {
        let error = tokenize_and_parse_sql_statement(
            "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG);
             CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field FIELD, tag TAG)",
        );

        assert!(error.is_err());

        assert_eq!(
            error.unwrap_err().to_string(),
            "Invalid Argument Error: Multiple SQL statements are not supported."
        );
    }

    #[test]
    fn test_tokenize_and_parse_create_table_with_lowercase_keyword_as_table_name() {
        parse_and_assert_that_keywords_are_restricted_in_table_name(
            str::to_lowercase,
            "CREATE TABLE {}(timestamp TIMESTAMP, values REAL, metadata REAL)",
        )
    }

    #[test]
    fn test_tokenize_and_parse_create_table_with_uppercase_keyword_as_table_name() {
        parse_and_assert_that_keywords_are_restricted_in_table_name(
            str::to_uppercase,
            "CREATE TABLE {}(timestamp TIMESTAMP, values REAL, metadata REAL)",
        )
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_lowercase_keyword_as_table_name() {
        parse_and_assert_that_keywords_are_restricted_in_table_name(
            str::to_lowercase,
            "CREATE TIME SERIES TABLE {}(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
    }

    #[test]
    fn test_tokenize_and_parse_create_time_series_table_with_uppercase_keyword_as_table_name() {
        parse_and_assert_that_keywords_are_restricted_in_table_name(
            str::to_uppercase,
            "CREATE TIME SERIES TABLE {}(timestamp TIMESTAMP, field FIELD, tag TAG)",
        )
    }

    fn parse_and_assert_that_keywords_are_restricted_in_table_name(
        keyword_to_table_name: fn(&str) -> String,
        sql: &str,
    ) {
        for keyword in ALL_KEYWORDS {
            // END-EXEC cannot be parsed by the SQL parser because of the hyphen, and is therefore
            // skipped in this test.
            if keyword == &"END-EXEC" {
                continue;
            }
            let modelardb_statement = tokenize_and_parse_sql_statement(
                sql.replace("{}", keyword_to_table_name(keyword).as_str())
                    .as_str(),
            );

            assert!(modelardb_statement.is_err());

            assert_eq!(
                modelardb_statement.unwrap_err().to_string(),
                format!(
                    "Invalid Argument Error: Reserved keyword '{}' cannot be used as a table name.",
                    keyword_to_table_name(keyword)
                )
            );
        }
    }

    #[test]
    fn test_tokenize_and_parse_can_reconstruct_sql_from_parsed_sql_expressions() {
        let generation_exprs_sql = vec![
            "37",
            "field_1 + 73",
            "field_1 + field_2",
            "COS(field_1 * PI() / 180)",
            "SIN(field_1 * PI() / 180)",
            "TAN(field_1 * PI() / 180)",
        ];

        let df_schema = Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
            Field::new("tag_1", DataType::Utf8, false),
        ])
        .to_dfschema()
        .unwrap();

        let dialect = ModelarDbDialect::new();
        for generation_expr in generation_exprs_sql {
            // Assert that the generation expressions can be reconstructed to a SQL expression.
            let mut parser = Parser::new(&dialect).try_with_sql(generation_expr).unwrap();
            assert_eq!(generation_expr, parser.parse_expr().unwrap().to_string());

            // Assert that the expression can be parsed to an Apache DataFusion expression.
            tokenize_and_parse_sql_expression(generation_expr, &df_schema).unwrap();
        }
    }

    #[test]
    fn test_semantic_checks_for_create_time_series_table_check_correct_generated_expression() {
        let modelardb_statement = tokenize_and_parse_sql_statement(
            "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field_1 FIELD, field_2 FIELD AS (COS(field_1 * PI() / 180)), tag TAG)",
        ).unwrap();

        assert!(is_statement_create_table(modelardb_statement));
    }

    #[test]
    fn test_semantic_checks_for_create_time_series_table_check_wrong_generated_expression() {
        assert!(tokenize_and_parse_sql_statement(
            "CREATE TIME SERIES TABLE table_name(timestamp TIMESTAMP, field_1 FIELD, field_2 FIELD AS (COS(field_3 * PI() / 180)), tag TAG)",
        ).is_err());
    }

    /// Return [`true`] if `modelardb_statement` is [`ModelarDbStatement::CreateNormalTable`] or
    /// [`ModelarDbStatement::CreateTimeSeriesTable`] and [`false`] otherwise.
    fn is_statement_create_table(modelardb_statement: ModelarDbStatement) -> bool {
        matches!(
            modelardb_statement,
            ModelarDbStatement::CreateNormalTable { .. }
                | ModelarDbStatement::CreateTimeSeriesTable(..)
        )
    }

    #[test]
    fn test_tokenize_and_parse_settings_with_click_house_dialect() {
        assert!(
            Parser::parse_sql(
                &ClickHouseDialect {},
                "SELECT * FROM table_name SETTINGS convert_query_to_cnf = true"
            )
            .is_ok()
        )
    }

    #[test]
    fn test_tokenize_and_parse_settings_with_modelardb_dialect() {
        assert!(
            Parser::parse_sql(
                &ModelarDbDialect::new(),
                "SELECT * FROM table_name SETTINGS convert_query_to_cnf = true"
            )
            .is_err()
        )
    }

    #[test]
    fn test_tokenize_and_parse_include_one_address_select() {
        assert!(
            tokenize_and_parse_sql_statement(
                "INCLUDE 'grpc://192.168.1.2:9999' SELECT * FROM table_name",
            )
            .is_ok()
        );
    }

    #[test]
    fn test_tokenize_and_parse_include_multiple_addresses_select() {
        assert!(tokenize_and_parse_sql_statement(
            "INCLUDE 'grpc://192.168.1.2:9999', 'grpc://192.168.1.3:9999' SELECT * FROM table_name",
        )
        .is_ok());
    }

    #[test]
    fn test_tokenize_and_parse_include_one_double_quoted_address_select() {
        assert!(
            tokenize_and_parse_sql_statement(
                "INCLUDE \"grpc://192.168.1.2:9999\" SELECT * FROM table_name",
            )
            .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_one_address_select() {
        assert!(
            tokenize_and_parse_sql_statement("'grpc://192.168.1.2:9999' SELECT * FROM table_name",)
                .is_err()
        );
    }

    #[test]
    fn test_tokenize_and_parse_include_zero_addresses_select() {
        assert!(tokenize_and_parse_sql_statement("INCLUDE SELECT * FROM table_name",).is_err());
    }

    #[test]
    fn test_tokenize_and_parse_vacuum_all_tables() {
        let table_names = parse_vacuum_and_extract_table_names("VACUUM");

        assert!(table_names.is_empty());
    }

    #[test]
    fn test_tokenize_and_parse_vacuum_single_table() {
        let table_names = parse_vacuum_and_extract_table_names("VACUUM table_name");

        assert_eq!(table_names, vec!["table_name".to_owned()]);
    }

    #[test]
    fn test_tokenize_and_parse_vacuum_multiple_tables() {
        let table_names = parse_vacuum_and_extract_table_names("VACUUM table_name_1, table_name_2");

        assert_eq!(
            table_names,
            vec!["table_name_1".to_owned(), "table_name_2".to_owned()]
        );
    }

    fn parse_vacuum_and_extract_table_names(sql_statement: &str) -> Vec<String> {
        let modelardb_statement = tokenize_and_parse_sql_statement(sql_statement).unwrap();

        match modelardb_statement {
            ModelarDbStatement::Vacuum(table_names) => table_names,
            _ => panic!("Expected ModelarDbStatement::Vacuum."),
        }
    }

    #[test]
    fn test_tokenize_and_parse_vacuum_trailing_comma() {
        assert!(tokenize_and_parse_sql_statement("VACUUM table_name,").is_err());
    }

    #[test]
    fn test_tokenize_and_parse_vacuum_leading_comma() {
        assert!(tokenize_and_parse_sql_statement("VACUUM ,table_name").is_err());
    }

    #[test]
    fn test_tokenize_and_parse_vacuum_quoted_table_name() {
        assert!(tokenize_and_parse_sql_statement("VACUUM 'table_name'").is_err());
    }
}
