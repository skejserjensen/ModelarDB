/* Copyright 2025 The ModelarDB Contributors
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

//! Operations for interacting with ModelarDB Apache Arrow Flight servers and data folders.

pub mod client;
pub mod data_folder;

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::DFSchema;
use datafusion::execution::RecordBatchStream;
use modelardb_storage::parser::tokenize_and_parse_sql_expression;
use modelardb_types::types::{ErrorBound, GeneratedColumn, TimeSeriesTableMetadata};

use crate::error::Result;
use crate::{Aggregate, TableType};

/// Trait for interacting with ModelarDB, either through an Apache Arrow Flight server or a data
/// folder.
#[async_trait]
pub trait Operations: Sync + Send {
    /// Returns the [`Operations`] instance as [`Any`] so that it can be downcast to a specific
    /// implementation.
    fn as_any(&self) -> &dyn Any;

    /// Creates a table with the name in `table_name` and the information in `table_type`.
    async fn create(&mut self, table_name: &str, table_type: TableType) -> Result<()>;

    /// Returns the name of all the tables.
    async fn tables(&mut self) -> Result<Vec<String>>;

    /// Returns the schema of the table with the name in `table_name`.
    async fn schema(&mut self, table_name: &str) -> Result<Schema>;

    /// Writes the data in `uncompressed_data` to the table with the name in `table_name`.
    async fn write(&mut self, table_name: &str, uncompressed_data: RecordBatch) -> Result<()>;

    /// Executes the SQL in `sql` and returns the result as a [`RecordBatchStream`].
    async fn read(&mut self, sql: &str) -> Result<Pin<Box<dyn RecordBatchStream + Send>>>;

    /// Executes the SQL in `sql` and writes the result to the normal table with the name in
    /// `target_table_name` in `target`. Note that data can be copied from both normal tables and
    /// time series tables but only to normal tables. This is to not lossy compress data multiple times.
    async fn copy(
        &mut self,
        sql: &str,
        target: &mut dyn Operations,
        target_table_name: &str,
    ) -> Result<()>;

    /// Reads data from the time series table with the name in `table_name` and returns it as a
    /// [`RecordBatchStream`]. The remaining parameters optionally specify which subset of the data
    /// to read.
    async fn read_time_series_table(
        &mut self,
        table_name: &str,
        columns: &[(String, Aggregate)],
        group_by: &[String],
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        tags: HashMap<String, String>,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send>>>;

    /// Copy the data from the time series table with the name in `source_table_name` in `self` to the
    /// time series table with the name in `target_table_name` in `target`. Note that duplicate
    /// data is not deleted.
    async fn copy_time_series_table(
        &self,
        source_table_name: &str,
        target: &dyn Operations,
        target_table_name: &str,
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        tags: HashMap<String, String>,
    ) -> Result<()>;

    /// Move all data from the table with the name in `source_table_name` in `self` to the table with
    /// the name in `target_table_name` in `target`.
    async fn r#move(
        &mut self,
        source_table_name: &str,
        target: &dyn Operations,
        target_table_name: &str,
    ) -> Result<()>;

    /// Truncate the table with the name in `table_name`.
    async fn truncate(&mut self, table_name: &str) -> Result<()>;

    /// Drop the table with the name in `table_name`.
    async fn drop(&mut self, table_name: &str) -> Result<()>;

    /// Vacuum the table with the name in `table_name`.
    async fn vacuum(&mut self, table_name: &str) -> Result<()>;
}

/// Use the time series table metadata in `table_name`, `schema`, `error_bounds`, and `generated_columns`
/// to create [`TimeSeriesTableMetadata`]. If the metadata is valid, return [`TimeSeriesTableMetadata`],
/// otherwise return [`ModelarDbEmbeddedError`].
fn try_new_time_series_table_metadata(
    table_name: &str,
    schema: Schema,
    mut error_bounds: HashMap<String, ErrorBound>,
    generated_columns: HashMap<String, String>,
) -> Result<TimeSeriesTableMetadata> {
    let schema = Arc::new(schema);
    let df_schema: DFSchema = schema.clone().try_into()?;

    let lossless = ErrorBound::try_new_absolute(0.0)?;

    let mut error_bounds_all = Vec::with_capacity(schema.fields().len());
    let mut generated_columns_all = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        error_bounds_all.push(error_bounds.remove(field.name()).unwrap_or(lossless));

        if let Some(sql_expr) = generated_columns.get(field.name()) {
            let expr = tokenize_and_parse_sql_expression(sql_expr, &df_schema)?;
            generated_columns_all.push(Some(GeneratedColumn::try_from_expr(expr, &df_schema)?));
        } else {
            generated_columns_all.push(None);
        }
    }

    TimeSeriesTableMetadata::try_new(
        table_name.to_owned(),
        schema,
        error_bounds_all,
        generated_columns_all,
    )
    .map_err(|error| error.into())
}

/// Constructs an SQL query to read data from the time series table with the name in `table_name`.
/// The remaining parameters optionally specify which subset of the data to read.
pub(super) fn generate_read_time_series_table_sql(
    table_name: &str,
    schema: &Schema,
    columns: &[(String, Aggregate)],
    group_by: &[String],
    maybe_start_time: Option<&str>,
    maybe_end_time: Option<&str>,
    mut tags: HashMap<String, String>,
) -> String {
    // Construct the SELECT clause.
    let mut select_clause_values = vec![];

    for (column, aggregate) in columns {
        match aggregate {
            Aggregate::None => select_clause_values.push(column.to_owned()),
            Aggregate::Count => select_clause_values.push(format!("COUNT({column})")),
            Aggregate::Min => select_clause_values.push(format!("MIN({column})")),
            Aggregate::Max => select_clause_values.push(format!("MAX({column})")),
            Aggregate::Sum => select_clause_values.push(format!("SUM({column})")),
            Aggregate::Avg => select_clause_values.push(format!("AVG({column})")),
        }
    }

    // Construct the WHERE clause.
    let mut where_clause_values: Vec<String> = tags
        .drain()
        .map(|(name, value)| format!("{name} = '{value}'"))
        .collect();

    if let Some(start_time) = maybe_start_time {
        where_clause_values.push(format!("'{start_time}' <= timestamp"));
    }

    if let Some(end_time) = maybe_end_time {
        where_clause_values.push(format!("timestamp <= '{end_time}'"));
    }

    // Set default value if no value is provided.
    if select_clause_values.is_empty() {
        schema
            .fields()
            .iter()
            .for_each(|field| select_clause_values.push(field.name().to_owned()));
    }

    // Construct the full SQL query.
    let where_clause = if where_clause_values.is_empty() {
        "".to_owned()
    } else {
        "WHERE ".to_owned() + &where_clause_values.join(" AND ")
    };

    let group_by_clause = if group_by.is_empty() {
        "".to_owned()
    } else {
        "GROUP BY ".to_owned() + &group_by.join(", ")
    };

    format!(
        "SELECT {} FROM {} {} {}",
        select_clause_values.join(", "),
        table_name,
        where_clause,
        group_by_clause
    )
}
