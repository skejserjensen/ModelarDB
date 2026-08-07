/* Copyright 2021 The ModelarDB Contributors
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

//! Implementation of [`TimeSeriesTable`] which allows time series tables to be queried through Apache
//! DataFusion. It takes the projection, filters as [`Exprs`](Expr), and limit of a query as input
//! and returns a physical query plan that produces all the data points required for the query.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::DataType::Utf8;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema, TimeUnit};
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::ToDFSchema;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::provider::TableProviderFilterPushDown;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{self, BinaryExpr, Expr, Operator, utils};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    LexOrdering, LexRequirement, PhysicalSortExpr, PhysicalSortRequirement, planner,
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use deltalake::kernel::LogicalFile;
use deltalake::{DeltaTable, DeltaTableError, ObjectMeta, PartitionFilter, PartitionValue};
use modelardb_types::schemas::{FIELD_COLUMN, GRID_SCHEMA, QUERY_COMPRESSED_SCHEMA};
use modelardb_types::types::{ArrowTimestamp, ArrowValue, TimeSeriesTableMetadata};

use crate::query::generated_as_exec::{ColumnToGenerate, GeneratedAsExec};
use crate::query::grid_exec::GridExec;
use crate::query::sorted_join_exec::{SortedJoinColumnType, SortedJoinExec};

/// A queryable representation of a time series table which stores multivariate time series as segments
/// containing metadata and models. [`TimeSeriesTable`] implements [`TableProvider`] so it can be
/// registered with Apache DataFusion and the multivariate time series queried as multiple
/// univariate time series.
pub(crate) struct TimeSeriesTable {
    /// Access to the Delta Lake table.
    delta_table: DeltaTable,
    /// Metadata for the time series table.
    time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
    /// Where data should be written to.
    data_sink: Arc<dyn DataSink>,
    /// Field column to use for queries that do not include fields.
    fallback_field_column: u16,
    /// Schema of the compressed segments stored on disk.
    query_compressed_schema: Arc<Schema>,
    /// The sort order [`DataSourceExec`] guarantees for the segments it produces. It is guaranteed
    /// by [`DataSourceExec`] because the storage engine uses this sort order for each Apache
    /// Parquet file in this time series table and these files are read sequentially by
    /// [`DataSourceExec`].
    query_order_segment: LexOrdering,
    /// The sort order that [`GridExec`] requires for the segments it receives as its input.
    query_requirement_segment: LexRequirement,
    /// Schema used to reconstruct the data points from each field column in the compressed segments.
    grid_schema: Arc<Schema>,
    /// The sort order [`GridExec`] guarantees for the data points it produces. It is guaranteed by
    /// [`GridExec`] because it receives segments sorted by `query_order_segment` from
    /// [`DataSourceExec`] and because these segments cannot contain data points for overlapping
    /// time intervals.
    query_order_data_point: LexOrdering,
    /// The sort order that [`SortedJoinExec`] requires for the data points it receives as its input.
    query_requirement_data_point: LexRequirement,
}

impl TimeSeriesTable {
    pub(crate) fn new(
        delta_table: DeltaTable,
        time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
        data_sink: Arc<dyn DataSink>,
    ) -> Arc<Self> {
        // Compute the index of the first stored field column in the time series table's query schema.
        // It is used for queries without fields as tags, timestamps, and values are stored together.
        let fallback_field_column = {
            // unwrap() is safe as all time series tables contain at least one field.
            time_series_table_metadata
                .query_schema
                .fields()
                .iter()
                .enumerate()
                .position(|(index, field)| {
                    time_series_table_metadata.generated_columns[index].is_none()
                        && field.data_type() == &ArrowValue::DATA_TYPE
                })
                .unwrap() as u16
        };

        // Add the tag columns to the base schema for queryable compressed segments.
        let mut query_compressed_schema_fields = Vec::with_capacity(
            QUERY_COMPRESSED_SCHEMA.0.fields.len()
                + time_series_table_metadata.tag_column_indices.len(),
        );

        query_compressed_schema_fields.extend(QUERY_COMPRESSED_SCHEMA.0.fields.clone().to_vec());
        for index in &time_series_table_metadata.tag_column_indices {
            query_compressed_schema_fields.push(Arc::new(
                time_series_table_metadata.schema.field(*index).clone(),
            ));
        }

        let query_compressed_schema = Arc::new(Schema::new(query_compressed_schema_fields));

        let (query_order_segment, query_requirement_segment) = query_order_and_requirement(
            &time_series_table_metadata,
            &query_compressed_schema,
            Column::new("start_time", 1),
        );

        // Add the tag columns to the base schema for data points.
        let mut grid_schema_fields = Vec::with_capacity(
            GRID_SCHEMA.0.fields.len() + time_series_table_metadata.tag_column_indices.len(),
        );

        grid_schema_fields.extend(GRID_SCHEMA.0.fields.clone().to_vec());
        for index in &time_series_table_metadata.tag_column_indices {
            grid_schema_fields.push(Arc::new(
                time_series_table_metadata.schema.field(*index).clone(),
            ));
        }

        let grid_schema = Arc::new(Schema::new(grid_schema_fields));

        let (query_order_data_point, query_requirement_data_point) = query_order_and_requirement(
            &time_series_table_metadata,
            &grid_schema,
            Column::new("timestamp", 0),
        );

        Arc::new(TimeSeriesTable {
            delta_table,
            time_series_table_metadata,
            data_sink,
            fallback_field_column,
            query_compressed_schema,
            query_order_segment,
            query_requirement_segment,
            grid_schema,
            query_order_data_point,
            query_requirement_data_point,
        })
    }

    /// Return the [`TimeSeriesTableMetadata`] for the time series table.
    pub(crate) fn time_series_table_metadata(&self) -> Arc<TimeSeriesTableMetadata> {
        self.time_series_table_metadata.clone()
    }

    /// Compute the schema that contains the stored columns in `projection`.
    fn apply_query_schema_projection_to_schema(&self, projection: &[usize]) -> Arc<Schema> {
        let columns = projection
            .iter()
            .filter_map(|query_schema_index| {
                if self.time_series_table_metadata.generated_columns[*query_schema_index].is_none()
                {
                    Some(
                        self.time_series_table_metadata
                            .query_schema
                            .field(*query_schema_index)
                            .clone(),
                    )
                } else {
                    None
                }
            })
            .collect::<Vec<Field>>();

        Arc::new(Schema::new(columns))
    }

    /// Return the index of the column in schema that has the same name as the column in query
    /// schema with the index `query_schema_index`. If a column with that name does not exist in
    /// schema a [`DataFusionError::Plan`] is returned.
    fn query_schema_index_to_schema_index(
        &self,
        query_schema_index: usize,
    ) -> DataFusionResult<usize> {
        let query_schema = &self.time_series_table_metadata.query_schema;
        let column_name = query_schema.field(query_schema_index).name();
        Ok(self
            .time_series_table_metadata
            .schema
            .index_of(column_name)?)
    }
}

/// The implementation is not derived as many instance variables does not implement [`fmt::Debug`].
impl fmt::Debug for TimeSeriesTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TimeSeriesTable: {}\n {:?}",
            self.time_series_table_metadata.name, self.time_series_table_metadata
        )
    }
}

/// Return a [`LexOrdering`] and [`LexRequirement`] that sort by the tag columns from
/// `time_series_table_metadata` in `schema` first and then by `time_column`.
fn query_order_and_requirement(
    time_series_table_metadata: &TimeSeriesTableMetadata,
    schema: &Schema,
    time_column: Column,
) -> (LexOrdering, LexRequirement) {
    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    let mut physical_sort_exprs =
        Vec::with_capacity(time_series_table_metadata.tag_column_indices.len() + 1);
    for index in &time_series_table_metadata.tag_column_indices {
        let tag_column_name = time_series_table_metadata.schema.field(*index).name();

        // unwrap() is safe as the tag columns are always present in the schema.
        let schema_index = schema.index_of(tag_column_name).unwrap();

        physical_sort_exprs.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(tag_column_name, schema_index)),
            options: sort_options,
        });
    }

    physical_sort_exprs.push(PhysicalSortExpr {
        expr: Arc::new(time_column),
        options: sort_options,
    });

    let physical_sort_requirements: Vec<PhysicalSortRequirement> = physical_sort_exprs
        .clone()
        .into_iter()
        .map(|physical_sort_expr| physical_sort_expr.into())
        .collect();

    (
        LexOrdering::new(physical_sort_exprs),
        LexRequirement::new(physical_sort_requirements),
    )
}

/// Rewrite and combine the `filters` that are written in terms of the time series table's query schema,
/// to a filter that is written in terms of the schema used for compressed segments by the storage
/// engine and a filter that is written in terms of the schema used for univariate time series by
/// [`GridExec`] for its output. If the filters cannot be rewritten an empty [`None`] is returned.
fn rewrite_and_combine_filters(schema: &Schema, filters: &[Expr]) -> (Option<Expr>, Option<Expr>) {
    let rewritten_filters = filters
        .iter()
        .filter_map(|filter| rewrite_filter(schema, filter));

    let (parquet_rewritten_filters, grid_rewritten_filters): (Vec<Expr>, Vec<Expr>) =
        rewritten_filters.unzip();

    let maybe_rewritten_parquet_filters = utils::conjunction(parquet_rewritten_filters);
    let maybe_rewritten_grid_filters = utils::conjunction(grid_rewritten_filters);

    (
        maybe_rewritten_parquet_filters,
        maybe_rewritten_grid_filters,
    )
}

/// Rewrite the `filter` that is written in terms of the time series table's query schema, to a filter
/// that is written in terms of the schema used for compressed segments by the storage engine and a
/// filter that is written in terms of the schema used for univariate time series by [`GridExec`].
/// If the filter cannot be rewritten, [`None`] is returned.
fn rewrite_filter(query_schema: &Schema, filter: &Expr) -> Option<(Expr, Expr)> {
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let Expr::Column(column) = &**left {
                // unwrap() is safe as it has already been checked that the fields exists.
                let field = query_schema.field_with_name(&column.name).unwrap();
                // Type aliases cannot be used as a constructor and thus cannot be used here.
                if *field.data_type() == DataType::Timestamp(TimeUnit::Microsecond, None) {
                    match op {
                        Operator::Gt | Operator::GtEq => Some((
                            new_binary_expr(logical_expr::col("end_time"), *op, *right.clone()),
                            new_binary_expr(logical_expr::col("timestamp"), *op, *right.clone()),
                        )),
                        Operator::Lt | Operator::LtEq => Some((
                            new_binary_expr(logical_expr::col("start_time"), *op, *right.clone()),
                            new_binary_expr(logical_expr::col("timestamp"), *op, *right.clone()),
                        )),
                        Operator::Eq => Some((
                            new_binary_expr(
                                new_binary_expr(
                                    logical_expr::col("start_time"),
                                    Operator::LtEq,
                                    *right.clone(),
                                ),
                                Operator::And,
                                new_binary_expr(
                                    logical_expr::col("end_time"),
                                    Operator::GtEq,
                                    *right.clone(),
                                ),
                            ),
                            new_binary_expr(logical_expr::col("timestamp"), *op, *right.clone()),
                        )),
                        _ => None,
                    }
                } else {
                    None
                }
            } else if let Expr::Column(column) = &**right {
                // unwrap() is safe as it has already been checked that the fields exists.
                let field = query_schema.field_with_name(&column.name).unwrap();
                // Type aliases cannot be used as a constructor and thus cannot be used here.
                if *field.data_type() == DataType::Timestamp(TimeUnit::Microsecond, None) {
                    match op {
                        Operator::Gt | Operator::GtEq => Some((
                            new_binary_expr(*left.clone(), *op, logical_expr::col("start_time")),
                            new_binary_expr(*left.clone(), *op, logical_expr::col("timestamp")),
                        )),
                        Operator::Lt | Operator::LtEq => Some((
                            new_binary_expr(*left.clone(), *op, logical_expr::col("end_time")),
                            new_binary_expr(*left.clone(), *op, logical_expr::col("timestamp")),
                        )),
                        Operator::Eq => Some((
                            new_binary_expr(
                                new_binary_expr(
                                    *left.clone(),
                                    Operator::LtEq,
                                    logical_expr::col("start_time"),
                                ),
                                Operator::And,
                                new_binary_expr(
                                    *left.clone(),
                                    Operator::GtEq,
                                    logical_expr::col("end_time"),
                                ),
                            ),
                            new_binary_expr(*right.clone(), *op, logical_expr::col("timestamp")),
                        )),
                        _ => None,
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        _other => None,
    }
}

/// Create a [`Expr::BinaryExpr`].
fn new_binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

/// Convert `maybe_expr` to a [`PhysicalExpr`] with the types in `query_schema` if possible.
fn try_convert_logical_expr_to_physical_expr(
    maybe_expr: Option<&Expr>,
    query_schema: Arc<Schema>,
) -> DataFusionResult<Option<Arc<dyn PhysicalExpr>>> {
    // Option.map() is not used so errors can be returned with ?.
    if let Some(maybe_expr) = maybe_expr {
        Ok(Some(convert_logical_expr_to_physical_expr(
            maybe_expr,
            query_schema,
        )?))
    } else {
        Ok(None)
    }
}

/// Convert `expr` to a [`PhysicalExpr`] with the types in `query_schema`.
fn convert_logical_expr_to_physical_expr(
    expr: &Expr,
    query_schema: Arc<Schema>,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    let df_query_schema = query_schema.clone().to_dfschema()?;
    planner::create_physical_expr(expr, &df_query_schema, &ExecutionProps::new())
}

/// Create an [`ExecutionPlan`] that will return the compressed segments that represent the data
/// points for `field_column_index` in `delta_table`. Returns a [`DataFusionError`] if the necessary
/// metadata cannot be retrieved from the metadata Delta Lake.
fn new_data_source_exec(
    delta_table: &DeltaTable,
    partition_filters: &[PartitionFilter],
    maybe_limit: Option<usize>,
    maybe_parquet_filters: &Option<Arc<dyn PhysicalExpr>>,
    file_schema: Arc<Schema>,
    output_ordering: Vec<LexOrdering>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    // Collect the LogicalFiles into a Vec so they can be sorted the same for all field columns.
    let mut logical_files = delta_table
        .get_active_add_actions_by_partitions(partition_filters)
        .map_err(|error| DataFusionError::Plan(error.to_string()))?
        .collect::<StdResult<Vec<LogicalFile>, DeltaTableError>>()
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;

    logical_files.sort_by_key(|logical_file| logical_file.modification_time());

    // Create the data source operator. Assumes the ObjectStore exists.
    let partitioned_files = logical_files
        .iter()
        .map(|logical_file| logical_file_to_partitioned_file(logical_file))
        .collect::<DataFusionResult<Vec<PartitionedFile>>>()?;
    let file_group = FileGroup::new(partitioned_files);

    let log_store = delta_table.log_store();
    let mut table_parquet_options = TableParquetOptions::new();
    table_parquet_options.global.pushdown_filters = true;
    table_parquet_options.global.reorder_filters = true;
    let file_source = if let Some(parquet_filters) = maybe_parquet_filters {
        Arc::new(
            ParquetSource::default()
                .with_predicate(file_schema.clone(), parquet_filters.to_owned()),
        )
    } else {
        Arc::new(ParquetSource::default())
    };

    let file_scan_config =
        FileScanConfigBuilder::new(log_store.object_store_url(), file_schema, file_source)
            .with_file_group(file_group)
            .with_limit(maybe_limit)
            .with_output_ordering(output_ordering);

    Ok(DataSourceExec::from_data_source(file_scan_config.build()))
}

/// Convert the [`LogicalFile`] `logical_file` to a [`PartitionFilter`]. A [`DataFusionError`] is
/// returned if the time the file was last modified cannot be read from `logical_file`.
fn logical_file_to_partitioned_file(
    logical_file: &LogicalFile,
) -> DataFusionResult<PartitionedFile> {
    let last_modified = logical_file
        .modification_datetime()
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;

    let object_meta = ObjectMeta {
        location: logical_file.object_store_path(),
        last_modified,
        size: logical_file.size() as u64,
        e_tag: None,
        version: None,
    };

    let partitioned_file = PartitionedFile {
        object_meta,
        partition_values: vec![],
        range: None,
        statistics: None,
        extensions: None,
        metadata_size_hint: None,
    };

    Ok(partitioned_file)
}

#[async_trait]
impl TableProvider for TimeSeriesTable {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the query schema of the time series table registered with Apache DataFusion.
    fn schema(&self) -> Arc<Schema> {
        self.time_series_table_metadata.query_schema.clone()
    }

    /// Specify that time series tables are base tables and not views or temporary tables.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Create an [`ExecutionPlan`] that will scan the time series table. Returns a
    /// [`DataFusionError::Plan`] if the necessary metadata cannot be retrieved.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Create shorthands for the metadata used during planning to improve readability.
        let schema = &self.time_series_table_metadata.schema;
        let tag_column_indices = &self.time_series_table_metadata.tag_column_indices;
        let query_schema = &self.time_series_table_metadata.query_schema;
        let generated_columns = &self.time_series_table_metadata.generated_columns;

        // Clone the Delta Lake table and update it to the latest version. self.delta_lake.load(
        // &mut self) is not an option due to TypeProvider::scan(&self, ...). Storing the DeltaTable
        // in a Mutex and RwLock is also not an option since most of the methods in TypeProvider
        // return a reference and the locks will be dropped at the end of the method.
        let mut delta_table = self.delta_table.clone();
        delta_table
            .load()
            .await
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        // Register the object store as done in DeltaTable so paths are from the table root.
        let log_store = delta_table.log_store();
        let object_store_url = log_store.object_store_url();
        state
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), log_store.object_store(None));

        // Ensures a projection is always present for looking up the columns to return.
        let mut projection: Vec<usize> = if let Some(projection) = projection {
            projection.to_vec()
        } else {
            (0..query_schema.fields().len()).collect()
        };

        // Compute the query schema for the record batches that Apache DataFusion needs to execute
        // the query.
        let query_schema_after_projection = Arc::new(query_schema.project(&projection)?);

        // Ensure that the columns which are required for any generated columns in the projection
        // are present in the record batches returned by SortedJoinStream and compute its schema.
        // GeneratedAsStream assumes that columns that are only used to generate columns are last.
        let mut generated_columns_sources: HashSet<usize> =
            HashSet::with_capacity(query_schema.fields().len());
        for query_schema_index in &projection {
            if let Some(generated_column) = &generated_columns[*query_schema_index] {
                generated_columns_sources.extend(&generated_column.source_columns);
            }
        }

        for generated_column_source in generated_columns_sources {
            if !projection.contains(&generated_column_source) {
                projection.push(generated_column_source);
            }
        }

        let schema_after_projection = self.apply_query_schema_projection_to_schema(&projection);

        // Compute the metadata needed to create the columns in the query in the correct order.
        let mut stored_columns_in_projection: Vec<SortedJoinColumnType> =
            Vec::with_capacity(projection.len());
        let mut stored_field_columns_in_projection: Vec<u16> =
            Vec::with_capacity(schema.fields.len() - 1 - tag_column_indices.len());
        let mut generated_columns_in_projection: Vec<ColumnToGenerate> =
            Vec::with_capacity(query_schema.fields.len() - schema.fields().len());

        for (result_index, query_schema_index) in projection.iter().enumerate() {
            if *query_schema.field(*query_schema_index).data_type() == ArrowTimestamp::DATA_TYPE {
                // Timestamp.
                stored_columns_in_projection.push(SortedJoinColumnType::Timestamp);
            } else if *query_schema.field(*query_schema_index).data_type() == Utf8 {
                // Tag.
                let tag_column_name = query_schema.fields[*query_schema_index].name().clone();
                stored_columns_in_projection.push(SortedJoinColumnType::Tag(tag_column_name));
            } else if let Some(generated_column) = &generated_columns[*query_schema_index] {
                // Generated field.
                let physical_expr = convert_logical_expr_to_physical_expr(
                    &generated_column.expr,
                    schema_after_projection.clone(),
                )?;

                generated_columns_in_projection
                    .push(ColumnToGenerate::new(result_index, physical_expr));
            } else {
                // Stored field.
                let schema_index = self.query_schema_index_to_schema_index(*query_schema_index);
                stored_field_columns_in_projection.push(schema_index? as u16);
                stored_columns_in_projection.push(SortedJoinColumnType::Field);
            }
        }

        // Filters are not converted to PhysicalExpr in rewrite_and_combine_filters() to simplify
        // testing rewrite_and_combine_filters() as Expr can be compared while PhysicalExpr cannot.
        let (maybe_rewritten_parquet_filters, maybe_rewritten_grid_filters) =
            rewrite_and_combine_filters(schema, filters);

        let maybe_physical_parquet_filters = try_convert_logical_expr_to_physical_expr(
            maybe_rewritten_parquet_filters.as_ref(),
            self.query_compressed_schema.clone(),
        )?;

        let maybe_physical_grid_filters = try_convert_logical_expr_to_physical_expr(
            maybe_rewritten_grid_filters.as_ref(),
            self.grid_schema.clone(),
        )?;

        if stored_field_columns_in_projection.is_empty() {
            stored_field_columns_in_projection.push(self.fallback_field_column);
        }

        // Request the matching files from the Delta Lake table and construct one or more GridExecs
        // and a SortedJoinExec. The write lock on the storage engine is held until object metas for
        // all columns have been retrieved to ensure they have the same number of data points.
        let mut field_column_execution_plans: Vec<Arc<dyn ExecutionPlan>> =
            Vec::with_capacity(stored_field_columns_in_projection.len());

        let mut partition_filters = vec![PartitionFilter {
            key: FIELD_COLUMN.to_owned(),
            value: PartitionValue::Equal("".to_owned()),
        }];

        // An expression is added so it is simple to replace with one that filters by field column.
        for field_column_index in stored_field_columns_in_projection {
            partition_filters[0].value = PartitionValue::Equal(field_column_index.to_string());

            let data_source_exec = new_data_source_exec(
                &delta_table,
                &partition_filters,
                limit,
                &maybe_physical_parquet_filters,
                self.query_compressed_schema.clone(),
                vec![LexOrdering::new(self.query_order_segment.to_vec())],
            )?;

            let grid_exec = GridExec::new(
                self.grid_schema.clone(),
                maybe_physical_grid_filters.clone(),
                limit,
                data_source_exec,
                self.query_requirement_segment.clone(),
                self.query_order_data_point.clone(),
            );

            field_column_execution_plans.push(grid_exec);
        }

        let sorted_join_exec = SortedJoinExec::new(
            schema_after_projection,
            stored_columns_in_projection,
            field_column_execution_plans,
            self.query_requirement_data_point.clone(),
        );

        // Only include GeneratedAsExec in the query plan if there are columns to generate.
        if generated_columns_in_projection.is_empty() {
            Ok(sorted_join_exec)
        } else {
            Ok(GeneratedAsExec::new(
                query_schema_after_projection,
                generated_columns_in_projection,
                sorted_join_exec,
            ))
        }
    }

    /// Specify that time series tables perform inexact predicate push-down.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_filter| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    /// Create an [`ExecutionPlan`] that will insert the result of `input` into the time series
    /// table. `inputs` must include generated columns to match the query schema returned by
    /// [`TableProvider::schema()`]. The generated columns are immediately dropped. Generally,
    /// [`arrow_flight::flight_service_server::FlightService::do_put()`] should be used instead of
    /// this method as it is more efficient. This method cannot fail, but it must return a
    /// [`DataFusionError`] to match the [`TableProvider`] trait.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let data_sink_exec = Arc::new(DataSinkExec::new(input, self.data_sink.clone(), None));
        Ok(data_sink_exec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::logical_expr::lit;
    use datafusion::prelude::Expr;
    use modelardb_test::table;
    use modelardb_types::types::Timestamp;

    const TIMESTAMP_PREDICATE_VALUE: Timestamp = 37;

    // Tests for rewrite_and_combine_filters().
    #[test]
    fn test_rewrite_empty_vec() {
        let schema = table::time_series_table_metadata().schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &[]);
        assert!(parquet_filter.is_none());
        assert!(grid_filter.is_none());
    }

    #[test]
    fn test_rewrite_greater_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Gt);
        let schema = table::time_series_table_metadata().schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        assert_binary_expr(
            parquet_filter.unwrap(),
            "end_time",
            Operator::Gt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::Gt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_greater_than_or_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::GtEq);
        let schema = table::time_series_table_metadata().schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        assert_binary_expr(
            parquet_filter.unwrap(),
            "end_time",
            Operator::GtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::GtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_less_than_timestamp() {
        let filters = new_timestamp_filters(Operator::Lt);
        let schema = table::time_series_table_metadata().schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        assert_binary_expr(
            parquet_filter.unwrap(),
            "start_time",
            Operator::Lt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::Lt,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_less_than_or_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::LtEq);
        let schema = table::time_series_table_metadata().schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        assert_binary_expr(
            parquet_filter.unwrap(),
            "start_time",
            Operator::LtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::LtEq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    #[test]
    fn test_rewrite_equal_timestamp() {
        let filters = new_timestamp_filters(Operator::Eq);
        let schema = table::time_series_table_metadata().schema;
        let (parquet_filter, grid_filter) = rewrite_and_combine_filters(&schema, &filters);

        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = parquet_filter.unwrap() {
            assert_binary_expr(
                *left,
                "start_time",
                Operator::LtEq,
                lit(TIMESTAMP_PREDICATE_VALUE),
            );
            assert_eq!(op, Operator::And);
            assert_binary_expr(
                *right,
                "end_time",
                Operator::GtEq,
                lit(TIMESTAMP_PREDICATE_VALUE),
            );
        } else {
            panic!("Expr is not a BinaryExpr.");
        }

        assert_binary_expr(
            grid_filter.unwrap(),
            "timestamp",
            Operator::Eq,
            lit(TIMESTAMP_PREDICATE_VALUE),
        );
    }

    fn new_timestamp_filters(operator: Operator) -> Vec<Expr> {
        vec![new_binary_expr(
            logical_expr::col("timestamp"),
            operator,
            lit(TIMESTAMP_PREDICATE_VALUE),
        )]
    }

    fn assert_binary_expr(expr: Expr, column: &str, operator: Operator, value: Expr) {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
            assert_eq!(*left, logical_expr::col(column));
            assert_eq!(op, operator);
            assert_eq!(*right, value);
        } else {
            panic!("Expr is not a BinaryExpr.");
        }
    }
}
