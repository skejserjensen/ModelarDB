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

//! Operations for reading from and writing to ModelarDB data folders.

use std::any::Any;
use std::collections::HashMap;
use std::env;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::path::Path as StdPath;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::array::{Float32Array, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::datasource::sink::DataSink;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::sorts::sort;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, common};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use modelardb_storage::delta_lake::{DeltaLake, DeltaTableWriter};
use modelardb_storage::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_types::types::{TimeSeriesTableMetadata, TimestampArray};

use crate::error::{ModelarDbEmbeddedError, Result};
use crate::operations::{
    Operations, generate_read_time_series_table_sql, try_new_time_series_table_metadata,
};
use crate::{Aggregate, TableType};

/// [`DataSink`] that rejects INSERT statements passed to [`DataFolder.read()`].
struct DataFolderDataSink {
    /// The schema of the data sink is empty since it rejects everything.
    schema: Arc<Schema>,
}

impl DataFolderDataSink {
    fn new() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
        }
    }
}

#[async_trait]
impl DataSink for DataFolderDataSink {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the [`DataSink's`](DataSink) schema.
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Return [`None`] as the [`DataSink`] collects no metrics.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Return a [`DataFusionError`] as INSERT is not supported.
    async fn write_all(
        &self,
        _data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> StdResult<u64, DataFusionError> {
        Err(DataFusionError::Execution(
            "INSERT is not supported, use DataFolder.write() to insert data.".to_owned(),
        ))
    }
}

impl Debug for DataFolderDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "DataFolderDataSink")
    }
}

impl DisplayAs for DataFolderDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "DataFolderDataSink")
    }
}

/// Provides access to modelardb_embedded's components.
pub struct DataFolder {
    /// Delta Lake for storing metadata and data in Apache Parquet files.
    delta_lake: DeltaLake,
    /// Metadata manager for providing access to metadata related to tables. It is stored in an
    /// [`Arc`] because it is shared with each of the time series tables for use in query planning.
    table_metadata_manager: Arc<TableMetadataManager>,
    /// Context providing access to a specific session of Apache DataFusion.
    session_context: SessionContext,
}

impl DataFolder {
    /// Creates a [`DataFolder`] that manages data in memory and returns it. If the metadata tables
    /// could not be created, [`ModelarDbEmbeddedError`] is returned.
    pub async fn open_memory() -> Result<Self> {
        let delta_lake = DeltaLake::new_in_memory();
        let table_metadata_manager = Arc::new(TableMetadataManager::try_new_in_memory().await?);

        Self::try_new_and_register_tables(delta_lake, table_metadata_manager).await
    }

    /// Creates a [`DataFolder`] that manages data in the local folder at `data_folder_path` and
    /// returns it. If the folder does not exist and could not be created or the metadata tables
    /// could not be created, [`ModelarDbEmbeddedError`] is returned.
    pub async fn open_local(data_folder_path: &StdPath) -> Result<Self> {
        let delta_lake = DeltaLake::try_from_local_path(data_folder_path)?;

        let table_metadata_manager =
            Arc::new(TableMetadataManager::try_from_path(data_folder_path).await?);

        Self::try_new_and_register_tables(delta_lake, table_metadata_manager).await
    }

    /// Creates a [`DataFolder`] that manages data in an object store with an S3-compatible API and
    /// returns it. If a connection to the object store could not be established or the metadata
    /// tables could not be created, [`ModelarDbEmbeddedError`] is returned.
    pub async fn open_s3(
        endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self> {
        // Register the S3 storage handlers to allow the use of Amazon S3 object stores. This is
        // required at runtime to initialize the S3 storage implementation in the deltalake_aws
        // storage subcrate. It is safe to call this function multiple times as the handlers are
        // stored in a DashMap, thus, the handlers are simply overwritten with the same each time.
        deltalake::aws::register_handlers(None);

        // Construct data folder.
        let delta_lake = DeltaLake::try_from_s3_configuration(
            endpoint.clone(),
            bucket_name.clone(),
            access_key_id.clone(),
            secret_access_key.clone(),
        )?;

        let table_metadata_manager = Arc::new(
            TableMetadataManager::try_from_s3_configuration(
                endpoint,
                bucket_name,
                access_key_id,
                secret_access_key,
            )
            .await?,
        );

        Self::try_new_and_register_tables(delta_lake, table_metadata_manager).await
    }

    /// Creates a [`DataFolder`] that manages data in an object store with an Azure-compatible API
    /// and returns it. If a connection to the object store could not be established or the metadata
    /// tables could not be created, [`ModelarDbEmbeddedError`] is returned.
    pub async fn open_azure(
        account_name: String,
        access_key: String,
        container_name: String,
    ) -> Result<Self> {
        let delta_lake = DeltaLake::try_from_azure_configuration(
            account_name.clone(),
            access_key.clone(),
            container_name.clone(),
        )?;

        let table_metadata_manager = Arc::new(
            TableMetadataManager::try_from_azure_configuration(
                account_name,
                access_key,
                container_name,
            )
            .await?,
        );

        Self::try_new_and_register_tables(delta_lake, table_metadata_manager).await
    }

    /// Create a [`DataFolder`], register all normal tables and time series tables in it with its
    /// [`SessionContext`], and return it. If the tables could not be registered,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn try_new_and_register_tables(
        delta_lake: DeltaLake,
        table_metadata_manager: Arc<TableMetadataManager>,
    ) -> Result<Self> {
        // Construct data folder.
        let session_context = modelardb_storage::create_session_context();

        let data_folder = DataFolder {
            delta_lake,
            table_metadata_manager,
            session_context,
        };

        // Register normal tables.
        let data_sink = Arc::new(DataFolderDataSink::new());

        for normal_table_name in data_folder
            .table_metadata_manager
            .normal_table_names()
            .await?
        {
            let delta_table = data_folder
                .delta_lake
                .delta_table(&normal_table_name)
                .await?;

            modelardb_storage::register_normal_table(
                &data_folder.session_context,
                &normal_table_name,
                delta_table,
                data_sink.clone(),
            )?;
        }

        // Register time series tables.
        for metadata in data_folder
            .table_metadata_manager
            .time_series_table_metadata()
            .await?
        {
            let delta_table = data_folder.delta_lake.delta_table(&metadata.name).await?;

            modelardb_storage::register_time_series_table(
                &data_folder.session_context,
                delta_table,
                metadata,
                data_sink.clone(),
            )?;
        }

        Ok(data_folder)
    }

    /// Compress the `uncompressed_data` from the table with `time_series_table_metadata` and return the
    /// resulting segments.
    pub async fn compress_all(
        &self,
        time_series_table_metadata: &TimeSeriesTableMetadata,
        uncompressed_data: &RecordBatch,
    ) -> Result<Vec<RecordBatch>> {
        // Sort by all tags and then time to simplify splitting the data into time series.
        let sorted_uncompressed_data =
            sort_record_batch_by_tags_and_time(time_series_table_metadata, uncompressed_data)?;

        // Split the sorted uncompressed data into time series and compress them separately.
        let mut compressed_data = vec![];

        let tag_column_arrays: Vec<&StringArray> = time_series_table_metadata
            .tag_column_indices
            .iter()
            .map(|index| modelardb_types::array!(sorted_uncompressed_data, *index, StringArray))
            .collect();

        let mut tag_values = Vec::with_capacity(tag_column_arrays.len());
        for tag_column_array in &tag_column_arrays {
            tag_values.push(tag_column_array.value(0).to_owned());
        }

        // The index of the first data point of each time series must be stored so slices
        // containing only data points for each time series can be extracted and compressed.
        let mut row_index_start = 0;
        for row_index in 0..sorted_uncompressed_data.num_rows() {
            // If any of the tags differ, the data point is from a new time series.
            let mut is_new_time_series = false;
            for tag_column_index in 0..tag_column_arrays.len() {
                is_new_time_series |= tag_values[tag_column_index]
                    != tag_column_arrays[tag_column_index].value(row_index);
            }

            if is_new_time_series {
                let time_series_length = row_index - row_index_start;
                let uncompressed_time_series =
                    sorted_uncompressed_data.slice(row_index_start, time_series_length);

                self.compress(
                    time_series_table_metadata,
                    &uncompressed_time_series,
                    &tag_values,
                    &mut compressed_data,
                )
                .await?;

                for (tag_column_index, tag_column_array) in tag_column_arrays.iter().enumerate() {
                    tag_values[tag_column_index] = tag_column_array.value(row_index).to_owned();
                }

                row_index_start = row_index;
            }
        }

        let time_series_length = sorted_uncompressed_data.num_rows() - row_index_start;
        let uncompressed_time_series =
            sorted_uncompressed_data.slice(row_index_start, time_series_length);

        self.compress(
            time_series_table_metadata,
            &uncompressed_time_series,
            &tag_values,
            &mut compressed_data,
        )
        .await?;

        Ok(compressed_data)
    }

    /// Compress the field columns in `uncompressed_time_series` from the table with
    /// `time_series_table_metadata` and append the result to `compressed_data`. It is assumed that
    /// all data points in `uncompressed_time_series` have the same tags as in `tag_values`.
    async fn compress(
        &self,
        time_series_table_metadata: &TimeSeriesTableMetadata,
        uncompressed_time_series: &RecordBatch,
        tag_values: &[String],
        compressed_data: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        let uncompressed_timestamps = modelardb_types::array!(
            uncompressed_time_series,
            time_series_table_metadata.timestamp_column_index,
            TimestampArray
        );

        for field_column_index in &time_series_table_metadata.field_column_indices {
            let uncompressed_values = modelardb_types::array!(
                uncompressed_time_series,
                *field_column_index,
                Float32Array
            );

            let error_bound = time_series_table_metadata.error_bounds[*field_column_index];

            // unwrap() is safe as uncompressed_timestamps and uncompressed_values have the same length.
            let compressed_time_series = modelardb_compression::try_compress(
                uncompressed_timestamps,
                uncompressed_values,
                error_bound,
                time_series_table_metadata.compressed_schema.clone(),
                tag_values.to_vec(),
                *field_column_index as i16,
            )
            .unwrap();

            compressed_data.push(compressed_time_series);
        }

        Ok(())
    }

    /// Create a writer for writing multiple batches of data to the table with the table name in
    /// `table_name`. If the table does not exist or a writer for it could not be created, a
    /// [`ModelarDbEmbeddedError`] is returned.
    pub async fn writer(&self, table_name: &str) -> Result<DeltaTableWriter> {
        let delta_table = self.delta_lake.delta_table(table_name).await?;
        if self.time_series_table_metadata(table_name).await.is_some() {
            self.delta_lake
                .time_series_table_writer(delta_table)
                .await
                .map_err(|error| error.into())
        } else {
            self.delta_lake
                .normal_or_metadata_table_writer(delta_table)
                .await
                .map_err(|error| error.into())
        }
    }

    /// Return the schema of the table with the name in `table_name` if it is a normal table. If the
    /// table does not exist or the table is not a normal table, return [`None`].
    async fn normal_table_schema(&self, table_name: &str) -> Option<Schema> {
        if self
            .table_metadata_manager
            .is_normal_table(table_name)
            .await
            .is_ok_and(|is_normal_table| is_normal_table)
        {
            self.delta_lake
                .delta_table(table_name)
                .await
                .expect("Delta Lake table should exist if the table is in the metadata Delta Lake.")
                .get_schema()
                .expect("Delta Lake table should be loaded and metadata should be in the log.")
                .try_into()
                .ok()
        } else {
            None
        }
    }

    /// Return [`TimeSeriesTableMetadata`] for the table with `table_name` if it exists, is registered
    /// with Apache DataFusion, and is a time series table.
    pub async fn time_series_table_metadata(
        &self,
        table_name: &str,
    ) -> Option<Arc<TimeSeriesTableMetadata>> {
        let table_provider = self.session_context.table_provider(table_name).await.ok()?;
        modelardb_storage::maybe_table_provider_to_time_series_table_metadata(table_provider)
    }
}

#[async_trait]
impl Operations for DataFolder {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a table with the name in `table_name` and the information in `table_type`. If the
    /// table could not be created or registered with Apache DataFusion, [`ModelarDbEmbeddedError`]
    /// is returned.
    async fn create(&mut self, table_name: &str, table_type: TableType) -> Result<()> {
        match table_type {
            TableType::NormalTable(schema) => {
                let delta_table = self
                    .delta_lake
                    .create_normal_table(table_name, &schema)
                    .await?;

                self.table_metadata_manager
                    .save_normal_table_metadata(table_name)
                    .await?;

                let data_sink = Arc::new(DataFolderDataSink::new());

                modelardb_storage::register_normal_table(
                    &self.session_context,
                    table_name,
                    delta_table,
                    data_sink.clone(),
                )?;
            }
            TableType::TimeSeriesTable(schema, error_bounds, generated_columns) => {
                let time_series_table_metadata = Arc::new(try_new_time_series_table_metadata(
                    table_name,
                    schema,
                    error_bounds,
                    generated_columns,
                )?);

                let delta_table = self
                    .delta_lake
                    .create_time_series_table(&time_series_table_metadata)
                    .await?;

                self.table_metadata_manager
                    .save_time_series_table_metadata(&time_series_table_metadata)
                    .await?;

                let data_sink = Arc::new(DataFolderDataSink::new());

                modelardb_storage::register_time_series_table(
                    &self.session_context,
                    delta_table,
                    time_series_table_metadata,
                    data_sink.clone(),
                )?
            }
        }

        Ok(())
    }

    /// Returns the name of all the tables. If the table names could not be retrieved from the
    /// metadata Delta Lake, [`ModelarDbEmbeddedError`] is returned.
    async fn tables(&mut self) -> Result<Vec<String>> {
        self.table_metadata_manager
            .table_names()
            .await
            .map_err(|error| error.into())
    }

    /// Returns the schema of the table with the name in `table_name`. If the table does not exist,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn schema(&mut self, table_name: &str) -> Result<Schema> {
        if let Some(time_series_table_metadata) = self.time_series_table_metadata(table_name).await
        {
            Ok((*time_series_table_metadata.query_schema).to_owned())
        } else if let Some(normal_table_schema) = self.normal_table_schema(table_name).await {
            Ok(normal_table_schema)
        } else {
            Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "{table_name} is not a table."
            )))
        }
    }

    /// Writes the data in `uncompressed_data` to the table with the table name in `table_name`. If
    /// `uncompressed_data` is empty, the schema of `uncompressed_data` does not match the schema of
    /// the table, or the data could not be written to the table, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn write(&mut self, table_name: &str, uncompressed_data: RecordBatch) -> Result<()> {
        if uncompressed_data.num_rows() == 0 {
            return Err(ModelarDbEmbeddedError::InvalidArgument(
                "The uncompressed data is empty.".to_owned(),
            ));
        }

        let schema_mismatch_error = ModelarDbEmbeddedError::InvalidArgument(format!(
            "The uncompressed data does not match the schema for the table: {table_name}."
        ));

        if let Some(time_series_table_metadata) = self.time_series_table_metadata(table_name).await
        {
            // Time series table.
            if !schemas_are_compatible(
                &uncompressed_data.schema(),
                &time_series_table_metadata.schema,
            ) {
                return Err(schema_mismatch_error);
            }

            let compressed_data = self
                .compress_all(&time_series_table_metadata, &uncompressed_data)
                .await?;

            self.delta_lake
                .write_compressed_segments_to_time_series_table(table_name, compressed_data)
                .await?;
        } else if let Some(normal_table_schema) = self.normal_table_schema(table_name).await {
            // Normal table.
            if !schemas_are_compatible(&uncompressed_data.schema(), &normal_table_schema) {
                return Err(schema_mismatch_error);
            }

            self.delta_lake
                .write_record_batches_to_normal_table(table_name, vec![uncompressed_data])
                .await?;
        } else {
            return Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "{table_name} is not a table."
            )));
        }

        Ok(())
    }

    /// Executes the SQL in `sql` and returns the result as a [`RecordBatchStream`]. If the SQL
    /// could not be executed, [`ModelarDbEmbeddedError`] is returned.
    async fn read(&mut self, sql: &str) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        let data_frame = self.session_context.sql(sql).await?;

        data_frame
            .execute_stream()
            .await
            .map_err(|error| error.into())
    }

    /// Executes the SQL in `sql` and writes the result to the normal table with the name in
    /// `target_table_name` in `target`. Note that data can be copied from both normal tables and
    /// time series tables but only to normal tables. This is to not lossy compress data multiple
    /// times. If `target` is not a data folder, the data could not be queried, or the result
    /// could not be written to the normal table, [`ModelarDbEmbeddedError`] is returned.
    async fn copy(
        &mut self,
        sql: &str,
        target: &mut dyn Operations,
        target_table_name: &str,
    ) -> Result<()> {
        let target_data_folder = target
            .as_any()
            .downcast_ref::<DataFolder>()
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument("target is not a data folder.".to_owned())
            })?;

        let target_normal_table_schema = target_data_folder
            .normal_table_schema(target_table_name)
            .await
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(format!(
                    "{target_table_name} is not a normal table."
                ))
            })?;

        let record_batch_stream = self.read(sql).await?;

        if record_batch_stream.schema() != target_normal_table_schema.into() {
            Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "The schema of the data to copy does not match the schema of {target_table_name}."
            )))
        } else {
            let record_batches = common::collect(record_batch_stream).await?;

            target_data_folder
                .delta_lake
                .write_record_batches_to_normal_table(target_table_name, record_batches)
                .await?;

            Ok(())
        }
    }

    /// Reads data from the time series table with the table name in `table_name` and returns it as a
    /// [`RecordBatchStream`]. The remaining parameters optionally specify which subset of the data
    /// to read. If the table is not a time series table or the data could not be read,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn read_time_series_table(
        &mut self,
        table_name: &str,
        columns: &[(String, Aggregate)],
        group_by: &[String],
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        tags: HashMap<String, String>,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        // DataFolder.read() interface is designed for time series tables.
        let time_series_table_medata = if let Some(time_series_table_metadata) =
            self.time_series_table_metadata(table_name).await
        {
            time_series_table_metadata
        } else {
            return Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "{table_name} is not a time series table."
            )));
        };

        let sql = generate_read_time_series_table_sql(
            table_name,
            &time_series_table_medata.query_schema,
            columns,
            group_by,
            maybe_start_time,
            maybe_end_time,
            tags,
        );

        self.read(&sql).await
    }

    /// Copy the data from the time series table with the name in `source_table_name` in `self` to
    /// the time series table with the name in `target_table_name` in `target`. Note that duplicate
    /// data is not deleted. If `target` is not a data folder, the schemas of the time series
    /// tables do not match, or the data could not be copied, [`ModelarDbEmbeddedError`] is returned.
    #[allow(clippy::too_many_arguments)]
    async fn copy_time_series_table(
        &self,
        source_table_name: &str,
        target: &dyn Operations,
        target_table_name: &str,
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        _tags: HashMap<String, String>, // TODO: Support tag-based filtering.
    ) -> Result<()> {
        let target_data_folder = target
            .as_any()
            .downcast_ref::<DataFolder>()
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument("target is not a data folder.".to_owned())
            })?;

        // DataFolder.copy_time_series_table() interface is designed for time series tables.
        let source_time_series_table_metadata = self
            .time_series_table_metadata(source_table_name)
            .await
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(format!(
                    "{source_table_name} is not a time series table."
                ))
            })?;

        let target_time_series_table_metadata = target_data_folder
            .time_series_table_metadata(target_table_name)
            .await
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument(format!(
                    "{target_table_name} is not a time series table."
                ))
            })?;

        // Check if the schemas of the time series tables match.
        if !schemas_are_compatible(
            &source_time_series_table_metadata.schema,
            &target_time_series_table_metadata.schema,
        ) {
            return Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "The schema of {source_table_name} does not match the schema of {target_table_name}."
            )));
        }

        // Construct the WHERE clause.
        let mut where_clause_values: Vec<String> = vec![];

        if let Some(start_time) = maybe_start_time {
            where_clause_values.push(format!("'{start_time}' <= start_time"));
        }

        if let Some(end_time) = maybe_end_time {
            where_clause_values.push(format!("end_time <= '{end_time}'"));
        }

        // Construct the full SQL query.
        let where_clause = if where_clause_values.is_empty() {
            "".to_owned()
        } else {
            "WHERE ".to_owned() + &where_clause_values.join(" AND ")
        };

        let sql = format!("SELECT * FROM {source_table_name} {where_clause}");

        // Read data to copy from source_table_name in source.
        let source_table = Arc::new(self.delta_lake.delta_table(source_table_name).await?);

        let session_context = SessionContext::new();
        session_context.register_table(source_table_name, source_table)?;

        let df = session_context.sql(&sql).await?;
        let record_batches = df.collect().await?;

        // Write read data to target_table_name in target.
        target_data_folder
            .delta_lake
            .write_compressed_segments_to_time_series_table(target_table_name, record_batches)
            .await?;

        Ok(())
    }

    /// Move all data from the table with the name in `source_table_name` in `self` to the table
    /// with the name in `target_table_name` in `target`. If `target` is not a data folder, the
    /// schemas of the tables do not match, or the data could not be moved,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn r#move(
        &mut self,
        source_table_name: &str,
        target: &dyn Operations,
        target_table_name: &str,
    ) -> Result<()> {
        let target_data_folder = target
            .as_any()
            .downcast_ref::<DataFolder>()
            .ok_or_else(|| {
                ModelarDbEmbeddedError::InvalidArgument("target is not a data folder.".to_owned())
            })?;

        let schema_mismatch_error = ModelarDbEmbeddedError::InvalidArgument(format!(
            "The schema of {source_table_name} does not match the schema of {target_table_name}."
        ));

        if let (Some(source_time_series_table_metadata), Some(target_time_series_table_metadata)) = (
            self.time_series_table_metadata(source_table_name).await,
            target_data_folder
                .time_series_table_metadata(target_table_name)
                .await,
        ) {
            // If both tables are time series tables, check if their schemas match and write the
            // data in source_table_name to target_table_name if so.
            if !schemas_are_compatible(
                &source_time_series_table_metadata.schema,
                &target_time_series_table_metadata.schema,
            ) {
                return Err(schema_mismatch_error);
            }

            let delta_ops = self.delta_lake.delta_ops(source_table_name).await?;
            let (_table, stream) = delta_ops.load().await?;
            let record_batches: Vec<RecordBatch> = stream.try_collect().await?;

            target_data_folder
                .delta_lake
                .write_compressed_segments_to_time_series_table(target_table_name, record_batches)
                .await?;
        } else if let (Some(source_normal_table_schema), Some(target_normal_table_schema)) = (
            self.normal_table_schema(source_table_name).await,
            target_data_folder
                .normal_table_schema(target_table_name)
                .await,
        ) {
            // If both tables are normal tables, check if their schemas match and write the data in
            // source_table_name to target_table_name if so.
            if !schemas_are_compatible(&source_normal_table_schema, &target_normal_table_schema) {
                return Err(schema_mismatch_error);
            }

            let delta_ops = self.delta_lake.delta_ops(source_table_name).await?;
            let (_table, stream) = delta_ops.load().await?;
            let record_batches: Vec<RecordBatch> = stream.try_collect().await?;

            target_data_folder
                .delta_lake
                .write_record_batches_to_normal_table(target_table_name, record_batches)
                .await?;
        } else {
            return Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "{source_table_name} and {target_table_name} are not both normal tables or time series tables."
            )));
        }

        // Truncate the table after moving the data. This will also delete the tag hash metadata
        // if the table is a time series table.
        self.truncate(source_table_name).await?;

        Ok(())
    }

    /// Truncate the table with the name in `table_name` by deleting the table data in the data
    /// Delta Lake. If the data could not be deleted, [`ModelarDbEmbeddedError`] is returned.
    async fn truncate(&mut self, table_name: &str) -> Result<()> {
        if self.tables().await?.contains(&table_name.to_owned()) {
            self.delta_lake
                .truncate_table(table_name)
                .await
                .map_err(|error| error.into())
        } else {
            Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }

    /// Drop the table with the name in `table_name` by deregistering the table from the Apache
    /// Arrow DataFusion session, deleting all the table files from the data Delta Lake, and
    /// deleting the table metadata from the metadata Delta Lake. If the table could not be
    /// deregistered or the metadata or data could not be dropped, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn drop(&mut self, table_name: &str) -> Result<()> {
        // Drop the table from the Apache Arrow DataFusion session.
        self.session_context.deregister_table(table_name)?;

        // Delete the table metadata from the metadata Delta Lake.
        self.table_metadata_manager
            .drop_table_metadata(table_name)
            .await?;

        // Drop the table from the Delta Lake.
        self.delta_lake.drop_table(table_name).await?;

        Ok(())
    }

    /// Vacuum the table with the name in `table_name`. If the table does not exist or the
    /// table could not be vacuumed, [`ModelarDbEmbeddedError`] is returned.
    async fn vacuum(&mut self, table_name: &str) -> Result<()> {
        if self.tables().await?.contains(&table_name.to_owned()) {
            let retention_period_in_seconds = env::var("MODELARDBD_RETENTION_PERIOD_IN_SECONDS")
                .map_or(60 * 60 * 24 * 7, |value| value.parse().unwrap());

            self.delta_lake
                .vacuum_table(table_name, retention_period_in_seconds)
                .await
                .map_err(|error| error.into())
        } else {
            Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                "Table with name '{table_name}' does not exist."
            )))
        }
    }
}

/// Sort the `uncompressed_data` from the time series table with `time_series_table_metadata`
/// according to its tags and then timestamps.
fn sort_record_batch_by_tags_and_time(
    time_series_table_metadata: &TimeSeriesTableMetadata,
    uncompressed_data: &RecordBatch,
) -> Result<RecordBatch> {
    let mut physical_sort_exprs = vec![];

    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    for tag_column_index in &time_series_table_metadata.tag_column_indices {
        let field = time_series_table_metadata.schema.field(*tag_column_index);
        physical_sort_exprs.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(field.name(), *tag_column_index)),
            options: sort_options,
        });
    }

    let timestamp_column_index = time_series_table_metadata.timestamp_column_index;
    let field = time_series_table_metadata
        .schema
        .field(timestamp_column_index);
    physical_sort_exprs.push(PhysicalSortExpr {
        expr: Arc::new(Column::new(field.name(), timestamp_column_index)),
        options: sort_options,
    });

    sort::sort_batch(
        uncompressed_data,
        &LexOrdering::new(physical_sort_exprs),
        None,
    )
    .map_err(|error| error.into())
}

/// Compare `source_schema` and `target_schema` and return [`true`] if they have the same number of
/// columns, their columns have the same types, and their columns nullability is less or equally
/// restrictive in `source_schema`. Otherwise [`False`] is returned.
fn schemas_are_compatible(source_schema: &Schema, target_schema: &Schema) -> bool {
    let source_fields = source_schema.fields();
    let target_fields = target_schema.fields();

    if source_fields.len() != target_fields.len() {
        return false;
    }

    for index in 0..source_fields.len() {
        let source_field = &source_fields[index];
        let target_field = &target_fields[index];

        if source_field.data_type() != target_field.data_type() {
            return false;
        }

        if source_field.is_nullable() && !target_field.is_nullable() {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{LazyLock, Mutex};

    use arrow::array::{Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array};
    use arrow::datatypes::{ArrowPrimitiveType, DataType, Field};
    use arrow_flight::flight_service_client::FlightServiceClient;
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::col;
    use modelardb_types::types::{
        ArrowTimestamp, ArrowValue, ErrorBound, GeneratedColumn, ValueArray,
    };
    use tempfile::TempDir;
    use tonic::transport::Channel;

    use crate::operations::client::{Client, Node};
    use crate::record_batch_stream_to_record_batch;

    const NORMAL_TABLE_NAME: &str = "normal_table";
    const TIME_SERIES_TABLE_NAME: &str = "time_series_table";
    const MISSING_TABLE_NAME: &str = "missing_table";
    const TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME: &str = "time_series_table_with_generated";
    const INVALID_COLUMN_NAME: &str = "invalid_column";

    /// Lock used for env::set_var() as it is not guaranteed to be thread-safe.
    static SET_VAR_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[tokio::test]
    async fn test_create_normal_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_normal_table().await;
        assert_normal_table_exists(&data_folder, NORMAL_TABLE_NAME, normal_table_schema()).await;
    }

    #[tokio::test]
    async fn test_register_existing_normal_tables_on_open() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create(
                "normal_table_1",
                TableType::NormalTable(normal_table_schema()),
            )
            .await
            .unwrap();

        data_folder
            .create(
                "normal_table_2",
                TableType::NormalTable(normal_table_schema()),
            )
            .await
            .unwrap();

        // Create a new data folder and verify that the existing normal tables are registered.
        let new_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();
        assert!(
            new_data_folder
                .session_context
                .table_exist("normal_table_1")
                .unwrap()
        );
        assert!(
            new_data_folder
                .session_context
                .table_exist("normal_table_2")
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_create_normal_table_with_empty_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .create(NORMAL_TABLE_NAME, TableType::NormalTable(Schema::empty()))
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "ModelarDB Storage Error: Delta Lake Error: Generic error: \
            At least one column must be defined to create a table."
        );
    }

    #[tokio::test]
    async fn test_create_existing_normal_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(normal_table_schema()),
            )
            .await;
        assert!(result.is_ok());

        let result = data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(normal_table_schema()),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "ModelarDB Storage Error: Delta Lake Error: Generic error: \
            A Delta Lake table already exists at that location."
        );
    }

    #[tokio::test]
    async fn test_create_time_series_table() {
        let (_temp_dir, data_folder) = create_data_folder_with_time_series_table().await;
        assert_time_series_table_exists(
            &data_folder,
            TIME_SERIES_TABLE_NAME,
            time_series_table_schema(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_create_time_series_table_with_error_bounds() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let error_bounds = HashMap::from([
            (
                "field_1".to_owned(),
                ErrorBound::try_new_absolute(1.0).unwrap(),
            ),
            (
                "field_2".to_owned(),
                ErrorBound::try_new_relative(5.0).unwrap(),
            ),
        ]);

        let table_type =
            TableType::TimeSeriesTable(time_series_table_schema(), error_bounds, HashMap::new());
        data_folder
            .create(TIME_SERIES_TABLE_NAME, table_type)
            .await
            .unwrap();

        let time_series_table_metadata = assert_time_series_table_exists(
            &data_folder,
            TIME_SERIES_TABLE_NAME,
            time_series_table_schema(),
        )
        .await;

        let error_bound_values: Vec<f32> = time_series_table_metadata
            .error_bounds
            .iter()
            .map(|error_bound| match error_bound {
                ErrorBound::Absolute(value) => *value,
                ErrorBound::Relative(value) => -*value,
            })
            .collect();

        assert_eq!(error_bound_values, vec![0.0, 0.0, 0.0, 1.0, -5.0]);
    }

    #[tokio::test]
    async fn test_create_time_series_table_with_generated_columns() {
        let (_temp_dir, data_folder) =
            create_data_folder_with_time_series_table_with_generated_column().await;

        let mut time_series_table_metadata = assert_time_series_table_exists(
            &data_folder,
            TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME,
            time_series_table_with_generated_column_schema(),
        )
        .await;

        let expected_generated_column = GeneratedColumn {
            expr: col("field_1") + col("field_2"),
            source_columns: vec![3, 4],
        };

        let mut actual_generated_column = time_series_table_metadata
            .generated_columns
            .pop()
            .unwrap()
            .unwrap();

        // Sort the source columns to ensure that the order is consistent.
        actual_generated_column.source_columns.sort();
        assert_eq!(actual_generated_column, expected_generated_column);
    }

    #[tokio::test]
    async fn test_register_existing_time_series_tables_on_open() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create(
                "time_series_table_1",
                TableType::TimeSeriesTable(
                    time_series_table_schema(),
                    HashMap::new(),
                    HashMap::new(),
                ),
            )
            .await
            .unwrap();

        data_folder
            .create(
                "time_series_table_2",
                TableType::TimeSeriesTable(
                    time_series_table_schema(),
                    HashMap::new(),
                    HashMap::new(),
                ),
            )
            .await
            .unwrap();

        // Create a new data folder and verify that the existing time series tables are registered.
        let new_data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();
        assert!(
            new_data_folder
                .session_context
                .table_exist("time_series_table_1")
                .unwrap()
        );
        assert!(
            new_data_folder
                .session_context
                .table_exist("time_series_table_2")
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_create_time_series_table_with_empty_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .create(
                TIME_SERIES_TABLE_NAME,
                TableType::TimeSeriesTable(Schema::empty(), HashMap::new(), HashMap::new()),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "ModelarDB Types Error: Invalid Argument Error: \
            There needs to be exactly one timestamp column."
        );
    }

    #[tokio::test]
    async fn test_create_existing_time_series_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .create(
                TIME_SERIES_TABLE_NAME,
                TableType::TimeSeriesTable(
                    time_series_table_schema(),
                    HashMap::new(),
                    HashMap::new(),
                ),
            )
            .await;
        assert!(result.is_ok());

        let result = data_folder
            .create(
                TIME_SERIES_TABLE_NAME,
                TableType::TimeSeriesTable(
                    time_series_table_schema(),
                    HashMap::new(),
                    HashMap::new(),
                ),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "ModelarDB Storage Error: Delta Lake Error: Generic error: \
            A Delta Lake table already exists at that location."
        );
    }

    #[tokio::test]
    async fn test_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let table_names = data_folder.tables().await.unwrap();
        assert!(table_names.is_empty());

        data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(normal_table_schema()),
            )
            .await
            .unwrap();

        data_folder
            .create(
                TIME_SERIES_TABLE_NAME,
                TableType::TimeSeriesTable(
                    time_series_table_schema(),
                    HashMap::new(),
                    HashMap::new(),
                ),
            )
            .await
            .unwrap();

        let table_names = data_folder.tables().await.unwrap();
        assert_eq!(table_names, vec![NORMAL_TABLE_NAME, TIME_SERIES_TABLE_NAME]);
    }

    #[tokio::test]
    async fn test_normal_table_schema() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let actual_schema = data_folder.schema(NORMAL_TABLE_NAME).await.unwrap();
        assert_eq!(actual_schema, normal_table_schema());
    }

    #[tokio::test]
    async fn test_time_series_table_schema() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        let actual_schema = data_folder.schema(TIME_SERIES_TABLE_NAME).await.unwrap();
        assert_eq!(actual_schema, time_series_table_schema());
    }

    #[tokio::test]
    async fn test_missing_table_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder.schema(MISSING_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a table.")
        );
    }

    #[tokio::test]
    async fn test_write_to_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;
        let mut delta_table = data_folder
            .delta_lake
            .delta_table(NORMAL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 0);

        data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 1);
    }

    #[tokio::test]
    async fn test_write_empty_data_to_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let empty_data = RecordBatch::new_empty(Arc::new(normal_table_schema()));
        let result = data_folder.write(NORMAL_TABLE_NAME, empty_data).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: The uncompressed data is empty."
        );
    }

    #[tokio::test]
    async fn test_write_data_with_invalid_schema_to_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let result = data_folder
            .write(NORMAL_TABLE_NAME, invalid_table_data())
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The uncompressed data does not \
                match the schema for the table: {NORMAL_TABLE_NAME}."
            )
        );
    }

    #[tokio::test]
    async fn test_write_to_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;
        let mut delta_table = data_folder
            .delta_lake
            .delta_table(TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 0);

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 2);
    }

    #[tokio::test]
    async fn test_write_empty_data_to_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        let empty_data = RecordBatch::new_empty(Arc::new(time_series_table_schema()));
        let result = data_folder.write(TIME_SERIES_TABLE_NAME, empty_data).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: The uncompressed data is empty."
        );
    }

    #[tokio::test]
    async fn test_write_data_with_invalid_schema_to_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        let result = data_folder
            .write(TIME_SERIES_TABLE_NAME, invalid_table_data())
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The uncompressed data does not \
                match the schema for the table: {TIME_SERIES_TABLE_NAME}."
            )
        );
    }

    #[tokio::test]
    async fn test_write_to_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder
            .write(MISSING_TABLE_NAME, time_series_table_data())
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a table.")
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let result = data_folder_read_time_series_table(
            &mut data_folder,
            NORMAL_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {NORMAL_TABLE_NAME} is not a time series table.")
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder_read_time_series_table(
            &mut data_folder,
            MISSING_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a time series table.")
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let actual_result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(actual_result, sorted_time_series_table_data());
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_empty_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        let actual_result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            actual_result,
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_columns() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let columns = vec![
            ("tag_1".to_owned(), Aggregate::None),
            ("field_1".to_owned(), Aggregate::None),
        ];

        let actual_result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &columns,
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            actual_result,
            sorted_time_series_table_data().project(&[1, 3]).unwrap()
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_aggregates() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let columns = vec![
            ("tag_1".to_owned(), Aggregate::None),
            ("field_1".to_owned(), Aggregate::Count),
            ("field_1".to_owned(), Aggregate::Min),
            ("field_1".to_owned(), Aggregate::Max),
            ("field_2".to_owned(), Aggregate::Sum),
            ("field_2".to_owned(), Aggregate::Avg),
        ];

        let actual_result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &columns,
            &["tag_1".to_owned()],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            **actual_result.column(0),
            StringArray::from(vec!["tag_x", "tag_y"])
        );
        assert_eq!(**actual_result.column(1), Int64Array::from(vec![3, 3]));
        assert_eq!(
            **actual_result.column(2),
            Float32Array::from(vec![37.0, 71.0])
        );
        assert_eq!(
            **actual_result.column(3),
            Float32Array::from(vec![39.0, 73.0])
        );
        assert_eq!(
            **actual_result.column(4),
            Float64Array::from(vec![75.0, 165.0])
        );
        assert_eq!(
            **actual_result.column(5),
            Float64Array::from(vec![25.0, 55.0])
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_invalid_column() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        let columns = vec![(INVALID_COLUMN_NAME.to_owned(), Aggregate::None)];

        let result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &columns,
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Schema error: No field named {INVALID_COLUMN_NAME}. \
                Valid fields are {}.",
                time_series_table_columns()
            )
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_invalid_group_by() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        let group_by = vec![INVALID_COLUMN_NAME.to_owned()];

        let result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &[],
            &group_by,
            None,
            None,
            HashMap::new(),
        )
        .await;

        // The error message includes the valid fields twice when group_by is included in the query.
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Schema error: No field named {INVALID_COLUMN_NAME}. \
                Valid fields are {}, {}.",
                time_series_table_columns(),
                time_series_table_columns()
            )
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_timestamps() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let actual_result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &[],
            &[],
            Some("1970-01-01T00:00:00.000150"),
            Some("1970-01-01T00:00:00.000250"),
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(actual_result, time_series_table_data().slice(2, 2));
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_invalid_timestamps() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let invalid_start_time = "01/01/1970T00:00:00.000150";
        let result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &[],
            &[],
            Some(invalid_start_time),
            Some("01/01/1970T00:00:00.000250"),
            HashMap::new(),
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Arrow error: Parser error: \
                Error parsing timestamp from '{invalid_start_time}': error parsing date"
            )
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_tags() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let mut tags = HashMap::new();
        tags.insert("tag_1".to_owned(), "tag_x".to_owned());
        tags.insert("tag_2".to_owned(), "tag_a".to_owned());

        let actual_result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            tags,
        )
        .await
        .unwrap();

        assert_eq!(actual_result, sorted_time_series_table_data().slice(0, 3));
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_invalid_tag() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        let mut tags = HashMap::new();
        tags.insert(INVALID_COLUMN_NAME.to_owned(), "tag_x".to_owned());

        let result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_NAME,
            &[],
            &[],
            None,
            None,
            tags,
        )
        .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Schema error: No field named {INVALID_COLUMN_NAME}. \
                Valid fields are {}.",
                time_series_table_columns()
            )
        );
    }

    fn time_series_table_columns() -> String {
        time_series_table_schema()
            .fields()
            .iter()
            .map(|field| format!("{TIME_SERIES_TABLE_NAME}.{}", field.name()))
            .collect::<Vec<String>>()
            .join(", ")
    }

    #[tokio::test]
    async fn test_read_time_series_table_from_time_series_table_with_generated_column() {
        let (_temp_dir, mut data_folder) =
            create_data_folder_with_time_series_table_with_generated_column().await;

        data_folder
            .write(
                TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME,
                time_series_table_data(),
            )
            .await
            .unwrap();

        let actual_result = data_folder_read_time_series_table(
            &mut data_folder,
            TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME,
            &[],
            &[],
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

        assert_generated_column_result_eq(actual_result, sorted_time_series_table_data());
    }

    async fn data_folder_read_time_series_table(
        data_folder: &mut DataFolder,
        table_name: &str,
        columns: &[(String, Aggregate)],
        group_by: &[String],
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        tags: HashMap<String, String>,
    ) -> Result<RecordBatch> {
        let record_batch_stream = data_folder
            .read_time_series_table(
                table_name,
                columns,
                group_by,
                maybe_start_time,
                maybe_end_time,
                tags,
            )
            .await?;

        record_batch_stream_to_record_batch(record_batch_stream).await
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_normal_table_to_time_series_table() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;
        let (_temp_dir, target) = create_data_folder_with_time_series_table().await;

        source
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let result = source
            .copy_time_series_table(
                NORMAL_TABLE_NAME,
                &target,
                TIME_SERIES_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {NORMAL_TABLE_NAME} is not a time series table.")
        );
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_time_series_table_to_normal_table() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;
        let (_temp_dir, target) = create_data_folder_with_normal_table().await;

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let result = source
            .copy_time_series_table(
                TIME_SERIES_TABLE_NAME,
                &target,
                NORMAL_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {NORMAL_TABLE_NAME} is not a time series table.")
        );
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let source = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let (_temp_dir, target) = create_data_folder_with_time_series_table().await;

        let result = source
            .copy_time_series_table(
                MISSING_TABLE_NAME,
                &target,
                TIME_SERIES_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a time series table.")
        );
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_time_series_table_to_missing_table() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let result = source
            .copy_time_series_table(
                TIME_SERIES_TABLE_NAME,
                &target,
                MISSING_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a time series table.")
        );
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_time_series_table_to_time_series_table_with_invalid_schema()
     {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let mut target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let table_type =
            TableType::TimeSeriesTable(invalid_table_schema(), HashMap::new(), HashMap::new());
        target
            .create(TIME_SERIES_TABLE_NAME, table_type)
            .await
            .unwrap();

        let result = source
            .copy_time_series_table(
                TIME_SERIES_TABLE_NAME,
                &target,
                TIME_SERIES_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The schema of {TIME_SERIES_TABLE_NAME} \
                does not match the schema of {TIME_SERIES_TABLE_NAME}."
            )
        );
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_time_series_table_to_time_series_table() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_time_series_table().await;

        let sql = format!("SELECT * FROM {TIME_SERIES_TABLE_NAME}");
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(target_actual_result.num_rows(), 0);

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        source
            .copy_time_series_table(
                TIME_SERIES_TABLE_NAME,
                &target,
                TIME_SERIES_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await
            .unwrap();

        let source_actual_result = data_folder_read(&mut source, &sql).await.unwrap();
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();

        assert_eq!(source_actual_result, sorted_time_series_table_data());
        assert_eq!(target_actual_result, source_actual_result);
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_time_series_table_to_time_series_table_with_timestamps()
     {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_time_series_table().await;

        let sql = format!("SELECT * FROM {TIME_SERIES_TABLE_NAME}");
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(target_actual_result.num_rows(), 0);

        // Force the physical data to have multiple segments by writing the data in three parts.
        for i in 0..3 {
            source
                .write(
                    TIME_SERIES_TABLE_NAME,
                    time_series_table_data().slice(i * 2, 2),
                )
                .await
                .unwrap();
        }

        source
            .copy_time_series_table(
                TIME_SERIES_TABLE_NAME,
                &target,
                TIME_SERIES_TABLE_NAME,
                Some("1970-01-01T00:00:00.000150"),
                Some("1970-01-01T00:00:00.000250"),
                HashMap::new(),
            )
            .await
            .unwrap();

        let source_actual_result = data_folder_read(&mut source, &sql).await.unwrap();
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();

        assert_eq!(source_actual_result, time_series_table_data());
        assert_eq!(target_actual_result, time_series_table_data().slice(2, 2));
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_time_series_table_to_time_series_table_with_invalid_timestamps()
     {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;
        let (_temp_dir, target) = create_data_folder_with_time_series_table().await;

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let invalid_start_time = "01/01/1970T00:00:00.000150";
        let result = source
            .copy_time_series_table(
                TIME_SERIES_TABLE_NAME,
                &target,
                TIME_SERIES_TABLE_NAME,
                Some(invalid_start_time),
                Some("01/01/1970T00:00:00.000250"),
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Arrow error: Parser error: \
                Error parsing timestamp from '{invalid_start_time}': error parsing date"
            )
        );
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_time_series_table_to_time_series_table_with_generated_column()
     {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;
        let (_temp_dir, mut target) =
            create_data_folder_with_time_series_table_with_generated_column().await;

        let target_sql = format!("SELECT * FROM {TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME}");
        let target_actual_result = data_folder_read(&mut target, &target_sql).await.unwrap();
        assert_eq!(target_actual_result.num_rows(), 0);

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        // Even though the query schemas are different, the data should still be copied.
        source
            .copy_time_series_table(
                TIME_SERIES_TABLE_NAME,
                &target,
                TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await
            .unwrap();

        let source_sql = format!("SELECT * FROM {TIME_SERIES_TABLE_NAME}");
        let source_actual_result = data_folder_read(&mut source, &source_sql).await.unwrap();

        let target_actual_result = data_folder_read(&mut target, &target_sql).await.unwrap();

        assert_eq!(source_actual_result, sorted_time_series_table_data());
        assert_generated_column_result_eq(target_actual_result, source_actual_result);
    }

    #[tokio::test]
    async fn test_copy_time_series_table_from_data_folder_to_client() {
        let (_temp_dir, source) = create_data_folder_with_time_series_table().await;
        let target_client = lazy_modelardb_client();

        let result = source
            .copy_time_series_table(
                TIME_SERIES_TABLE_NAME,
                &target_client,
                TIME_SERIES_TABLE_NAME,
                None,
                None,
                HashMap::new(),
            )
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: target is not a data folder.".to_owned()
        );
    }

    #[tokio::test]
    async fn test_read_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let expected_result = normal_table_data();
        data_folder
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let actual_result = data_folder_read(&mut data_folder, &sql).await.unwrap();

        assert_eq!(actual_result, expected_result);
    }

    #[tokio::test]
    async fn test_read_empty_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        let sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let actual_result = data_folder_read(&mut data_folder, &sql).await.unwrap();

        assert_eq!(
            actual_result,
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        );
    }

    #[tokio::test]
    async fn test_read_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let sql = format!("SELECT * FROM {TIME_SERIES_TABLE_NAME}");
        let actual_result = data_folder_read(&mut data_folder, &sql).await.unwrap();

        assert_eq!(actual_result, sorted_time_series_table_data());
    }

    #[tokio::test]
    async fn test_read_empty_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        let sql = format!("SELECT * FROM {TIME_SERIES_TABLE_NAME}");
        let actual_result = data_folder_read(&mut data_folder, &sql).await.unwrap();

        assert_eq!(
            actual_result,
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        );
    }

    #[tokio::test]
    async fn test_read_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let sql = format!("SELECT * FROM {MISSING_TABLE_NAME}");
        let result = data_folder_read(&mut data_folder, &sql).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Error during planning: \
                table 'datafusion.public.{MISSING_TABLE_NAME}' not found"
            )
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_normal_table() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_normal_table().await;

        let sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(target_actual_result.num_rows(), 0);

        source
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT * FROM {NORMAL_TABLE_NAME} LIMIT 3");
        source
            .copy(&copy_sql, &mut target, NORMAL_TABLE_NAME)
            .await
            .unwrap();

        let source_actual_result = data_folder_read(&mut source, &sql).await.unwrap();
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();

        assert_eq!(source_actual_result, normal_table_data());
        assert_eq!(target_actual_result, source_actual_result.slice(0, 3));
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_normal_table_with_different_schema() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let schema = normal_table_schema().project(&[0, 1]).unwrap();
        target
            .create(NORMAL_TABLE_NAME, TableType::NormalTable(schema))
            .await
            .unwrap();

        let sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(target_actual_result.num_rows(), 0);

        source
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT id, name FROM {NORMAL_TABLE_NAME}");
        source
            .copy(&copy_sql, &mut target, NORMAL_TABLE_NAME)
            .await
            .unwrap();

        let source_actual_result = data_folder_read(&mut source, &sql).await.unwrap();
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();

        assert_eq!(source_actual_result, normal_table_data());
        assert_eq!(
            target_actual_result,
            source_actual_result.project(&[0, 1]).unwrap()
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_normal_table_with_invalid_schema() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_normal_table().await;

        source
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT id, name FROM {NORMAL_TABLE_NAME}");
        let result = source.copy(&copy_sql, &mut target, NORMAL_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The schema of the data to copy does not \
                match the schema of {NORMAL_TABLE_NAME}."
            )
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_time_series_table_to_normal_table() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;

        // Create a normal table that has the same schema as the time series table in source.
        let temp_dir = tempfile::tempdir().unwrap();
        let mut target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let schema = time_series_table_schema();
        target
            .create(NORMAL_TABLE_NAME, TableType::NormalTable(schema))
            .await
            .unwrap();

        let target_sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let target_actual_result = data_folder_read(&mut target, &target_sql).await.unwrap();
        assert_eq!(target_actual_result.num_rows(), 0);

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT * FROM {TIME_SERIES_TABLE_NAME}");
        source
            .copy(&copy_sql, &mut target, NORMAL_TABLE_NAME)
            .await
            .unwrap();

        let source_actual_result = data_folder_read(&mut source, &copy_sql).await.unwrap();
        let target_actual_result = data_folder_read(&mut target, &target_sql).await.unwrap();

        assert_eq!(source_actual_result, sorted_time_series_table_data());
        assert_eq!(target_actual_result, source_actual_result);
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_time_series_table() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_time_series_table().await;

        source
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let result = source
            .copy(&copy_sql, &mut target, TIME_SERIES_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {TIME_SERIES_TABLE_NAME} is not a normal table.")
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_normal_table_to_missing_table() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        source
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let copy_sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let result = source
            .copy(&copy_sql, &mut target, MISSING_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid Argument Error: {MISSING_TABLE_NAME} is not a normal table.")
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_missing_table_to_normal_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut source = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let (_temp_dir, mut target) = create_data_folder_with_normal_table().await;

        let copy_sql = format!("SELECT * FROM {MISSING_TABLE_NAME}");
        let result = source.copy(&copy_sql, &mut target, NORMAL_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "DataFusion Error: Error during planning: \
                table 'datafusion.public.{MISSING_TABLE_NAME}' not found"
            )
        );
    }

    #[tokio::test]
    async fn test_copy_normal_table_from_data_folder_to_client() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;
        let mut target_client = lazy_modelardb_client();

        let copy_sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let result = source
            .copy(&copy_sql, &mut target_client, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: target is not a data folder.".to_owned()
        );
    }

    #[tokio::test]
    async fn test_drop_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        assert!(
            data_folder
                .session_context
                .table_exist(NORMAL_TABLE_NAME)
                .unwrap()
        );

        data_folder.drop(NORMAL_TABLE_NAME).await.unwrap();

        // Verify that the normal table was deregistered from Apache DataFusion.
        assert!(
            !data_folder
                .session_context
                .table_exist(NORMAL_TABLE_NAME)
                .unwrap()
        );

        // Verify that the normal table was dropped from the metadata Delta Lake.
        assert!(
            !data_folder
                .table_metadata_manager
                .is_normal_table(NORMAL_TABLE_NAME)
                .await
                .unwrap()
        );

        // Verify that the normal table was dropped from the Delta Lake.
        assert!(
            data_folder
                .delta_lake
                .delta_table(NORMAL_TABLE_NAME)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_drop_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        assert!(
            data_folder
                .session_context
                .table_exist(TIME_SERIES_TABLE_NAME)
                .unwrap()
        );

        data_folder.drop(TIME_SERIES_TABLE_NAME).await.unwrap();

        // Verify that the time series table was deregistered from Apache DataFusion.
        assert!(
            !data_folder
                .session_context
                .table_exist(TIME_SERIES_TABLE_NAME)
                .unwrap()
        );

        // Verify that the time series table was dropped from the metadata Delta Lake.
        assert!(
            !data_folder
                .table_metadata_manager
                .is_time_series_table(TIME_SERIES_TABLE_NAME)
                .await
                .unwrap()
        );

        // Verify that the time series table was dropped from the Delta Lake.
        assert!(
            data_folder
                .delta_lake
                .delta_table(TIME_SERIES_TABLE_NAME)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_drop_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder.drop(MISSING_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "ModelarDB Storage Error: Invalid Argument Error: \
                Table with name '{MISSING_TABLE_NAME}' does not exist."
            )
        );
    }

    #[tokio::test]
    async fn test_truncate_normal_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        let mut delta_table = data_folder
            .delta_lake
            .delta_table(NORMAL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 1);

        data_folder.truncate(NORMAL_TABLE_NAME).await.unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);

        // Verify that the normal table still exists.
        assert_normal_table_exists(&data_folder, NORMAL_TABLE_NAME, normal_table_schema()).await;
    }

    #[tokio::test]
    async fn test_truncate_time_series_table() {
        let (_temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let mut delta_table = data_folder
            .delta_lake
            .delta_table(TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 2);

        data_folder.truncate(TIME_SERIES_TABLE_NAME).await.unwrap();

        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);

        // Verify that the time series table still exists.
        assert_time_series_table_exists(
            &data_folder,
            TIME_SERIES_TABLE_NAME,
            time_series_table_schema(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_truncate_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder.truncate(MISSING_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: Table with name '{MISSING_TABLE_NAME}' does not exist."
            )
        );
    }

    #[tokio::test]
    async fn test_vacuum_normal_table() {
        // env::set_var is safe to call in a single-threaded program.
        unsafe {
            let _mutex_guard = SET_VAR_LOCK.lock();
            env::set_var("MODELARDBD_RETENTION_PERIOD_IN_SECONDS", "0");
        }

        let (temp_dir, mut data_folder) = create_data_folder_with_normal_table().await;

        data_folder
            .write(NORMAL_TABLE_NAME, normal_table_data())
            .await
            .unwrap();

        data_folder.truncate(NORMAL_TABLE_NAME).await.unwrap();

        // The files should still exist on disk even though they are no longer active.
        let table_path = format!(
            "{}/tables/{}",
            temp_dir.path().to_str().unwrap(),
            NORMAL_TABLE_NAME
        );
        let files = std::fs::read_dir(&table_path).unwrap();
        assert_eq!(files.count(), 2);

        data_folder.vacuum(NORMAL_TABLE_NAME).await.unwrap();

        // Only the _delta_log folder should remain.
        let files = std::fs::read_dir(&table_path).unwrap();
        assert_eq!(files.count(), 1);
    }

    #[tokio::test]
    async fn test_vacuum_time_series_table() {
        // env::set_var is safe to call in a single-threaded program.
        unsafe {
            let _mutex_guard = SET_VAR_LOCK.lock();
            env::set_var("MODELARDBD_RETENTION_PERIOD_IN_SECONDS", "0");
        }

        let (temp_dir, mut data_folder) = create_data_folder_with_time_series_table().await;

        data_folder
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        data_folder.truncate(TIME_SERIES_TABLE_NAME).await.unwrap();

        // The files should still exist on disk even though they are no longer active.
        let column_path = format!(
            "{}/tables/{}/field_column=3",
            temp_dir.path().to_str().unwrap(),
            TIME_SERIES_TABLE_NAME
        );
        let files = std::fs::read_dir(&column_path).unwrap();
        assert_eq!(files.count(), 1);

        data_folder.vacuum(TIME_SERIES_TABLE_NAME).await.unwrap();

        // No files should remain in the column folder.
        let files = std::fs::read_dir(&column_path).unwrap();
        assert_eq!(files.count(), 0);
    }

    #[tokio::test]
    async fn test_vacuum_missing_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let result = data_folder.vacuum(MISSING_TABLE_NAME).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: Table with name '{MISSING_TABLE_NAME}' does not exist."
            )
        );
    }

    #[tokio::test]
    async fn test_move_normal_table_to_normal_table() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_normal_table().await;

        let sql = format!("SELECT * FROM {NORMAL_TABLE_NAME}");
        let actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(actual_result.num_rows(), 0);

        let expected_result = normal_table_data();
        source
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let mut delta_table = source
            .delta_lake
            .delta_table(NORMAL_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 1);

        source
            .r#move(NORMAL_TABLE_NAME, &target, NORMAL_TABLE_NAME)
            .await
            .unwrap();

        // Verify that the data was deleted but the normal table still exists.
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);
        assert_normal_table_exists(&source, NORMAL_TABLE_NAME, normal_table_schema()).await;

        // Verify that the normal table data was moved to the new data folder.
        let actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(actual_result, expected_result);
    }

    async fn assert_normal_table_exists(
        data_folder: &DataFolder,
        table_name: &str,
        expected_schema: Schema,
    ) {
        // Verify that the normal table exists in the Delta Lake.
        let delta_table = data_folder
            .delta_lake
            .delta_table(table_name)
            .await
            .unwrap();

        let actual_schema = TableProvider::schema(&delta_table);
        assert_eq!(actual_schema, Arc::new(expected_schema));

        // Verify that the normal table exists in the metadata Delta Lake.
        assert!(
            data_folder
                .table_metadata_manager
                .is_normal_table(table_name)
                .await
                .unwrap()
        );

        // Verify that the normal table is registered with Apache DataFusion.
        assert!(data_folder.session_context.table_exist(table_name).unwrap())
    }

    #[tokio::test]
    async fn test_move_normal_table_to_normal_table_with_invalid_schema() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        target
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(invalid_table_schema()),
            )
            .await
            .unwrap();

        let expected_result = normal_table_data();
        source
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let result = source
            .r#move(NORMAL_TABLE_NAME, &target, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The schema of {NORMAL_TABLE_NAME} \
                does not match the schema of {NORMAL_TABLE_NAME}."
            )
        );

        assert_table_not_moved(
            &mut source,
            NORMAL_TABLE_NAME,
            expected_result,
            Some(&mut target),
            Some(NORMAL_TABLE_NAME),
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_normal_table_to_time_series_table() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_time_series_table().await;

        let expected_result = normal_table_data();
        source
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let result = source
            .r#move(NORMAL_TABLE_NAME, &target, TIME_SERIES_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {NORMAL_TABLE_NAME} and {TIME_SERIES_TABLE_NAME} are not \
                both normal tables or time series tables."
            )
        );

        assert_table_not_moved(
            &mut source,
            NORMAL_TABLE_NAME,
            expected_result,
            Some(&mut target),
            Some(TIME_SERIES_TABLE_NAME),
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_normal_table_to_missing_table() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let expected_result = normal_table_data();
        source
            .write(NORMAL_TABLE_NAME, expected_result.clone())
            .await
            .unwrap();

        let result = source
            .r#move(NORMAL_TABLE_NAME, &target, MISSING_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {NORMAL_TABLE_NAME} and {MISSING_TABLE_NAME} are not \
                both normal tables or time series tables."
            )
        );

        assert_table_not_moved(&mut source, NORMAL_TABLE_NAME, expected_result, None, None).await;
    }

    #[tokio::test]
    async fn test_move_time_series_table_to_time_series_table() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_time_series_table().await;

        let sql = format!("SELECT * FROM {TIME_SERIES_TABLE_NAME}");
        let target_actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(target_actual_result.num_rows(), 0);

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let mut delta_table = source
            .delta_lake
            .delta_table(TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 2);

        source
            .r#move(TIME_SERIES_TABLE_NAME, &target, TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        // Verify that the data was deleted but the time series table still exists.
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);
        assert_time_series_table_exists(
            &source,
            TIME_SERIES_TABLE_NAME,
            time_series_table_schema(),
        )
        .await;

        // Verify that the time series table data was moved to the new data folder.
        let actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(actual_result, sorted_time_series_table_data());
    }

    #[tokio::test]
    async fn test_move_time_series_table_to_time_series_table_with_generated_column() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;
        let (_temp_dir, mut target) =
            create_data_folder_with_time_series_table_with_generated_column().await;

        let sql = format!("SELECT * FROM {TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME}");
        let actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_eq!(actual_result.num_rows(), 0);

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let mut delta_table = source
            .delta_lake
            .delta_table(TIME_SERIES_TABLE_NAME)
            .await
            .unwrap();

        assert_eq!(delta_table.get_files_count(), 2);

        // Even though the query schemas are different, the data should still be moved.
        source
            .r#move(
                TIME_SERIES_TABLE_NAME,
                &target,
                TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME,
            )
            .await
            .unwrap();

        // Verify that the data was deleted but the time series table still exists.
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 0);
        assert_time_series_table_exists(
            &source,
            TIME_SERIES_TABLE_NAME,
            time_series_table_schema(),
        )
        .await;

        // Verify that the time series table data was moved to the new data folder.
        let actual_result = data_folder_read(&mut target, &sql).await.unwrap();
        assert_generated_column_result_eq(actual_result, sorted_time_series_table_data());
    }

    async fn assert_time_series_table_exists(
        data_folder: &DataFolder,
        table_name: &str,
        expected_schema: Schema,
    ) -> TimeSeriesTableMetadata {
        // Verify that the time series table exists in the Delta Lake.
        assert!(data_folder.delta_lake.delta_table(table_name).await.is_ok());

        // Verify that the time series table exists in the metadata Delta Lake with the correct schema.
        let time_series_table_metadata = data_folder
            .table_metadata_manager
            .time_series_table_metadata_for_time_series_table(table_name)
            .await
            .unwrap();

        assert_eq!(time_series_table_metadata.name, table_name);
        assert_eq!(*time_series_table_metadata.query_schema, expected_schema);

        // Verify that the time series table is registered with Apache DataFusion.
        assert!(data_folder.session_context.table_exist(table_name).unwrap());

        time_series_table_metadata
    }

    fn assert_generated_column_result_eq(
        mut actual_result: RecordBatch,
        expected_result_without_generated_column: RecordBatch,
    ) {
        let generated_column_result = actual_result.remove_column(5);
        assert_eq!(actual_result, expected_result_without_generated_column);
        assert_eq!(
            *generated_column_result
                .as_any()
                .downcast_ref::<ValueArray>()
                .unwrap(),
            ValueArray::from(vec![61.0, 63.0, 65.0, 129.0, 127.0, 125.0])
        );
    }

    #[tokio::test]
    async fn test_move_time_series_table_to_time_series_table_with_invalid_schema() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        target
            .create(
                TIME_SERIES_TABLE_NAME,
                TableType::TimeSeriesTable(invalid_table_schema(), HashMap::new(), HashMap::new()),
            )
            .await
            .unwrap();

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let result = source
            .r#move(TIME_SERIES_TABLE_NAME, &target, TIME_SERIES_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: The schema of {TIME_SERIES_TABLE_NAME} \
                does not match the schema of {TIME_SERIES_TABLE_NAME}."
            )
        );

        assert_table_not_moved(
            &mut source,
            TIME_SERIES_TABLE_NAME,
            sorted_time_series_table_data(),
            Some(&mut target),
            Some(TIME_SERIES_TABLE_NAME),
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_time_series_table_to_normal_table() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;
        let (_temp_dir, mut target) = create_data_folder_with_normal_table().await;

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let result = source
            .r#move(TIME_SERIES_TABLE_NAME, &target, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {TIME_SERIES_TABLE_NAME} and {NORMAL_TABLE_NAME} \
                are not both normal tables or time series tables."
            )
        );

        assert_table_not_moved(
            &mut source,
            TIME_SERIES_TABLE_NAME,
            sorted_time_series_table_data(),
            Some(&mut target),
            Some(NORMAL_TABLE_NAME),
        )
        .await;
    }

    #[tokio::test]
    async fn test_move_time_series_table_to_missing_table() {
        let (_temp_dir, mut source) = create_data_folder_with_time_series_table().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let target = DataFolder::open_local(temp_dir.path()).await.unwrap();

        source
            .write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            .await
            .unwrap();

        let result = source
            .r#move(TIME_SERIES_TABLE_NAME, &target, MISSING_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {TIME_SERIES_TABLE_NAME} and {MISSING_TABLE_NAME} \
                are not both normal tables or time series tables."
            )
        );

        assert_table_not_moved(
            &mut source,
            TIME_SERIES_TABLE_NAME,
            sorted_time_series_table_data(),
            None,
            None,
        )
        .await;
    }

    async fn assert_table_not_moved(
        source: &mut DataFolder,
        source_table_name: &str,
        expected_result: RecordBatch,
        maybe_target: Option<&mut DataFolder>,
        maybe_target_table_name: Option<&str>,
    ) {
        let source_sql = format!("SELECT * FROM {source_table_name}");
        let source_actual_result = data_folder_read(source, &source_sql).await.unwrap();
        assert_eq!(source_actual_result, expected_result);

        if let (Some(target), Some(target_table_name)) = (maybe_target, maybe_target_table_name) {
            let target_sql = format!("SELECT * FROM {target_table_name}");
            let target_actual_result = data_folder_read(target, &target_sql).await.unwrap();
            assert_eq!(target_actual_result.num_rows(), 0);
        }
    }

    async fn data_folder_read(data_folder: &mut DataFolder, sql: &str) -> Result<RecordBatch> {
        let record_batch_stream = data_folder.read(sql).await?;
        record_batch_stream_to_record_batch(record_batch_stream).await
    }

    #[tokio::test]
    async fn test_move_missing_table_to_time_series_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut source = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let (_temp_dir, target) = create_data_folder_with_time_series_table().await;

        let result = source
            .r#move(MISSING_TABLE_NAME, &target, TIME_SERIES_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Invalid Argument Error: {MISSING_TABLE_NAME} and {TIME_SERIES_TABLE_NAME} \
                are not both normal tables or time series tables."
            )
        );
    }

    #[tokio::test]
    async fn test_move_from_data_folder_to_client() {
        let (_temp_dir, mut source) = create_data_folder_with_normal_table().await;
        let target_client = lazy_modelardb_client();

        let result = source
            .r#move(NORMAL_TABLE_NAME, &target_client, NORMAL_TABLE_NAME)
            .await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: target is not a data folder.".to_owned()
        );
    }

    async fn create_data_folder_with_normal_table() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        data_folder
            .create(
                NORMAL_TABLE_NAME,
                TableType::NormalTable(normal_table_schema()),
            )
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    fn lazy_modelardb_client() -> Client {
        Client {
            node: Node::Server("localhost".to_owned()),
            flight_client: FlightServiceClient::new(
                Channel::from_static("localhost").connect_lazy(),
            ),
        }
    }

    fn normal_table_data() -> RecordBatch {
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]);
        let ages = Int8Array::from(vec![20, 30, 40, 25, 35, 45]);
        let heights = Int16Array::from(vec![160, 170, 180, 165, 175, 185]);
        let weights = Int16Array::from(vec![60, 70, 80, 65, 75, 85]);

        RecordBatch::try_new(
            Arc::new(normal_table_schema()),
            vec![
                Arc::new(ids),
                Arc::new(names),
                Arc::new(ages),
                Arc::new(heights),
                Arc::new(weights),
            ],
        )
        .unwrap()
    }

    fn normal_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int8, false),
            Field::new("height", DataType::Int16, false),
            Field::new("weight", DataType::Int16, false),
        ])
    }

    async fn create_data_folder_with_time_series_table() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let table_type =
            TableType::TimeSeriesTable(time_series_table_schema(), HashMap::new(), HashMap::new());

        data_folder
            .create(TIME_SERIES_TABLE_NAME, table_type)
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    fn sorted_time_series_table_data() -> RecordBatch {
        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let physical_sort_exprs = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("tag_1", 1)),
                options: sort_options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("tag_2", 2)),
                options: sort_options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("timestamp", 0)),
                options: sort_options,
            },
        ];

        sort::sort_batch(
            &time_series_table_data(),
            &LexOrdering::new(physical_sort_exprs),
            None,
        )
        .unwrap()
    }

    fn time_series_table_data() -> RecordBatch {
        let timestamps = TimestampArray::from(vec![100, 100, 200, 200, 300, 300]);
        let tag_1 = StringArray::from(vec!["tag_x", "tag_y", "tag_x", "tag_y", "tag_x", "tag_y"]);
        let tag_2 = StringArray::from(vec!["tag_a", "tag_b", "tag_a", "tag_b", "tag_a", "tag_b"]);
        let field_1 = ValueArray::from(vec![37.0, 73.0, 38.0, 72.0, 39.0, 71.0]);
        let field_2 = ValueArray::from(vec![24.0, 56.0, 25.0, 55.0, 26.0, 54.0]);

        RecordBatch::try_new(
            Arc::new(time_series_table_schema()),
            vec![
                Arc::new(timestamps),
                Arc::new(tag_1),
                Arc::new(tag_2),
                Arc::new(field_1),
                Arc::new(field_2),
            ],
        )
        .unwrap()
    }

    fn time_series_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("tag_1", DataType::Utf8, false),
            Field::new("tag_2", DataType::Utf8, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ])
    }

    async fn create_data_folder_with_time_series_table_with_generated_column()
    -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut data_folder = DataFolder::open_local(temp_dir.path()).await.unwrap();

        let generated_columns = vec![("generated".to_owned(), "field_1 + field_2".to_owned())];
        let table_type = TableType::TimeSeriesTable(
            time_series_table_with_generated_column_schema(),
            HashMap::new(),
            generated_columns.into_iter().collect(),
        );

        data_folder
            .create(TIME_SERIES_TABLE_WITH_GENERATED_COLUMN_NAME, table_type)
            .await
            .unwrap();

        (temp_dir, data_folder)
    }

    fn time_series_table_with_generated_column_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("tag_1", DataType::Utf8, false),
            Field::new("tag_2", DataType::Utf8, false),
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
            Field::new("generated", ArrowValue::DATA_TYPE, false),
        ])
    }

    fn invalid_table_data() -> RecordBatch {
        let timestamps = TimestampArray::from(vec![100, 100, 200, 200, 300, 300]);
        let tag = StringArray::from(vec!["tag_x", "tag_y", "tag_x", "tag_y", "tag_x", "tag_y"]);
        let field = ValueArray::from(vec![37.0, 73.0, 38.0, 72.0, 39.0, 71.0]);

        RecordBatch::try_new(
            Arc::new(invalid_table_schema()),
            vec![Arc::new(timestamps), Arc::new(tag), Arc::new(field)],
        )
        .unwrap()
    }

    fn invalid_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("field", ArrowValue::DATA_TYPE, false),
        ])
    }
}
