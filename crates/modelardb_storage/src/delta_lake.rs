/* Copyright 2024 The ModelarDB Contributors
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

//! Implementation of the type used to interact with local and remote storage through a Delta Lake.

use std::collections::HashMap;
use std::fs;
use std::path::Path as StdPath;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::TimeDelta;
use dashmap::DashMap;
use datafusion::catalog::TableProvider;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::format::SortingColumn;
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Add, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use futures::{StreamExt, TryStreamExt};
use modelardb_types::flight::protocol;
use modelardb_types::schemas::{COMPRESSED_SCHEMA, FIELD_COLUMN};
use modelardb_types::types::TimeSeriesTableMetadata;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use url::Url;
use uuid::Uuid;

use crate::error::{ModelarDbStorageError, Result};
use crate::{METADATA_FOLDER, TABLE_FOLDER, apache_parquet_writer_properties};

/// Functionality for managing Delta Lake tables in a local folder or an object store.
pub struct DeltaLake {
    /// URL to access the root of the Delta Lake.
    location: String,
    /// Storage options required to access Delta Lake.
    storage_options: HashMap<String, String>,
    /// [`ObjectStore`] to access the root of the Delta Lake.
    object_store: Arc<dyn ObjectStore>,
    /// Cache of Delta tables to avoid opening the same table multiple times.
    delta_table_cache: DashMap<String, DeltaTable>,
}

impl DeltaLake {
    /// Create a new [`DeltaLake`] that manages the Delta tables at `local_url`. If `local_url` has
    /// the schema `file` or no schema, the Delta tables are managed in a local data folder. If
    /// `local_url` has the schema `memory`, the Delta tables are managed in memory. Return
    /// [`ModelarDbStorageError`] if `local_url` cannot be parsed.
    pub fn try_from_local_url(local_url: &str) -> Result<Self> {
        match local_url.split_once("://") {
            None => Self::try_from_local_path(StdPath::new(local_url)),
            Some(("file", local_path)) => Self::try_from_local_path(StdPath::new(local_path)),
            Some(("memory", _)) => Ok(Self::new_in_memory()),
            _ => Err(ModelarDbStorageError::InvalidArgument(format!(
                "{local_url} is not a valid local URL."
            ))),
        }
    }

    /// Create a new [`DeltaLake`] that manages the Delta tables in memory.
    pub fn new_in_memory() -> Self {
        Self {
            location: "memory:///modelardb".to_owned(),
            storage_options: HashMap::new(),
            object_store: Arc::new(InMemory::new()),
            delta_table_cache: DashMap::new(),
        }
    }

    /// Create a new [`DeltaLake`] that manages the Delta tables in `data_folder_path`. Returns a
    /// [`ModelarDbStorageError`] if `data_folder_path` does not exist and could not be created.
    pub fn try_from_local_path(data_folder_path: &StdPath) -> Result<Self> {
        // Ensure the directories in the path exists as LocalFileSystem otherwise returns an error.
        fs::create_dir_all(data_folder_path)
            .map_err(|error| DeltaTableError::generic(error.to_string()))?;

        // Use with_automatic_cleanup to ensure empty directories are deleted automatically.
        let object_store = LocalFileSystem::new_with_prefix(data_folder_path)
            .map_err(|error| DeltaTableError::generic(error.to_string()))?
            .with_automatic_cleanup(true);

        let location = data_folder_path
            .to_str()
            .ok_or_else(|| DeltaTableError::generic("Local data folder path is not UTF-8."))?
            .to_owned();

        Ok(Self {
            location,
            storage_options: HashMap::new(),
            object_store: Arc::new(object_store),
            delta_table_cache: DashMap::new(),
        })
    }

    /// Create a new [`DeltaLake`] that manages Delta tables in the remote object store given by
    /// `storage_configuration`. Returns [`ModelarDbStorageError`] if a connection to the specified
    /// object store could not be created.
    pub fn try_remote_from_storage_configuration(
        storage_configuration: protocol::manager_metadata::StorageConfiguration,
    ) -> Result<Self> {
        match storage_configuration {
            protocol::manager_metadata::StorageConfiguration::S3Configuration(s3_configuration) => {
                // Register the S3 storage handlers to allow the use of Amazon S3 object stores.
                // This is required at runtime to initialize the S3 storage implementation in the
                // deltalake_aws storage subcrate.
                deltalake::aws::register_handlers(None);

                Self::try_from_s3_configuration(
                    s3_configuration.endpoint,
                    s3_configuration.bucket_name,
                    s3_configuration.access_key_id,
                    s3_configuration.secret_access_key,
                )
            }
            protocol::manager_metadata::StorageConfiguration::AzureConfiguration(
                azure_configuration,
            ) => Self::try_from_azure_configuration(
                azure_configuration.account_name,
                azure_configuration.access_key,
                azure_configuration.container_name,
            ),
        }
    }

    /// Create a new [`DeltaLake`] that manages the Delta tables in an object store with an
    /// S3-compatible API. Returns a [`ModelarDbStorageError`] if a connection to the object store
    /// could not be made.
    pub fn try_from_s3_configuration(
        endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self> {
        let location = format!("s3://{bucket_name}");

        // TODO: Determine if it is safe to use AWS_S3_ALLOW_UNSAFE_RENAME.
        let storage_options = HashMap::from([
            ("aws_access_key_id".to_owned(), access_key_id),
            ("aws_secret_access_key".to_owned(), secret_access_key),
            ("aws_endpoint_url".to_owned(), endpoint),
            ("aws_bucket_name".to_owned(), bucket_name),
            ("aws_s3_allow_unsafe_rename".to_owned(), "true".to_owned()),
        ]);

        let url = Url::parse(&location)
            .map_err(|error| ModelarDbStorageError::InvalidArgument(error.to_string()))?;

        // Build the Amazon S3 object store with the given storage options manually to allow http.
        let object_store = storage_options
            .iter()
            .fold(
                AmazonS3Builder::new()
                    .with_url(url.to_string())
                    .with_allow_http(true),
                |builder, (key, value)| match key.parse() {
                    Ok(k) => builder.with_config(k, value),
                    Err(_) => builder,
                },
            )
            .build()?;

        Ok(DeltaLake {
            location,
            storage_options,
            object_store: Arc::new(object_store),
            delta_table_cache: DashMap::new(),
        })
    }

    /// Create a new [`DeltaLake`] that manages the Delta tables in an object store with an
    /// Azure-compatible API. Returns a [`ModelarDbStorageError`] if a connection to the object
    /// store could not be made.
    pub fn try_from_azure_configuration(
        account_name: String,
        access_key: String,
        container_name: String,
    ) -> Result<Self> {
        let location = format!("az://{container_name}");

        // TODO: Needs to be tested.
        let storage_options = HashMap::from([
            ("azure_storage_account_name".to_owned(), account_name),
            ("azure_storage_account_key".to_owned(), access_key),
            ("azure_container_name".to_owned(), container_name),
        ]);
        let url = Url::parse(&location)
            .map_err(|error| ModelarDbStorageError::InvalidArgument(error.to_string()))?;
        let (object_store, _path) = object_store::parse_url_opts(&url, &storage_options)?;

        Ok(DeltaLake {
            location,
            storage_options,
            object_store: Arc::new(object_store),
            delta_table_cache: DashMap::new(),
        })
    }

    /// Return an [`ObjectStore`] to access the root of the Delta Lake.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    /// Return a [`DeltaTable`] for manipulating the metadata table with `table_name` in the
    /// Delta Lake, or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be
    /// established or the table does not exist.
    pub async fn metadata_delta_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.location_of_metadata_table(table_name);
        self.delta_table_from_path(&table_path).await
    }

    /// Return a [`DeltaTable`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or the
    /// table does not exist.
    pub async fn delta_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.location_of_compressed_table(table_name);
        self.delta_table_from_path(&table_path).await
    }

    /// Return a [`DeltaOps`] for manipulating the metadata table with `table_name` in the Delta
    /// Lake, or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established
    /// or the table does not exist.
    pub async fn metadata_delta_ops(&self, table_name: &str) -> Result<DeltaOps> {
        let table_path = self.location_of_metadata_table(table_name);
        self.delta_table_from_path(&table_path)
            .await
            .map(Into::into)
    }

    /// Return a [`DeltaOps`] for manipulating the table with `table_name` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or the
    /// table does not exist.
    pub async fn delta_ops(&self, table_name: &str) -> Result<DeltaOps> {
        let table_path = self.location_of_compressed_table(table_name);
        self.delta_table_from_path(&table_path)
            .await
            .map(Into::into)
    }

    /// Return a [`DeltaTable`] for manipulating the table at `table_path` in the Delta Lake, or a
    /// [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or the
    /// table does not exist.
    async fn delta_table_from_path(&self, table_path: &str) -> Result<DeltaTable> {
        // Use the cache if possible and load to get the latest table data.
        if let Some(mut delta_table) = self.delta_table_cache.get_mut(table_path) {
            delta_table.load().await?;
            Ok(delta_table.clone())
        } else {
            // If the table is not in the cache, open it and add it to the cache before returning.
            let delta_table = deltalake::open_table_with_storage_options(
                &table_path,
                self.storage_options.clone(),
            )
            .await?;

            self.delta_table_cache
                .insert(table_path.to_owned(), delta_table.clone());

            Ok(delta_table)
        }
    }

    /// Return a [`DeltaTableWriter`] for writing to the time series table with `delta_table` in the
    /// Delta Lake, or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be
    /// established or the table does not exist.
    pub async fn time_series_table_writer(
        &self,
        delta_table: DeltaTable,
    ) -> Result<DeltaTableWriter> {
        let partition_columns = vec![FIELD_COLUMN.to_owned()];

        // Specify that the file must be sorted by the tag columns and then by start_time.
        let base_compressed_schema_len = COMPRESSED_SCHEMA.0.fields().len();
        let compressed_schema_len = TableProvider::schema(&delta_table).fields().len();
        let sorting_columns_len = (compressed_schema_len - base_compressed_schema_len) + 1;
        let mut sorting_columns = Vec::with_capacity(sorting_columns_len);

        // Compressed segments have the tag columns at the end of the schema.
        for tag_column_index in base_compressed_schema_len..compressed_schema_len {
            sorting_columns.push(SortingColumn::new(tag_column_index as i32, false, false));
        }

        // Compressed segments store the first timestamp in the second column.
        sorting_columns.push(SortingColumn::new(1, false, false));

        let writer_properties = apache_parquet_writer_properties(Some(sorting_columns));
        DeltaTableWriter::try_new(delta_table, partition_columns, writer_properties)
    }

    /// Return a [`DeltaTableWriter`] for writing to the table with `delta_table` in the Delta Lake,
    /// or a [`ModelarDbStorageError`] if a connection to the Delta Lake cannot be established or
    /// the table does not exist.
    pub async fn normal_or_metadata_table_writer(
        &self,
        delta_table: DeltaTable,
    ) -> Result<DeltaTableWriter> {
        let writer_properties = apache_parquet_writer_properties(None);
        DeltaTableWriter::try_new(delta_table, vec![], writer_properties)
    }

    /// Create a Delta Lake table for a metadata table with `table_name` and `schema` if it does not
    /// already exist. If the metadata table could not be created, [`ModelarDbStorageError`] is
    /// returned. An error is not returned if the metadata table already exists.
    pub async fn create_metadata_table(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<DeltaTable> {
        self.create_table(
            table_name,
            schema,
            &[],
            self.location_of_metadata_table(table_name),
            SaveMode::Ignore,
        )
        .await
    }

    /// Create a Delta Lake table for a normal table with `table_name` and `schema` if it does not
    /// already exist. If the normal table could not be created, e.g., because it already exists,
    /// [`ModelarDbStorageError`] is returned.
    pub async fn create_normal_table(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<DeltaTable> {
        self.create_table(
            table_name,
            schema,
            &[],
            self.location_of_compressed_table(table_name),
            SaveMode::ErrorIfExists,
        )
        .await
    }

    /// Create a Delta Lake table for a time series table with `time_series_table_metadata` if it
    /// does not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbStorageError`] if it could not.
    pub async fn create_time_series_table(
        &self,
        time_series_table_metadata: &TimeSeriesTableMetadata,
    ) -> Result<DeltaTable> {
        self.create_table(
            &time_series_table_metadata.name,
            &time_series_table_metadata.compressed_schema,
            &[FIELD_COLUMN.to_owned()],
            self.location_of_compressed_table(&time_series_table_metadata.name),
            SaveMode::ErrorIfExists,
        )
        .await
    }

    /// Create a Delta Lake table with `table_name`, `schema`, and `partition_columns` if it does
    /// not already exist. Returns [`DeltaTable`] if the table could be created and
    /// [`ModelarDbStorageError`] if it could not.
    async fn create_table(
        &self,
        table_name: &str,
        schema: &Schema,
        partition_columns: &[String],
        location: String,
        save_mode: SaveMode,
    ) -> Result<DeltaTable> {
        let mut columns: Vec<StructField> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let field: &Field = field;

            // Delta Lake does not support unsigned integers. Thus tables containing the Apache
            // Arrow types UInt8, UInt16, UInt32, and UInt64 must currently be rejected.
            match field.data_type() {
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    Err(DeltaTableError::SchemaMismatch {
                        msg: "Unsigned integers are not supported.".to_owned(),
                    })?
                }
                _ => {} // All possible cases must be handled.
            }

            let struct_field: StructField = field.try_into()?;
            columns.push(struct_field);
        }

        let delta_table = CreateBuilder::new()
            .with_storage_options(self.storage_options.clone())
            .with_table_name(table_name)
            .with_location(location.clone())
            .with_columns(columns)
            .with_partition_columns(partition_columns)
            .with_save_mode(save_mode)
            .await?;

        // If the table was created successfully, add it to the cache.
        self.delta_table_cache.insert(location, delta_table.clone());

        Ok(delta_table)
    }

    /// Drop the metadata Delta Lake table with `table_name` from the Delta Lake by deleting every
    /// file related to the table. The table folder cannot be deleted directly since folders do not
    /// exist in object stores and therefore cannot be operated upon. If the table was dropped
    /// successfully, the paths to the deleted files are returned, otherwise a
    /// [`ModelarDbStorageError`] is returned.
    pub async fn drop_metadata_table(&self, table_name: &str) -> Result<Vec<Path>> {
        let table_path = format!("{METADATA_FOLDER}/{table_name}");
        self.delete_table_files(&table_path).await
    }

    /// Drop the Delta Lake table with `table_name` from the Delta Lake by deleting every file
    /// related to the table. The table folder cannot be deleted directly since folders do not exist
    /// in object stores and therefore cannot be operated upon. If the table was dropped
    /// successfully, the paths to the deleted files are returned, otherwise a
    /// [`ModelarDbStorageError`] is returned.
    pub async fn drop_table(&self, table_name: &str) -> Result<Vec<Path>> {
        let table_path = format!("{TABLE_FOLDER}/{table_name}");
        self.delete_table_files(&table_path).await
    }

    /// Delete all files in the folder at `table_path` using bulk operations if available. If the
    /// files were deleted successfully, the paths to the deleted files are returned.
    async fn delete_table_files(&self, table_path: &str) -> Result<Vec<Path>> {
        let file_locations = self
            .object_store
            .list(Some(&Path::from(table_path)))
            .map_ok(|object_meta| object_meta.location)
            .boxed();

        let deleted_paths = self
            .object_store
            .delete_stream(file_locations)
            .try_collect::<Vec<Path>>()
            .await?;

        // Remove the table from the cache.
        let delta_table_path = format!("{}/{}", self.location, table_path);
        self.delta_table_cache.remove(&delta_table_path);

        Ok(deleted_paths)
    }

    /// Truncate the Delta Lake table with `table_name` by deleting all rows in the table. If the
    /// rows could not be deleted, a [`ModelarDbStorageError`] is returned.
    pub async fn truncate_table(&self, table_name: &str) -> Result<()> {
        let delta_table_ops = self.delta_ops(table_name).await?;
        delta_table_ops.delete().await?;

        Ok(())
    }

    /// Vacuum the Delta Lake table with `table_name` by deleting all files that are older than
    /// `retention_period_in_seconds` seconds. If the retention period is out of bounds or the
    /// files could not be deleted, a [`ModelarDbStorageError`] is returned.
    pub async fn vacuum_table(
        &self,
        table_name: &str,
        retention_period_in_seconds: usize,
    ) -> Result<()> {
        let delta_table_ops = self.delta_ops(table_name).await?;

        let retention_period = TimeDelta::new(retention_period_in_seconds as i64, 0).ok_or(
            ModelarDbStorageError::InvalidArgument(format!(
                "Retention period of {retention_period_in_seconds} seconds is out of bounds."
            )),
        )?;

        delta_table_ops
            .vacuum()
            .with_retention_period(retention_period)
            .with_enforce_retention_duration(false)
            .await?;

        Ok(())
    }

    /// Write `columns` to a metadata Delta Lake table with `table_name`. Returns an updated
    /// [`DeltaTable`] version if the file was written successfully, otherwise returns
    /// [`ModelarDbStorageError`].
    pub async fn write_columns_to_metadata_table(
        &self,
        table_name: &str,
        columns: Vec<ArrayRef>,
    ) -> Result<DeltaTable> {
        let delta_table = self.metadata_delta_table(table_name).await?;
        let record_batch = RecordBatch::try_new(TableProvider::schema(&delta_table), columns)?;
        let delta_table_writer = self.normal_or_metadata_table_writer(delta_table).await?;
        self.write_record_batches_to_table(delta_table_writer, vec![record_batch])
            .await
    }

    /// Write `record_batches` to a Delta Lake table for a normal table with `table_name`. Returns
    /// an updated [`DeltaTable`] version if the file was written successfully, otherwise returns
    /// [`ModelarDbStorageError`].
    pub async fn write_record_batches_to_normal_table(
        &self,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        let delta_table = self.delta_table(table_name).await?;
        let delta_table_writer = self.normal_or_metadata_table_writer(delta_table).await?;
        self.write_record_batches_to_table(delta_table_writer, record_batches)
            .await
    }

    /// Write `record_batches` with segments to a Delta Lake table for a time series table with
    /// `table_name`. Returns an updated [`DeltaTable`] if the file was written successfully,
    /// otherwise returns [`ModelarDbStorageError`].
    pub async fn write_compressed_segments_to_time_series_table(
        &self,
        table_name: &str,
        compressed_segments: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        let delta_table = self.delta_table(table_name).await?;
        let delta_table_writer = self.time_series_table_writer(delta_table).await?;
        self.write_record_batches_to_table(delta_table_writer, compressed_segments)
            .await
    }

    /// Write `record_batches` to the `delta_table_writer` and commit. Returns an updated
    /// [`DeltaTable`] if all `record_batches` are written and committed successfully, otherwise it
    /// rollback all writes done using `delta_table_writer` and returns [`ModelarDbStorageError`].
    async fn write_record_batches_to_table(
        &self,
        mut delta_table_writer: DeltaTableWriter,
        record_batches: Vec<RecordBatch>,
    ) -> Result<DeltaTable> {
        match delta_table_writer.write_all(&record_batches).await {
            Ok(_) => delta_table_writer.commit().await,
            Err(error) => {
                delta_table_writer.rollback().await?;
                Err(error)
            }
        }
    }

    /// Return the location of the compressed time series or normal table with `table_name`.
    fn location_of_compressed_table(&self, table_name: &str) -> String {
        format!("{}/{TABLE_FOLDER}/{table_name}", self.location)
    }

    /// Return the location of the metadata table with `table_name`.
    fn location_of_metadata_table(&self, table_name: &str) -> String {
        format!("{}/{METADATA_FOLDER}/{table_name}", self.location)
    }
}

/// Functionality for transactionally writing [`RecordBatches`](RecordBatch) to a Delta table stored
/// in an object store.
pub struct DeltaTableWriter {
    /// Delta table that all of the record batches will be written to.
    delta_table: DeltaTable,
    /// Checker that ensures all of the record batches match the table.
    delta_data_checker: DeltaDataChecker,
    /// Write operation that will be committed to the Delta table.
    delta_operation: DeltaOperation,
    /// Unique identifier for this write operation to the Delta table.
    operation_id: Uuid,
    /// Writes record batches to the Delta table as Apache Parquet files.
    delta_writer: DeltaWriter,
}

impl DeltaTableWriter {
    /// Create a new [`DeltaTableWriter`]. Returns a [`ModelarDbStorageError`] if the state of the
    /// Delta table cannot be loaded from `delta_table`.
    pub fn try_new(
        delta_table: DeltaTable,
        partition_columns: Vec<String>,
        writer_properties: WriterProperties,
    ) -> Result<Self> {
        // Checker for if record batches match the table’s invariants, constraints, and nullability.
        let snapshot = delta_table.snapshot()?;
        let delta_data_checker = DeltaDataChecker::new(snapshot);

        // Operation that will be committed.
        let delta_operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns.clone())
            },
            predicate: None,
        };

        // A UUID version 4 is used as the operation id to match the existing Operation trait in the
        // deltalake crate as it is pub(trait) and thus cannot be used directly in DeltaTableWriter.
        let operation_id = Uuid::new_v4();

        // Writer that will write the record batches.
        let object_store = delta_table.log_store().object_store(Some(operation_id));
        let table_schema: Arc<Schema> = TableProvider::schema(&delta_table);
        let num_indexed_cols = table_schema.fields.len() as i32;
        let writer_config = WriterConfig::new(
            table_schema,
            partition_columns,
            Some(writer_properties),
            None,
            None,
            num_indexed_cols,
            None,
        );
        let delta_writer = DeltaWriter::new(object_store, writer_config);

        Ok(Self {
            delta_table,
            delta_data_checker,
            delta_operation,
            operation_id,
            delta_writer,
        })
    }

    /// Write `record_batch` to the Delta table. Returns a [`ModelarDbStorageError`] if the
    /// [`RecordBatches`](RecordBatch) does not match the schema of the Delta table or if the
    /// writing fails.
    pub async fn write(&mut self, record_batch: &RecordBatch) -> Result<()> {
        self.delta_data_checker.check_batch(record_batch).await?;
        self.delta_writer.write(record_batch).await?;
        Ok(())
    }

    /// Write all `record_batches` to the Delta table. Returns a [`ModelarDbStorageError`] if one of
    /// the [`RecordBatches`](RecordBatch) does not match the schema of the Delta table or if the
    /// writing fails.
    pub async fn write_all(&mut self, record_batches: &[RecordBatch]) -> Result<()> {
        for record_batch in record_batches {
            self.write(record_batch).await?;
        }
        Ok(())
    }

    /// Consume the [`DeltaTableWriter`], finish the writing, and commit the files that have been
    /// written to the log. If an error occurs before the commit is finished, the already written
    /// files are deleted if possible. Returns a [`ModelarDbStorageError`] if an error occurs when
    /// finishing the writing, committing the files that have been written, deleting the written
    /// files, or updating the [`DeltaTable`].
    pub async fn commit(mut self) -> Result<DeltaTable> {
        // Write the remaining buffered files.
        let added_files = self.delta_writer.close().await?;

        // Clone added_files in case of rollback.
        let actions = added_files
            .clone()
            .into_iter()
            .map(Action::Add)
            .collect::<Vec<Action>>();

        // Prepare all inputs to the commit.
        let object_store = self.delta_table.object_store();
        let commit_properties = CommitProperties::default();
        let table_data = match self.delta_table.snapshot() {
            Ok(table_data) => table_data,
            Err(delta_table_error) => {
                delete_added_files(&object_store, added_files).await?;
                return Err(ModelarDbStorageError::DeltaLake(delta_table_error));
            }
        };
        let log_store = self.delta_table.log_store();

        // Construct the commit to be written.
        let commit_builder = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .with_operation_id(self.operation_id)
            .build(Some(table_data), log_store, self.delta_operation);

        // Write the commit to the Delta table.
        let _finalized_commit = match commit_builder.await {
            Ok(finalized_commit) => finalized_commit,
            Err(delta_table_error) => {
                delete_added_files(&object_store, added_files).await?;
                return Err(ModelarDbStorageError::DeltaLake(delta_table_error));
            }
        };

        // Return Delta table with the commit.
        self.delta_table.load().await?;
        Ok(self.delta_table)
    }

    /// Consume the [`DeltaTableWriter`], abort the writing, and delete all of the files that have
    /// already been written. Returns a [`ModelarDbStorageError`] if an error occurs when aborting
    /// the writing or deleting the files that have already been written. Rollback is not called
    /// automatically as drop() is not async and async_drop() is not yet a stable API.
    pub async fn rollback(self) -> Result<DeltaTable> {
        let object_store = self.delta_table.object_store();
        let added_files = self.delta_writer.close().await?;
        delete_added_files(&object_store, added_files).await?;
        Ok(self.delta_table)
    }
}

/// Delete the `added_files` from `object_store`. Returns a [`ModelarDbStorageError`] if a file
/// could not be deleted. It is a function instead of a method on [`DeltaTableWriter`] so it can be
/// called by [`DeltaTableWriter`] after the [`DeltaWriter`] is closed without lifetime issues.
async fn delete_added_files(object_store: &dyn ObjectStore, added_files: Vec<Add>) -> Result<()> {
    for add_file in added_files {
        let path: Path = Path::from(add_file.path);
        object_store.delete(&path).await?;
    }
    Ok(())
}
