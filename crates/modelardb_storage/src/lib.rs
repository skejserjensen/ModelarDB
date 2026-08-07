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

//! Utility functions to register metadata tables, normal tables, and time series tables with Apache
//! DataFusion and to read and write Apache Parquet files to and from an object store.

pub mod delta_lake;
pub mod error;
pub mod metadata;
mod optimizer;
pub mod parser;
mod query;

use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute;
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use bytes::Bytes;
use datafusion::catalog::TableProvider;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStream,
};
use datafusion::parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::format::SortingColumn;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use deltalake::DeltaTable;
use futures::StreamExt;
use modelardb_types::types::TimeSeriesTableMetadata;
use object_store::ObjectStore;
use object_store::path::Path;
use sqlparser::ast::Statement;

use crate::error::Result;
use crate::query::metadata_table::MetadataTable;
use crate::query::normal_table::NormalTable;
use crate::query::time_series_table::TimeSeriesTable;

/// The folder storing compressed table data in the data folders.
const TABLE_FOLDER: &str = "tables";

/// The folder storing metadata in the data folders.
const METADATA_FOLDER: &str = "metadata";

/// Create a new [`SessionContext`] for interacting with Apache DataFusion. The [`SessionContext`]
/// is constructed with the default configuration, default resource managers, and additional
/// optimizer rules that rewrite simple aggregate queries to be executed directly on the segments
/// containing metadata and models instead of on reconstructed data points created from the segments
/// for time series tables.
pub fn create_session_context() -> SessionContext {
    let mut session_state_builder = SessionStateBuilder::new().with_default_features();

    // Uses the rule method instead of the rules method as the rules method replaces the built-ins.
    for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
        session_state_builder =
            session_state_builder.with_physical_optimizer_rule(physical_optimizer_rule);
    }

    let session_state = session_state_builder.build();
    SessionContext::new_with_state(session_state)
}

/// Register the metadata table stored in `delta_table` with `table_name` in `session_context`. If
/// the metadata table could not be registered with Apache DataFusion, return
/// [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub fn register_metadata_table(
    session_context: &SessionContext,
    table_name: &str,
    delta_table: DeltaTable,
) -> Result<()> {
    let metadata_table = Arc::new(MetadataTable::new(delta_table));
    session_context.register_table(table_name, metadata_table)?;

    Ok(())
}

/// Register the normal table stored in `delta_table` with `table_name` and `data_sink` in
/// `session_context`. If the normal table could not be registered with Apache DataFusion, return
/// [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub fn register_normal_table(
    session_context: &SessionContext,
    table_name: &str,
    delta_table: DeltaTable,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let normal_table = Arc::new(NormalTable::new(delta_table, data_sink));
    session_context.register_table(table_name, normal_table)?;

    Ok(())
}

/// Register the time series table stored in `delta_table` with `time_series_table_metadata` and
/// `data_sink` in `session_context`. If the time series table could not be registered with Apache
/// DataFusion, return [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub fn register_time_series_table(
    session_context: &SessionContext,
    delta_table: DeltaTable,
    time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
    data_sink: Arc<dyn DataSink>,
) -> Result<()> {
    let time_series_table =
        TimeSeriesTable::new(delta_table, time_series_table_metadata.clone(), data_sink);

    session_context.register_table(&time_series_table_metadata.name, time_series_table)?;

    Ok(())
}

/// Return the [`Arc<TimeSeriesTableMetadata>`] of the table `maybe_time_series_table` if it is a
/// time series table, otherwise [`None`] is returned.
pub fn maybe_table_provider_to_time_series_table_metadata(
    maybe_time_series_table: Arc<dyn TableProvider>,
) -> Option<Arc<TimeSeriesTableMetadata>> {
    maybe_time_series_table
        .as_any()
        .downcast_ref::<TimeSeriesTable>()
        .map(|time_series_table| time_series_table.time_series_table_metadata())
}

/// Execute `statement` in `session_context` and return the result as a
/// [`SendableRecordBatchStream`]. If `statement` could not be executed successfully,
/// [`ModelarDbStorageError`](error::ModelarDbStorageError) is returned.
pub async fn execute_statement(
    session_context: &SessionContext,
    statement: Statement,
) -> Result<SendableRecordBatchStream> {
    let session_state = session_context.state();
    let df_statement = DFStatement::Statement(Box::new(statement));

    let logical_plan = session_state.statement_to_plan(df_statement).await?;
    let data_frame = session_context.execute_logical_plan(logical_plan).await?;
    let sendable_record_batch_stream = data_frame.execute_stream().await?;

    Ok(sendable_record_batch_stream)
}

/// Execute the SQL query `sql` in `session_context` and return the result as a single
/// [`RecordBatch`]. If the query could not be executed successfully, return
/// [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub async fn sql_and_concat(session_context: &SessionContext, sql: &str) -> Result<RecordBatch> {
    let dataframe = session_context.sql(sql).await?;
    let schema = Schema::from(dataframe.schema());

    let record_batches = dataframe.collect().await?;
    let record_batch = concat_batches(&schema.into(), &record_batches)?;

    Ok(record_batch)
}

/// Read all rows from the Apache Parquet file at the location given by `file_path` in
/// `object_store` and return them as a [`RecordBatch`]. If the file could not be read successfully,
/// [`ModelarDbStorageError`](error::ModelarDbStorageError) is returned.
pub async fn read_record_batch_from_apache_parquet_file(
    file_path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<RecordBatch> {
    // Create an object reader for the Apache Parquet file.
    let file_metadata = object_store
        .head(file_path)
        .await
        .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

    let reader = ParquetObjectReader::new(object_store, file_metadata.location);

    // Stream the data from the Apache Parquet file into a single record batch.
    let record_batches = read_batches_from_apache_parquet_file(reader).await?;

    let schema = record_batches[0].schema();
    compute::concat_batches(&schema, &record_batches).map_err(|error| error.into())
}

/// Read each batch of data from the Apache Parquet file given by `reader` and return them as a
/// [`Vec`] of [`RecordBatch`]. If the file could not be read successfully,
/// [`ModelarDbStorageError`](error::ModelarDbStorageError) is returned.
pub async fn read_batches_from_apache_parquet_file<R>(reader: R) -> Result<Vec<RecordBatch>>
where
    R: AsyncFileReader + Send + Unpin + 'static,
    ParquetRecordBatchStream<R>: StreamExt<Item = StdResult<RecordBatch, ParquetError>>,
{
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let mut stream = builder.build()?;

    let mut record_batches = Vec::new();
    while let Some(maybe_record_batch) = stream.next().await {
        let record_batch = maybe_record_batch?;
        record_batches.push(record_batch);
    }

    Ok(record_batches)
}

/// Write the rows in `record_batch` to an Apache Parquet file at the location given by `file_path`
/// in `object_store`. `file_path` must use the extension `.parquet`. `sorting_columns` can be set
/// to control the sorting order of the rows in the written file. Return [`Ok`] if the file was
/// written successfully, otherwise return [`ModelarDbStorageError`](error::ModelarDbStorageError).
pub async fn write_record_batch_to_apache_parquet_file(
    file_path: &Path,
    record_batch: &RecordBatch,
    sorting_columns: Option<Vec<SortingColumn>>,
    object_store: &dyn ObjectStore,
) -> Result<()> {
    // Check if the extension of the given path is correct.
    if file_path.extension() == Some("parquet") {
        let props = apache_parquet_writer_properties(sorting_columns);

        // Write the record batch to the object store.
        let mut buffer = Vec::new();
        let mut writer =
            AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), Some(props))?;
        writer.write(record_batch).await?;
        writer.close().await?;

        object_store
            .put(file_path, Bytes::from(buffer).into())
            .await
            .map_err(|error: object_store::Error| ParquetError::General(error.to_string()))?;

        Ok(())
    } else {
        Err(ParquetError::General(format!(
            "'{}' is not a valid file path for an Apache Parquet file.",
            file_path.as_ref()
        )))?
    }
}

/// Return [`WriterProperties`] optimized for compressed segments for Apache Parquet and Delta Lake.
fn apache_parquet_writer_properties(
    sorting_columns: Option<Vec<SortingColumn>>,
) -> WriterProperties {
    WriterProperties::builder()
        .set_data_page_size_limit(16384)
        .set_max_row_group_size(65536)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_sorting_columns(sorting_columns)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{Field, Schema};
    use modelardb_test::table;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    // Tests for read_record_batch_from_apache_parquet_file().
    #[tokio::test]
    async fn test_read_record_batch_from_apache_parquet_file() {
        let record_batch = table::compressed_segments_record_batch();
        let apache_parquet_path = Path::from("test.parquet");

        let (temp_dir, _result) =
            write_record_batch_to_temp_dir(&apache_parquet_path, &record_batch).await;

        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
        let result =
            read_record_batch_from_apache_parquet_file(&apache_parquet_path, object_store).await;

        assert!(result.is_ok());
        assert_eq!(record_batch, result.unwrap());
    }

    #[tokio::test]
    async fn test_read_record_batch_from_non_apache_parquet_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let path = Path::from("test.txt");
        object_store
            .put(&path, Bytes::from(Vec::new()).into())
            .await
            .unwrap();

        let result = read_record_batch_from_apache_parquet_file(&path, object_store);
        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn test_read_record_batch_from_non_existent_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

        let path = Path::from("test.parquet");

        let result = read_record_batch_from_apache_parquet_file(&path, object_store);
        assert!(result.await.is_err());
    }

    // Tests for write_record_batch_to_apache_parquet_file().
    #[tokio::test]
    async fn test_write_record_batch_to_apache_parquet_file() {
        let record_batch = table::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.parquet"), &record_batch).await;

        assert!(result.is_ok());
        assert!(temp_dir.path().join("test.parquet").exists());
    }

    #[tokio::test]
    async fn test_write_empty_record_batch_to_apache_parquet_file() {
        let fields: Vec<Field> = vec![];
        let schema = Schema::new(fields);
        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.parquet"), &record_batch).await;

        assert!(result.is_ok());
        assert!(temp_dir.path().join("test.parquet").exists());
    }

    #[tokio::test]
    async fn test_write_record_batch_to_file_path_with_invalid_extension() {
        let record_batch = table::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test.txt"), &record_batch).await;

        assert!(result.is_err());
        assert!(!temp_dir.path().join("test.txt").exists());
    }

    #[tokio::test]
    async fn test_write_record_batch_to_file_path_without_extension() {
        let record_batch = table::compressed_segments_record_batch();
        let (temp_dir, result) =
            write_record_batch_to_temp_dir(&Path::from("test"), &record_batch).await;

        assert!(result.is_err());
        assert!(!temp_dir.path().join("test").exists());
    }

    async fn write_record_batch_to_temp_dir(
        file_path: &Path,
        record_batch: &RecordBatch,
    ) -> (TempDir, Result<()>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

        let result =
            write_record_batch_to_apache_parquet_file(file_path, record_batch, None, &object_store)
                .await;

        (temp_dir, result)
    }
}
