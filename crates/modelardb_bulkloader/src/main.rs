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

//! Bulkloader for reading and writing ModelarDB data folders.

use std::io::Write;
use std::path::Path as StdPath;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::{env, io, process};

use arrow::array::RecordBatch;
use arrow::compute;
use arrow::compute::kernels;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::RecordBatchStream;
use datafusion::prelude::SessionContext;
use deltalake::kernel::StructField;
use deltalake::logstore::object_store::local::LocalFileSystem;
use deltalake::operations::create::CreateBuilder;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::{ObjectStore, Path};
use futures::stream::StreamExt;
use modelardb_embedded::error::{ModelarDbEmbeddedError, Result};
use modelardb_embedded::operations::Operations;
use modelardb_embedded::operations::data_folder::DataFolder;
use modelardb_storage::delta_lake::DeltaTableWriter;
use modelardb_types::types::TimeSeriesTableMetadata;
use sysinfo::System;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse the command line arguments.
    let mut args = env::args();
    if args.len() < 5 {
        print_usage_and_exit_with_error();
    }

    // unwrap() is safe as the path of the binary is dropped.
    args.next().unwrap();

    // unwrap() on args.next() is safe as args contains at least five elements.
    let operation = args.next().unwrap();
    let input_path = args.next().unwrap();
    let output_path = args.next().unwrap();
    let table_name = args.next().unwrap();

    // Collect arguments for flags.
    let mut pre_sql: Vec<String> = vec![];
    let mut partition_by: Vec<String> = vec![];
    let mut post_sql: Vec<String> = vec![];
    let mut cast_double_to_float = false;

    let mut maybe_arg_values: Option<&mut Vec<String>> = None;
    for arg in args {
        match arg.as_str() {
            "--pre-sql" => maybe_arg_values = Some(&mut pre_sql),
            "--partition-by" => maybe_arg_values = Some(&mut partition_by),
            "--post-sql" => maybe_arg_values = Some(&mut post_sql),
            "--cast-double-to-float" => cast_double_to_float = true,
            _ => {
                if let Some(ref mut arg_values) = maybe_arg_values {
                    arg_values.push(arg);
                } else {
                    print_usage_and_exit_with_error();
                }
            }
        }
    }

    // Either import or export data to or from table_name depending on operation.
    match operation.as_str() {
        "import" => {
            import(
                &input_path,
                &output_path,
                &table_name,
                &pre_sql,
                &post_sql,
                cast_double_to_float,
            )
            .await
        }
        "export" => {
            export(
                &input_path,
                &output_path,
                &table_name,
                &pre_sql,
                &post_sql,
                partition_by,
            )
            .await
        }
        _ => Err(ModelarDbEmbeddedError::InvalidArgument(
            "Operation must be import or export.".to_owned(),
        )),
    }
}

/// Print a usage message to stderr and exit with an error status code.
fn print_usage_and_exit_with_error() -> ! {
    // The errors are consciously ignored as the client is terminating.
    let binary_path = env::current_exe().unwrap();
    let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
    eprintln!(
        "Usage: {binary_name} operation [flags]\n\n\
            operations\n \
            import parquet_folder data_folder table_name   imports data from parquet_folder to the table with table_name in data_folder\n \
            export data_folder parquet_folder table_name   exports data from the table with table_name in data_folder to parquet_folder\n\n\
            flags\n \
            --pre-sql statements                           one or more quoted SQL statements run before any operation, e.g, CREATE TIME SERIES TABLE\n \
            --partition-by columns                         columns to partition the output data by when exporting to Apache Parquet files\n \
            --post-sql statements                          one or more quoted SQL statements run after a successful operation, e.g, DROP TABLE\n \
            --cast-double-to-float                         cast double to float, which may be a lossy cast, to simplify loading time series tables"
    );
    process::exit(1);
}

/// Import data from `input_path` to the table with `table_name` in `output_path`. `pre_sql` is
/// executed in the [`DataFolder`] at `output_path` before the importing starts, and `post_sql` is
/// executed after it has finished successfully. `cast_double_to_float` specifies if
/// [`DataType::Float64`] should be cast to [`DataType::Float32`] for time series tables to simplify
/// importing data into time series tables. If data cannot be read, the table already exists with a
/// different schema, or the data cannot be written, a [`ModelarDbEmbeddedError`] is returned.
async fn import(
    input_path: &str,
    output_path: &str,
    table_name: &str,
    pre_sql: &[String],
    post_sql: &[String],
    cast_double_to_float: bool,
) -> Result<()> {
    // Create an ExecutionPlan that reads all Apache Parquet files in the input path.
    let input_stream = input_stream(input_path).await?;

    // Open DataFolder to write each of the Apache Parquet files to table_name to.
    let mut data_folder = create_data_folder(output_path).await?;

    // Ensure the operations that will be performed is as the user expects.
    println!(
        "Import\n- Pre-SQL: {}\n- Read From: {input_path}\n- Write To: {table_name} In {output_path}\n- With Cast Double To Float: {cast_double_to_float}\n- Post-SQL: {}",
        pre_sql.join("; "),
        post_sql.join("; ")
    );

    if !ask_user_for_confirmation_to_start()? {
        return Ok(());
    }

    for sql in pre_sql {
        data_folder.read(sql).await?;
    }

    if let Some(time_series_table_metadata) =
        data_folder.time_series_table_metadata(table_name).await
    {
        import_time_series_table(
            input_stream,
            &time_series_table_metadata,
            &mut data_folder,
            cast_double_to_float,
        )
        .await?;
    } else {
        if cast_double_to_float {
            return Err(ModelarDbEmbeddedError::InvalidArgument(
                "Float can only be cast to doubles for time series tables.".to_owned(),
            ));
        }
        import_normal_table(input_stream, table_name, &mut data_folder).await?;
    }

    for sql in post_sql {
        data_folder.read(sql).await?;
    }

    Ok(())
}

/// Import data from `input_stream` to the time series table with `time_series_table_metadata` in
/// `data_folder`. If `cast_double_to_float` is [`true`], [`DataType::Float64`] is cast to
/// [`DataType::Float32`] to simplify importing time series into time series tables. If data cannot
/// be read or written, a [`ModelarDbEmbeddedError`] is returned.
async fn import_time_series_table(
    mut input_stream: Pin<Box<dyn RecordBatchStream>>,
    time_series_table_metadata: &TimeSeriesTableMetadata,
    data_folder: &mut DataFolder,
    cast_double_to_float: bool,
) -> Result<()> {
    let table_name = &time_series_table_metadata.name;
    let mut delta_table_writer = data_folder.writer(table_name).await?;

    let mut system = System::new();
    let mut current_batch = vec![];
    let mut current_batch_size = 0;
    while let Some(record_batch) = input_stream.next().await {
        let uncompressed_data = cast_record_batch(record_batch?, cast_double_to_float)?;
        current_batch_size += uncompressed_data.get_array_memory_size();
        current_batch.push(uncompressed_data);

        // Write the current batch if it uses more than half the memory so concatenation is
        // possible. The amount of available memory is reduced by 20% for other variables.
        system.refresh_memory();
        if current_batch_size > (system.available_memory() as usize / 10 * 8)
            && let Err(write_error) = import_and_clear_time_series_table_batch(
                data_folder,
                &mut delta_table_writer,
                time_series_table_metadata,
                &mut current_batch,
                &mut current_batch_size,
            )
            .await
        {
            delta_table_writer.rollback().await?;
            return Err(write_error);
        }
    }

    if let Err(write_error) = import_and_clear_time_series_table_batch(
        data_folder,
        &mut delta_table_writer,
        time_series_table_metadata,
        &mut current_batch,
        &mut current_batch_size,
    )
    .await
    {
        delta_table_writer.rollback().await?;
        return Err(write_error);
    }

    delta_table_writer.commit().await?;

    Ok(())
}

/// Import data from `input_stream` to the normal table with `table_name` in `data_folder`. If data
/// cannot be read or written, a [`ModelarDbEmbeddedError`] is returned.
async fn import_normal_table(
    mut input_stream: Pin<Box<dyn RecordBatchStream>>,
    table_name: &str,
    data_folder: &mut DataFolder,
) -> Result<()> {
    let mut delta_table_writer = data_folder.writer(table_name).await?;

    while let Some(record_batch) = input_stream.next().await {
        let record_batch = record_batch?;
        if let Err(write_error) = delta_table_writer.write(&record_batch).await {
            delta_table_writer.rollback().await?;
            return Err(ModelarDbEmbeddedError::ModelarDbStorage(write_error));
        }
    }
    delta_table_writer.commit().await?;

    Ok(())
}

/// Returns a [`RecordBatchStream`] that produces the data in `input_path` as
/// [`RecordBatches`](RecordBatch). If [`ObjectStore`] cannot be created, the tables partitioning
/// columns cannot be read, or the [`RecordBatchStream`] cannot be created, a
/// [`ModelarDbEmbeddedError`] is returned.
async fn input_stream(input_path: &str) -> Result<Pin<Box<dyn RecordBatchStream>>> {
    let session_context = SessionContext::new();

    let input_object_store = LocalFileSystem::new_with_prefix(input_path)?;
    let table_partitioning_cols = table_partitioning_cols(&input_object_store).await?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_table_partition_cols(table_partitioning_cols);

    session_context
        .register_listing_table("table_name", input_path, listing_options, None, None)
        .await
        .unwrap();

    let input_data_frame = session_context.sql("SELECT * FROM table_name").await?;
    let input_stream = input_data_frame.execute_stream().await?;

    Ok(input_stream)
}

/// Returns the columns the Apache Parquet files in `object_store` are partitioned by from the file
/// paths. If the files in the folder cannot be listed or the file signature cannot be read from a
/// potential Apache Parquet, a [`ModelarDbEmbeddedError`] is returned.
async fn table_partitioning_cols(
    object_store: &dyn ObjectStore,
) -> Result<Vec<(String, DataType)>> {
    let mut file_stream = object_store.list(None);

    while let Some(maybe_object_meta) = file_stream.next().await {
        let object_meta = maybe_object_meta?;

        if has_apache_parquet_signature(object_store, &object_meta.location).await? {
            let mut table_partition_cols = vec![];
            for part in object_meta.location.parts() {
                let part_str = part.as_ref();
                if part_str.contains("=") && !part_str.ends_with(".parquet") {
                    // unwrap() is safe as the string contains an equal sign.
                    let (column_name, _column_value) = part_str.split_once("=").unwrap();
                    table_partition_cols.push((column_name.to_owned(), DataType::Utf8));
                }
            }
            return Ok(table_partition_cols);
        }
    }

    Ok(vec![])
}

/// Read the first four bytes of the file at `path` in `object_store` and returns:
/// * [`true`] if the file can be read and has the signature of an Apache Parquet file.
/// * [`false`] if the file can be read and does not have the signature of an Apache Parquet file.
/// * [`ModelarDbEmbeddedError`] if an error occurs.
async fn has_apache_parquet_signature(object_store: &dyn ObjectStore, path: &Path) -> Result<bool> {
    object_store
        .get_range(path, 0..4)
        .await
        .map(|magic_bytes| magic_bytes == "PAR1")
        .map_err(ModelarDbEmbeddedError::ObjectStore)
}

/// Cast the schema of `record_batch` to be compatible with [`deltalake`] if it can be done
/// losslessly and `cast_double_to_float` is [`false`], otherwise [`ModelarDbEmbeddedError`] is
/// returned. Although if `cast_double_to_float` is [`true`], all arrays of type [`DataType::Float64`]
/// are also cast to [`DataType::Float32`] to simplify loading time series tables.
fn cast_record_batch(record_batch: RecordBatch, cast_double_to_float: bool) -> Result<RecordBatch> {
    let schema = record_batch.schema();
    let columns = record_batch.columns();

    let mut cast_fields = Vec::with_capacity(schema.fields.len());
    let mut cast_columns = Vec::with_capacity(columns.len());

    // Clippy wants to use for_each() but that does not allow use of ? for returning errors.
    #[allow(clippy::needless_range_loop)]
    for index in 0..columns.len() {
        let field = &schema.fields[index];
        let column = &columns[index];

        let (field, column) = match field.data_type() {
            // No cast is required for Microsecond and casting Nanosecond would be lossy.
            DataType::Timestamp(TimeUnit::Second, time_zone)
            | DataType::Timestamp(TimeUnit::Millisecond, time_zone) => {
                let data_type = DataType::Timestamp(TimeUnit::Microsecond, time_zone.clone());
                let column = kernels::cast(column, &data_type)?;
                let field = Field::new(field.name(), data_type, false);
                (Arc::new(field), column)
            }
            DataType::Float16 => {
                let data_type = DataType::Float32;
                let column = kernels::cast(column, &data_type)?;
                let field = Field::new(field.name(), data_type, false);
                (Arc::new(field), column)
            }
            DataType::Float64 if cast_double_to_float => {
                let data_type = DataType::Float32;
                let column = kernels::cast(column, &data_type)?;
                let field = Field::new(field.name(), data_type, false);
                (Arc::new(field), column)
            }
            _ => (field.clone(), column.clone()),
        };

        cast_fields.push(field);
        cast_columns.push(column);
    }

    let cast_schema = Arc::new(Schema::new(cast_fields));
    RecordBatch::try_new(cast_schema, cast_columns).map_err(|error| error.into())
}

/// Import the `current_batch` into the time series table with `time_series_table_metadata` in
/// `data_folder` using `delta_table_writer`. Then clear `current_batch` and zero
/// `current_batch_size`. If a [`RecordBatch`] in `current_batch` has a different schema, the
/// compression fails, or the write fails, a [`ModelarDbEmbeddedError`] is returned.
async fn import_and_clear_time_series_table_batch(
    data_folder: &DataFolder,
    delta_table_writer: &mut DeltaTableWriter,
    time_series_table_metadata: &TimeSeriesTableMetadata,
    current_batch: &mut Vec<RecordBatch>,
    current_batch_size: &mut usize,
) -> Result<()> {
    if *current_batch_size != 0 {
        let schema = current_batch[0].schema();
        let uncompressed_data = compute::concat_batches(&schema, &*current_batch)?;
        let compressed_data = data_folder
            .compress_all(time_series_table_metadata, &uncompressed_data)
            .await?;
        delta_table_writer.write_all(&compressed_data).await?;
        current_batch.clear();
        *current_batch_size = 0;
    }

    Ok(())
}

/// Export data from the table with `table_name` in `input_path` to `output_path` with the data
/// partitioned by columns in `partition_by`. `pre_sql` is executed in the [`DataFolder`] at
/// `input_path` before the exporting starts, and `post_sql` is executed after it has finished
/// successfully. If data cannot be read from the table, or the data cannot be written, a
/// [`ModelarDbEmbeddedError`] is returned.
async fn export(
    input_path: &str,
    output_path: &str,
    table_name: &str,
    pre_sql: &[String],
    post_sql: &[String],
    partition_by: Vec<String>,
) -> Result<()> {
    // Open DataFolder to read from and ensure the table exists.
    let mut data_folder = create_data_folder(input_path).await?;

    // Ensure the operations that will be performed is as the user expects.
    println!(
        "Export\n- Pre-SQL: {}\n- Read From: {table_name} In {input_path}\n- Write To: {output_path}\n- With Partition By: {}\n- Post-SQL: {}",
        pre_sql.join("; "),
        partition_by.join(", "),
        post_sql.join("; ")
    );

    if !ask_user_for_confirmation_to_start()? {
        return Ok(());
    }

    for sql in pre_sql {
        data_folder.read(sql).await?;
    }

    // Create Delta Lake table at output_path.
    let schema = data_folder.schema(table_name).await?;
    let columns: StdResult<Vec<StructField>, ArrowError> = schema
        .fields()
        .iter()
        .map(|field| {
            let field: &Field = field;
            let struct_field: StdResult<StructField, ArrowError> = field.try_into();
            struct_field
        })
        .collect();

    let delta_table = CreateBuilder::new()
        .with_table_name(table_name)
        .with_location(output_path)
        .with_columns(columns?)
        .with_partition_columns(&partition_by)
        .await?;

    // Export data to the table in batches.
    let sql = format!("SELECT * FROM {table_name}");
    let mut record_batch_stream = data_folder.read(&sql).await?;

    let mut delta_table_writer =
        DeltaTableWriter::try_new(delta_table, partition_by, WriterProperties::default())?;

    while let Some(record_batch) = record_batch_stream.next().await {
        let record_batch = record_batch?;
        if let Err(write_error) = delta_table_writer.write(&record_batch).await {
            delta_table_writer.rollback().await?;
            return Err(ModelarDbEmbeddedError::ModelarDbStorage(write_error));
        }
    }
    delta_table_writer.commit().await?;

    for sql in post_sql {
        data_folder.read(sql).await?;
    }

    Ok(())
}

/// Returns a [`DataFolder`] for `data_folder_path`. If the necessary environment variables are not
/// set for S3 and Azure or the [`DataFolder`] cannot access `data_folder_path`, a
/// [`ModelarDbEmbeddedError`] is returned.
async fn create_data_folder(data_folder_path: &str) -> Result<DataFolder> {
    match data_folder_path.split_once("://") {
        Some(("s3", bucket_name)) => {
            let endpoint = env::var("AWS_ENDPOINT")?;
            let access_key_id = env::var("AWS_ACCESS_KEY_ID")?;
            let secret_access_key = env::var("AWS_SECRET_ACCESS_KEY")?;

            DataFolder::open_s3(
                endpoint,
                bucket_name.to_owned(),
                access_key_id,
                secret_access_key,
            )
            .await
        }
        Some(("az", container_name)) => {
            let account_name = env::var("AZURE_STORAGE_ACCOUNT_NAME")?;
            let access_key = env::var("AZURE_STORAGE_ACCESS_KEY")?;

            DataFolder::open_azure(account_name, access_key, container_name.to_owned()).await
        }
        _ => {
            let data_folder_path = StdPath::new(data_folder_path);
            DataFolder::open_local(data_folder_path).await
        }
    }
}

/// Asks the user for permission to start and return [`true`] if the user agrees and [`false`] if
/// they do not. If the users permission cannot be read, return [`ModelarDbEmbeddedError`].
fn ask_user_for_confirmation_to_start() -> Result<bool> {
    let mut user_input = String::new();
    loop {
        user_input.clear();
        print!("Press y/Y+Enter to start or n/N+Enter to quit> ");
        io::stdout()
            .flush()
            .map_err(|error| ModelarDbEmbeddedError::InvalidArgument(error.to_string()))?;
        io::stdin()
            .read_line(&mut user_input)
            .map_err(|error| ModelarDbEmbeddedError::InvalidArgument(error.to_string()))?;

        match user_input.as_str() {
            "y\n" => return Ok(true),
            "Y\n" => return Ok(true),
            "n\n" => return Ok(false),
            "N\n" => return Ok(false),
            _ => (),
        }
    }
}
