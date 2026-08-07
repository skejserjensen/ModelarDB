/* Copyright 2023 The ModelarDB Contributors
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

//! Implementation of table related functions and constants used throughout ModelarDB for testing purposes.

use std::sync::Arc;

use arrow::array::{BinaryArray, Float32Array, Int8Array, Int16Array, RecordBatch, StringArray};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};
use modelardb_types::types::{
    ArrowTimestamp, ArrowValue, ErrorBound, TimeSeriesTableMetadata, Timestamp, TimestampArray,
    Value, ValueArray,
};

use crate::{ERROR_BOUND_FIVE, ERROR_BOUND_ONE, ERROR_BOUND_ZERO};

/// SQL to create a normal table with a timestamp column and two floating point columns.
pub const NORMAL_TABLE_SQL: &str =
    "CREATE TABLE normal_table(timestamp TIMESTAMP, values REAL, metadata REAL)";

/// Name of the normal table used in tests.
pub const NORMAL_TABLE_NAME: &str = "normal_table";

/// SQL to create a time series table with a timestamp column, two field columns, and a tag column.
pub const TIME_SERIES_TABLE_SQL: &str = "CREATE TIME SERIES TABLE time_series_table(timestamp TIMESTAMP, field_1 FIELD, field_2 FIELD, tag TAG)";

/// Name of the time series table used in tests.
pub const TIME_SERIES_TABLE_NAME: &str = "time_series_table";

/// Return protobuf message bytes containing metadata for a normal table and a time series table.
pub fn table_metadata_protobuf_bytes() -> Vec<u8> {
    let encoded_normal_table = modelardb_types::flight::encode_normal_table_metadata(
        NORMAL_TABLE_NAME,
        &normal_table_schema(),
    )
    .unwrap();

    let encoded_time_series_table =
        modelardb_types::flight::encode_time_series_table_metadata(&time_series_table_metadata())
            .unwrap();

    modelardb_types::flight::serialize_table_metadata(
        vec![encoded_normal_table],
        vec![encoded_time_series_table],
    )
}

/// Return a [`Schema`] for a normal table with a timestamp column and two floating point columns.
pub fn normal_table_schema() -> Schema {
    Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("values", ArrowValue::DATA_TYPE, false),
        Field::new("metadata", ArrowValue::DATA_TYPE, false),
    ])
}

/// Return a [`RecordBatch`] containing five rows for a normal table with a timestamp column and
/// two floating point columns.
pub fn normal_table_record_batch() -> RecordBatch {
    let timestamps = TimestampArray::from(vec![0, 1, 2, 3, 4]);
    let values = ValueArray::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    let metadata = ValueArray::from(vec![6.0, 7.0, 8.0, 9.0, 10.0]);

    RecordBatch::try_new(
        Arc::new(normal_table_schema()),
        vec![Arc::new(timestamps), Arc::new(values), Arc::new(metadata)],
    )
    .unwrap()
}

/// Return [`TimeSeriesTableMetadata`] for a time series table with a schema containing a tag column,
/// a timestamp column, and two field columns.
pub fn time_series_table_metadata() -> TimeSeriesTableMetadata {
    let query_schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("field_1", ArrowValue::DATA_TYPE, false),
        Field::new("field_2", ArrowValue::DATA_TYPE, false),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let error_bounds = vec![
        ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
        ErrorBound::try_new_absolute(ERROR_BOUND_ONE).unwrap(),
        ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap(),
        ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
    ];

    let generated_columns = vec![None, None, None, None];

    TimeSeriesTableMetadata::try_new(
        TIME_SERIES_TABLE_NAME.to_owned(),
        query_schema,
        error_bounds,
        generated_columns,
    )
    .unwrap()
}

/// Return [`TimeSeriesTableMetadata`] in an [`Arc`] for a time series table with a schema containing
/// a tag column, a timestamp column, and two field columns.
pub fn time_series_table_metadata_arc() -> Arc<TimeSeriesTableMetadata> {
    Arc::new(time_series_table_metadata())
}

/// Create a [`RecordBatch`] with data that resembles uncompressed data with a single tag and two
/// field columns. The returned data has `row_count` rows, with a different tag for each row.
pub fn uncompressed_time_series_table_record_batch(row_count: usize) -> RecordBatch {
    let tags: Vec<String> = (0..row_count).map(|tag| tag.to_string()).collect();
    let timestamps: Vec<Timestamp> = (0..row_count).map(|ts| ts as Timestamp).collect();
    let values: Vec<Value> = (0..row_count).map(|value| value as Value).collect();

    RecordBatch::try_new(
        time_series_table_metadata().schema.clone(),
        vec![
            Arc::new(TimestampArray::from(timestamps)),
            Arc::new(ValueArray::from(values.clone())),
            Arc::new(ValueArray::from(values)),
            Arc::new(StringArray::from(tags)),
        ],
    )
    .unwrap()
}

/// Return a [`RecordBatch`] containing three compressed segments.
pub fn compressed_segments_record_batch() -> RecordBatch {
    compressed_segments_record_batch_with_time(0, 0, 0.0)
}

/// Return a [`RecordBatch`] containing three compressed segments. The compressed segments time
/// range is from `time_ms` to `time_ms` + 3, while the value range is from`offset` + 5.2 to
/// `offset` + 34.2.
pub fn compressed_segments_record_batch_with_time(
    field_column: i16,
    time_ms: i64,
    offset: f32,
) -> RecordBatch {
    let start_times = vec![time_ms, time_ms + 2, time_ms + 4];
    let end_times = vec![time_ms + 1, time_ms + 3, time_ms + 5];
    let min_values = vec![offset + 5.2, offset + 10.3, offset + 30.2];
    let max_values = vec![offset + 20.2, offset + 12.2, offset + 34.2];

    let model_type_id = Int8Array::from(vec![1, 1, 2]);
    let start_time = TimestampArray::from(start_times);
    let end_time = TimestampArray::from(end_times);
    let timestamps = BinaryArray::from_vec(vec![b"", b"", b""]);
    let min_value = ValueArray::from(min_values);
    let max_value = ValueArray::from(max_values);
    let values = BinaryArray::from_vec(vec![b"", b"", b""]);
    let residuals = BinaryArray::from_vec(vec![b"", b"", b""]);
    let error = Float32Array::from(vec![0.2, 0.5, 0.1]);
    let field_column = Int16Array::from(vec![field_column, field_column, field_column]);
    let tag_column = StringArray::from(vec!["tag", "tag", "tag"]);

    RecordBatch::try_new(
        time_series_table_metadata().compressed_schema,
        vec![
            Arc::new(model_type_id),
            Arc::new(start_time),
            Arc::new(end_time),
            Arc::new(timestamps),
            Arc::new(min_value),
            Arc::new(max_value),
            Arc::new(values),
            Arc::new(residuals),
            Arc::new(error),
            Arc::new(field_column),
            Arc::new(tag_column),
        ],
    )
    .unwrap()
}
