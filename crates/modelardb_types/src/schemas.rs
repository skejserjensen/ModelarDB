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

//! The schemas used throughout the system.

use std::sync::Arc;
use std::sync::LazyLock;

use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema};

use crate::types::{
    ArrowTimestamp, ArrowValue, CompressedSchema, GridSchema, QueryCompressedSchema,
};

/// Name of the column used to partition the compressed segments.
pub const FIELD_COLUMN: &str = "field_column";

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for compressed segments.
pub static COMPRESSED_SCHEMA: LazyLock<CompressedSchema> = LazyLock::new(|| {
    let mut query_compressed_schema_fields = QUERY_COMPRESSED_SCHEMA.0.fields().to_vec();
    let field_column = Arc::new(Field::new(FIELD_COLUMN, DataType::Int16, false));
    query_compressed_schema_fields.push(field_column);
    CompressedSchema(Arc::new(Schema::new(query_compressed_schema_fields)))
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used for compressed segments when
/// executing queries as [`FIELD_COLUMN`] is not stored in the Apache Parquet files.
pub static QUERY_COMPRESSED_SCHEMA: LazyLock<QueryCompressedSchema> = LazyLock::new(|| {
    QueryCompressedSchema(Arc::new(Schema::new(vec![
        Field::new("model_type_id", DataType::Int8, false),
        Field::new("start_time", ArrowTimestamp::DATA_TYPE, false),
        Field::new("end_time", ArrowTimestamp::DATA_TYPE, false),
        Field::new("timestamps", DataType::Binary, false),
        Field::new("min_value", ArrowValue::DATA_TYPE, false),
        Field::new("max_value", ArrowValue::DATA_TYPE, false),
        Field::new("values", DataType::Binary, false),
        Field::new("residuals", DataType::Binary, false),
        Field::new("error", DataType::Float32, false),
    ])))
});

/// Minimum size of the metadata required for a compressed segment. Meaning that the sizes of
/// `timestamps` and `values` are not included as they are [`DataType::Binary`] and thus their size
/// depend on which model is selected to represent the values for that compressed segment.
pub static COMPRESSED_METADATA_SIZE_IN_BYTES: LazyLock<usize> = LazyLock::new(|| {
    QUERY_COMPRESSED_SCHEMA
        .0
        .fields()
        .iter()
        .map(|field| field.data_type().primitive_width().unwrap_or(0))
        .sum()
});

/// [`RecordBatch`](arrow::record_batch::RecordBatch) [`Schema`] used internally during query processing.
pub static GRID_SCHEMA: LazyLock<GridSchema> = LazyLock::new(|| {
    GridSchema(Arc::new(Schema::new(vec![
        Field::new("timestamp", ArrowTimestamp::DATA_TYPE, false),
        Field::new("value", ArrowValue::DATA_TYPE, false),
    ])))
});
