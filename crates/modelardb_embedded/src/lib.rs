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

//! Read from and write to ModelarDB instances and data folders.

mod capi;
pub mod error; // Public because public functions return ModelarDbEmbeddedError.
pub mod operations;

use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute;
use arrow::datatypes::Schema;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::common;
use modelardb_types::types::ErrorBound;

use crate::error::Result;

/// Types of tables supported by [`create()`](operations::Operations::create).
#[derive(Clone)]
pub enum TableType {
    NormalTable(Schema),
    TimeSeriesTable(Schema, HashMap<String, ErrorBound>, HashMap<String, String>),
}

/// Aggregate operations supported by [`read_time_series_table()`](operations::Operations::read_time_series_table).
#[derive(Debug)]
pub enum Aggregate {
    None,
    Count,
    Min,
    Max,
    Sum,
    Avg,
}

/// Collects a [`RecordBatchStream`] into a [`RecordBatch`]`and returns it. If the
/// [`RecordBatchStream`] cannot be collected or the [`RecordBatches`](RecordBatch) cannot be
/// concatenated, a [`ModelarDbEmbeddedError`] is returned.
async fn record_batch_stream_to_record_batch(
    record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<RecordBatch> {
    let record_batches = common::collect(record_batch_stream).await?;
    if record_batches.is_empty() {
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    } else {
        compute::concat_batches(&record_batches[0].schema(), &record_batches)
            .map_err(|error| error.into())
    }
}
