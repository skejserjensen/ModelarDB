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

//! Implementation of [`DataSinks`](`DataSink`) that writes [`RecordBatches`](RecordBatch) to
//! [`StorageEngine`].

use std::any::Any;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::arrow::datatypes::Schema;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use modelardb_types::types::TimeSeriesTableMetadata;
use tokio::sync::RwLock;

use crate::storage::StorageEngine;

/// [`DataSink`] that writes [`RecordBatches`](RecordBatch) to [`StorageEngine`]. Use
/// [`TimeSeriesTableDataSink`] for writing multivariate time series to [`StorageEngine`].
pub struct NormalTableDataSink {
    /// The name of the normal table inserted data will be written to.
    table_name: String,
    /// The schema of the normal table inserted data will be written to.
    schema: Arc<Schema>,
    /// Manages all uncompressed and compressed data in the system.
    storage_engine: Arc<RwLock<StorageEngine>>,
}

impl NormalTableDataSink {
    pub fn new(
        table_name: String,
        schema: Arc<Schema>,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Self {
        Self {
            table_name,
            schema,
            storage_engine,
        }
    }
}

#[async_trait]
impl DataSink for NormalTableDataSink {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the [`DataSink's`](DataSink) schema.
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Return a snapshot of the set of metrics being collected by the [`DataSink`].
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Write all rows in `data` to [`StorageEngine`]. Returns the number of rows that have been
    /// written or a [`DataFusionError`] if the rows could not be inserted.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut rows_inserted: u64 = 0;

        while let Some(record_batch) = data.next().await {
            let record_batch = record_batch?;
            rows_inserted += record_batch.num_rows() as u64;

            let storage_engine = self.storage_engine.read().await;
            storage_engine
                .insert_record_batch(&self.table_name, record_batch)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }

        Ok(rows_inserted)
    }
}

impl Debug for NormalTableDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let name = &self.table_name;
        write!(f, "NormalTableDataSink for {name}")
    }
}

impl DisplayAs for NormalTableDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        let name = &self.table_name;
        write!(f, "NormalTableDataSink for {name}")
    }
}

/// [`DataSink`] that writes [`RecordBatches`](RecordBatch) containing multivariate time series to
/// [`StorageEngine`]. Assumes the generated columns are included, thus they are dropped without
/// checking the schema.
pub struct TimeSeriesTableDataSink {
    /// Metadata for the time series table inserted data will be written to.
    time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
    /// Manages all uncompressed and compressed data in the system.
    storage_engine: Arc<RwLock<StorageEngine>>,
}

impl TimeSeriesTableDataSink {
    pub fn new(
        time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
        storage_engine: Arc<RwLock<StorageEngine>>,
    ) -> Self {
        Self {
            time_series_table_metadata,
            storage_engine,
        }
    }
}

#[async_trait]
impl DataSink for TimeSeriesTableDataSink {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the [`DataSink's`](DataSink) schema.
    fn schema(&self) -> &Arc<Schema> {
        &self.time_series_table_metadata.schema
    }

    /// Return a snapshot of the set of metrics being collected by the [`DataSink`].
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Write all data points in `data` to [`StorageEngine`]. Returns the number of data points that
    /// has been written or a [`DataFusionError`] if the data points could not be inserted.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut data_points_inserted: u64 = 0;

        while let Some(record_batch) = data.next().await {
            // Remove the generated columns from the record batch. The generated columns must be
            // part of the inserted data since Apache DataFusion checks it before passing it to
            // write_all().
            let record_batch =
                record_batch?.project(&self.time_series_table_metadata.query_schema_to_schema)?;

            // Create a new record batch with the schema of the time series table to fix the problem
            // where the schema of the inserted data has nullable fields.
            let record_batch = RecordBatch::try_new(
                self.time_series_table_metadata.schema.clone(),
                record_batch.columns().to_vec(),
            )?;

            data_points_inserted += record_batch.num_rows() as u64;

            let mut storage_engine = self.storage_engine.write().await;
            storage_engine
                .insert_data_points(self.time_series_table_metadata.clone(), record_batch)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }

        Ok(data_points_inserted)
    }
}

impl Debug for TimeSeriesTableDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let name = &self.time_series_table_metadata.name;
        write!(f, "TimeSeriesTableDataSink for {name}")
    }
}

impl DisplayAs for TimeSeriesTableDataSink {
    /// Write a string-based representation of the [`DataSink`] to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        let name = &self.time_series_table_metadata.name;
        write!(f, "TimeSeriesTableDataSink for {name}")
    }
}
