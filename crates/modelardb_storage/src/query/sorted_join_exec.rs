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

//! Implementation of the Apache DataFusion execution plan [`SortedJoinExec`] and its corresponding
//! stream [`SortedJoinStream`] which joins multiple sorted array produced by
//! [`GridExecs`](crate::query::grid_exec::GridExec) streams and combines them with the time series
//! tags retrieved from the [`TableMetadataManager`](metadata::table_metadata_manager::TableMetadataManager)
//! to create the complete results containing a timestamp column, one or more field columns, and zero
//! or more tag columns.

use std::any::Any;
use std::fmt::{Formatter, Result as FmtResult};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use arrow::datatypes::Schema;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, LexRequirement};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::stream::{Stream, StreamExt};

/// The different types of columns supported by [`SortedJoinExec`], used for specifying the order in
/// which the timestamp, field, and tag columns should be returned by [`SortedJoinStream`].
#[derive(Debug, Clone)]
pub(crate) enum SortedJoinColumnType {
    Timestamp,
    Field,
    Tag(String),
}

/// An execution plan that joins arrays of data points sorted by tag columns and `timestamp` from
/// multiple execution plans. It is `pub(crate)` so the additional rules added to Apache
/// DataFusion's physical optimizer can pattern match on it.
#[derive(Debug)]
pub(crate) struct SortedJoinExec {
    /// Schema of the execution plan.
    schema: Arc<Schema>,
    /// Order of columns to return.
    return_order: Vec<SortedJoinColumnType>,
    /// Execution plans to read batches of data points from.
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Properties about the plan used in query optimization.
    plan_properties: PlanProperties,
    /// The sort order that [`SortedJoinExec`] requires for the data points it receives as its input.
    query_requirement_data_point: LexRequirement,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    metrics: ExecutionPlanMetricsSet,
}

impl SortedJoinExec {
    pub(crate) fn new(
        schema: Arc<Schema>,
        return_order: Vec<SortedJoinColumnType>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        query_requirement_data_point: LexRequirement,
    ) -> Arc<Self> {
        // Specify that the record batches produced by the execution plan will have an unknown order.
        let equivalence_properties = EquivalenceProperties::new(schema.clone());

        let plan_properties = PlanProperties::new(
            equivalence_properties,
            inputs[0].output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Arc::new(SortedJoinExec {
            schema,
            return_order,
            inputs,
            plan_properties,
            query_requirement_data_point,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for SortedJoinExec {
    /// Return the name of the [`ExecutionPlan`].
    fn name(&self) -> &str {
        Self::static_name()
    }

    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the schema of the plan.
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Return properties of the output of the plan.
    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    /// Return the single execution plan batches of rows are read from.
    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan)>> {
        // iter() returns an iterator that produces elements of type &T.
        self.inputs.iter().collect()
    }

    /// Return a new [`SortedJoinExec`] with the execution plan to read batches of reconstructed
    /// data points from replaced. [`DataFusionError::Plan`] is returned if `children` does not
    /// contain at least one element.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<(dyn ExecutionPlan)>>,
    ) -> DataFusionResult<Arc<(dyn ExecutionPlan)>> {
        if !children.is_empty() {
            Ok(SortedJoinExec::new(
                self.schema.clone(),
                self.return_order.clone(),
                children,
                self.query_requirement_data_point.clone(),
            ))
        } else {
            Err(DataFusionError::Plan(format!(
                "At least one child must be provided {self:?}.",
            )))
        }
    }

    /// Create a stream that reads batches of reconstructed data points from the child streams,
    /// joins the arrays of data points with the timestamps and tags, and returns the result.
    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let streams = self
            .inputs
            .iter()
            .map(|input| input.execute(partition, task_context.clone()))
            .collect::<DataFusionResult<Vec<SendableRecordBatchStream>>>()?;

        Ok(Box::pin(SortedJoinStream::new(
            self.schema.clone(),
            self.return_order.clone(),
            streams,
            BaselineMetrics::new(&self.metrics, partition),
        )))
    }

    /// Specify that [`SortedJoinExec`] knows nothing about the data it will output.
    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    /// Specify that [`SortedJoinStream`] requires one partition for each input as it assumes that
    /// the sort order is the same for all inputs and Apache DataFusion only guarantees the sort
    /// order within each partition rather than the inputs' global sort order.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; self.inputs.len()]
    }

    /// Specify that [`SortedJoinStream`] requires that its inputs' provide data that is sorted by
    /// `query_requirement_data_point`.
    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        vec![Some(self.query_requirement_data_point.clone()); self.inputs.len()]
    }

    /// Return a snapshot of the set of metrics being collected by the execution plain.
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for SortedJoinExec {
    /// Write a string-based representation of the operator to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.name())
    }
}

struct SortedJoinStream {
    /// Schema of the stream.
    schema: Arc<Schema>,
    /// Order of columns to return.
    return_order: Vec<SortedJoinColumnType>,
    /// Streams to read batches of data points from.
    inputs: Vec<SendableRecordBatchStream>,
    /// Current batch of data points to join from.
    batches: Vec<Option<RecordBatch>>,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    baseline_metrics: BaselineMetrics,
}

impl SortedJoinStream {
    fn new(
        schema: Arc<Schema>,
        return_order: Vec<SortedJoinColumnType>,
        inputs: Vec<SendableRecordBatchStream>,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        // Must be created before SortedJoinStream as inputs are moved into it.
        let batches = vec![None; inputs.len()];

        SortedJoinStream {
            schema,
            return_order,
            inputs,
            batches,
            baseline_metrics,
        }
    }

    /// Pools all inputs for [`RecordBatches`](RecordBatch) and returns [`None`] if a batch has been
    /// returned from all inputs, otherwise a [`Some`] stating the reason why it failed is returned.
    fn poll_all_pending_inputs(
        &mut self,
        cx: &mut StdTaskContext<'_>,
    ) -> Option<Poll<Option<DataFusionResult<RecordBatch>>>> {
        let mut reason_for_not_ok = None;
        for index in 0..self.batches.len() {
            if self.batches[index].is_none() {
                let poll = self.inputs[index].poll_next_unpin(cx);
                if let Poll::Ready(Some(Ok(batch))) = poll {
                    self.batches[index] = Some(batch);
                } else {
                    reason_for_not_ok = Some(poll);
                }
            }
        }
        reason_for_not_ok
    }

    /// Ensure all [`RecordBatches`](RecordBatch) pooled from the inputs contain the same number of
    /// rows and if not drop rows from the [`RecordBatches`](RecordBatch) that contain extra. This
    /// can occur as compressed segments are not transferred atomically to the remote data folder.
    fn set_batch_num_rows_to_smallest(&mut self) {
        // unwrap() is safe as a record batch is read from each input before this method is called.
        let first_batch_num_rows = self.batches[0].as_ref().unwrap().num_rows();

        let mut all_same_num_rows = true;
        let mut smallest_num_rows = usize::MAX;
        for batch in &self.batches {
            let batch_num_rows = batch.as_ref().unwrap().num_rows();
            all_same_num_rows = all_same_num_rows && batch_num_rows == first_batch_num_rows;
            smallest_num_rows = smallest_num_rows.min(batch_num_rows);
        }

        if !all_same_num_rows {
            self.batches = self
                .batches
                .iter()
                .map(|batch| Some(batch.as_ref().unwrap().slice(0, smallest_num_rows)))
                .collect();
        }
    }

    /// Create a [`RecordBatch`] containing the requested timestamp, field, and tag columns, delete
    /// the [`RecordBatches`](RecordBatch) read from the inputs, and return the [`RecordBatch`]
    /// containing the requested timestamp, field, and tag columns.
    fn sorted_join(&self) -> Poll<Option<DataFusionResult<RecordBatch>>> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields.len());

        // unwrap() is safe as a record batch is read from each input before this method is called.
        let batch = self.batches[0].as_ref().unwrap();

        // The batches are already in the correct order, so they can be appended.
        let mut field_index = 0;

        for element in &self.return_order {
            match element {
                SortedJoinColumnType::Timestamp => columns.push(batch.column(0).clone()),
                SortedJoinColumnType::Field => {
                    // unwrap() is safe as a record batch has already been read from each input.
                    let batch = self.batches[field_index].as_ref().unwrap();
                    columns.push(batch.column(1).clone());
                    field_index += 1;
                }
                SortedJoinColumnType::Tag(tag_column_name) => {
                    // unwrap() is safe as all tag columns are present in the schema.
                    columns.push(batch.column_by_name(tag_column_name).unwrap().clone());
                }
            }
        }

        // unwrap() is safe as SortedJoinStream has ordered columns to match the schema.
        let batch = RecordBatch::try_new(self.schema.clone(), columns).unwrap();
        Poll::Ready(Some(Ok(batch)))
    }
}

impl Stream for SortedJoinStream {
    /// Specify that [`SortedJoinStream`] returns [`DataFusionResult<RecordBatch>`] when polled.
    type Item = DataFusionResult<RecordBatch>;

    /// Try to poll the next batch of data points from the [`SortedJoinStream`] and returns:
    /// * `Poll::Pending` if the next batch is not yet ready.
    /// * `Poll::Ready(Some(Ok(batch)))` if the next batch is ready.
    /// * `Poll::Ready(None)` if the stream is empty.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut StdTaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(reason_for_not_ok) = self.poll_all_pending_inputs(cx) {
            reason_for_not_ok
        } else {
            self.set_batch_num_rows_to_smallest();
            let poll = self.sorted_join();
            for batch in &mut self.batches {
                *batch = None;
            }
            self.baseline_metrics.record_poll(poll)
        }
    }
}

impl RecordBatchStream for SortedJoinStream {
    /// Return the schema of the stream.
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
