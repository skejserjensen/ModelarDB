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

//! Implementation of the Apache DataFusion execution plan [`GridExec`] and its corresponding
//! stream [`GridStream`] which reconstructs the data points for a specific column from the
//! compressed segments containing metadata and models.

use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Formatter, Result as FmtResult};
use std::pin::Pin;
use std::slice;
use std::sync::Arc;
use std::task::{Context as StdTaskContext, Poll};

use arrow::array::{StringArray, StringBuilder};
use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryArray, Float32Array, Int8Array,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, LexRequirement};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PhysicalExpr, PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::stream::{Stream, StreamExt};
use modelardb_compression::{self, MODEL_TYPE_COUNT, MODEL_TYPE_NAMES};
use modelardb_types::schemas::QUERY_COMPRESSED_SCHEMA;
use modelardb_types::types::{TimestampArray, TimestampBuilder, ValueArray, ValueBuilder};

/// An execution plan that reconstructs the data points stored as compressed segments containing
/// metadata and models. It is `pub(crate)` so the additional rules added to Apache DataFusion's
/// physical optimizer can pattern match on it.
#[derive(Debug, Clone)]
pub(crate) struct GridExec {
    /// Schema of the execution plan.
    schema: Arc<Schema>,
    /// Predicate to filter data points by.
    maybe_predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Number of data points requested by the query.
    limit: Option<usize>,
    /// Execution plan to read batches of segments from.
    input: Arc<dyn ExecutionPlan>,
    /// Properties about the plan used in query optimization.
    plan_properties: PlanProperties,
    /// The sort order that [`GridExec`] requires for the segments it receives as its input.
    query_requirement_segment: LexRequirement,
    /// The sort order [`GridExec`] guarantees for the data points it produces.
    query_order_data_point: LexOrdering,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    metrics: ExecutionPlanMetricsSet,
}

impl GridExec {
    pub(super) fn new(
        schema: Arc<Schema>,
        maybe_predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
        input: Arc<dyn ExecutionPlan>,
        query_requirement_segment: LexRequirement,
        query_order_data_point: LexOrdering,
    ) -> Arc<Self> {
        // The sort order for the data points produced by the set of GridExec instances producing
        // input for a SortedJoinExec must be the same. This is needed because SortedJoinExec
        // assumes the data it receives from all of its inputs uses the same sort order.
        let equivalence_properties = EquivalenceProperties::new_with_orderings(
            schema.clone(),
            slice::from_ref(&query_order_data_point),
        );

        let plan_properties = PlanProperties::new(
            equivalence_properties,
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Arc::new(GridExec {
            maybe_predicate,
            schema,
            limit,
            input,
            plan_properties,
            query_requirement_segment,
            query_order_data_point,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[async_trait]
impl ExecutionPlan for GridExec {
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
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// Return a new [`GridExec`] with the execution plan to read batches of compressed segments
    /// from replaced. [`DataFusionError::Plan`] is returned if `children` does not contain a single
    /// element.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(GridExec::new(
                self.schema.clone(),
                self.maybe_predicate.clone(),
                self.limit,
                children[0].clone(),
                self.query_requirement_segment.clone(),
                self.query_order_data_point.clone(),
            ))
        } else {
            Err(DataFusionError::Plan(format!(
                "Exactly one child must be provided {self:?}.",
            )))
        }
    }

    /// Create a stream that reads batches of compressed segments from the child stream,
    /// reconstructs the data points from the metadata and models in the segments, and returns
    /// batches of rows with data points.
    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Must be read before GridStream as task_context is moved into input.
        let batch_size = task_context.session_config().batch_size();
        let grid_stream_metrics = GridStreamMetrics::new(&self.metrics, partition);

        Ok(Box::pin(GridStream::new(
            self.schema.clone(),
            self.maybe_predicate.clone(),
            self.limit,
            self.input.execute(partition, task_context)?,
            batch_size,
            grid_stream_metrics,
        )))
    }

    /// Specify that [`GridExec`] knows nothing about the data it will output.
    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    /// Specify that [`GridExec`] requires one partition for each input as it assumes that the
    /// sort order are the same for its input and Apache DataFusion only guarantees the sort order
    /// within each partition rather than the input's global sort order.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    /// Specify that [`GridExec`] requires that its input provides data that is sorted by
    /// `query_requirement_segment`.
    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        vec![Some(self.query_requirement_segment.clone())]
    }

    /// Return a snapshot of the set of metrics being collected by the execution plain.
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for GridExec {
    /// Write a string-based representation of the operator to `f`. Returns
    /// `Err` if `std::write` cannot format the string and write it to `f`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}: limit={:?}", self.name(), self.limit)
    }
}

/// A stream that read batches of rows with segments from the input stream, reconstructs the data
/// points from the metadata and models in the segments, and returns batches of data points.
struct GridStream {
    /// Schema of the stream.
    schema: Arc<Schema>,
    /// Predicate to filter data points by.
    maybe_predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Stream to read batches of compressed segments from.
    input: SendableRecordBatchStream,
    /// Size of the batches returned when this stream is pooled.
    batch_size: usize,
    /// Current batch of data points to return data points from when the stream is pooled.
    current_batch: RecordBatch,
    /// Next data point in the current batch of data points to return when the stream is pooled.
    current_batch_offset: usize,
    /// Metrics collected during execution for use by EXPLAIN ANALYZE.
    grid_stream_metrics: GridStreamMetrics,
}

impl GridStream {
    fn new(
        schema: Arc<Schema>,
        maybe_predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
        input: SendableRecordBatchStream,
        batch_size: usize,
        grid_stream_metrics: GridStreamMetrics,
    ) -> Self {
        // Assumes limit is mostly used to request less than batch_size rows so one batch is enough.
        // If it is a bit larger than batch_size the second batch will contain too many data points.
        // Also limit is not simply used as batch size to prevent OOM issues with a very big limit.
        let batch_size = if let Some(limit) = limit {
            usize::min(limit, batch_size)
        } else {
            batch_size
        };

        GridStream {
            schema: schema.clone(),
            maybe_predicate,
            input,
            batch_size,
            current_batch: RecordBatch::new_empty(schema),
            current_batch_offset: 0,
            grid_stream_metrics,
        }
    }

    /// Replace the current batch with a sorted [`RecordBatch`] that contains the remaining data
    /// points in the current batch and those reconstructed from the compressed segments in `batch`.
    fn grid_and_append_to_leftovers_in_current_batch(&mut self, batch: &RecordBatch) {
        // Record the time elapsed from the timer is created to it is dropped.
        let _timer = self
            .grid_stream_metrics
            .baseline_metrics
            .elapsed_compute()
            .timer();

        // Retrieve the arrays from batch and cast them to their concrete type.
        modelardb_types::arrays!(
            batch,
            model_type_ids,
            start_times,
            end_times,
            timestamps,
            min_values,
            max_values,
            values,
            residuals,
            _error_array
        );

        let mut tag_arrays =
            Vec::with_capacity(batch.num_columns() - QUERY_COMPRESSED_SCHEMA.0.fields().len());
        for tag_index in QUERY_COMPRESSED_SCHEMA.0.fields().len()..batch.num_columns() {
            tag_arrays.push(modelardb_types::array!(batch, tag_index, StringArray));
        }

        // Allocate builders with approximately enough capacity. The builders are allocated with
        // enough capacity for the remaining data points in the current batch and one data point
        // from each segment in the new batch as each segment contains at least one data point.
        let current_rows = self.current_batch.num_rows() - self.current_batch_offset;
        let new_rows = batch.num_rows();
        let mut timestamp_builder = TimestampBuilder::with_capacity(current_rows + new_rows);
        let mut value_builder = ValueBuilder::with_capacity(current_rows + new_rows);

        let mut tag_builders = Vec::with_capacity(tag_arrays.len());
        for _ in 0..tag_arrays.len() {
            tag_builders.push(StringBuilder::with_capacity(
                current_rows + new_rows,
                current_rows + new_rows,
            ));
        }

        // Copy over the data points from the current batch to keep the resulting batch sorted.
        let current_batch = &self.current_batch; // Required as self cannot be passed to array!.
        timestamp_builder.append_slice(
            &modelardb_types::array!(current_batch, 0, TimestampArray).values()
                [self.current_batch_offset..],
        );
        value_builder.append_slice(
            &modelardb_types::array!(current_batch, 1, ValueArray).values()
                [self.current_batch_offset..],
        );

        for (index, tag_builder) in tag_builders.iter_mut().enumerate() {
            let tag_array = modelardb_types::array!(current_batch, index + 2, StringArray);

            // Append each value individually since StringBuilder does not have an append_slice() method.
            for i in self.current_batch_offset..current_batch.num_rows() {
                tag_builder.append_value(tag_array.value(i));
            }
        }

        // Reconstruct the data points from the compressed segments.
        for row_index in 0..new_rows {
            let length_before = value_builder.len();

            modelardb_compression::grid(
                model_type_ids.value(row_index),
                start_times.value(row_index),
                end_times.value(row_index),
                timestamps.value(row_index),
                min_values.value(row_index),
                max_values.value(row_index),
                values.value(row_index),
                residuals.value(row_index),
                &mut timestamp_builder,
                &mut value_builder,
            );

            let created_rows = value_builder.len() - length_before;

            for (tag_builder, tag_array) in tag_builders.iter_mut().zip(&tag_arrays) {
                let tag_value = tag_array.value(row_index);
                for _ in 0..created_rows {
                    tag_builder.append_value(tag_value);
                }
            }

            self.grid_stream_metrics.add(
                model_type_ids.value(row_index),
                created_rows,
                !residuals.value(row_index).is_empty(),
                modelardb_compression::are_compressed_timestamps_regular(timestamps.values()),
            );
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(tag_builders.len() + 2);
        columns.push(Arc::new(timestamp_builder.finish()));
        columns.push(Arc::new(value_builder.finish()));

        for mut tag_builder in tag_builders {
            columns.push(Arc::new(tag_builder.finish()));
        }

        // Update the current batch, unwrap() is safe as GridStream uses a static schema.
        // For simplicity, all data points are reconstructed and then pruned by time.
        let current_batch = RecordBatch::try_new(self.schema.clone(), columns).unwrap();

        self.current_batch = if let Some(predicate) = &self.maybe_predicate {
            // unwrap() is safe as the predicate has been written for the schema.
            let column_value = predicate.evaluate(&current_batch).unwrap();
            let array = column_value.into_array(current_batch.num_rows()).unwrap();
            let boolean_array = as_boolean_array(&array).unwrap();
            filter_record_batch(&current_batch, boolean_array).unwrap()
        } else {
            current_batch
        };

        // As a new batch have been created the offset into this batch must be set to zero.
        self.current_batch_offset = 0;
    }
}

impl Stream for GridStream {
    /// Specify that [`GridStream`] returns [`DataFusionResult<RecordBatch>`] when polled.
    type Item = DataFusionResult<RecordBatch>;

    /// Try to poll the next batch of data points from the [`GridStream`] and returns:
    /// * `Poll::Pending` if the next batch is not yet ready.
    /// * `Poll::Ready(Some(Ok(batch)))` if the next batch is ready.
    /// * `Poll::Ready(None)` if the stream is empty.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut StdTaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Try to ensure there are enough data points in the current batch to match batch size.
        if (self.current_batch.num_rows() - self.current_batch_offset) < self.batch_size {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.grid_and_append_to_leftovers_in_current_batch(&batch);
                }
                Poll::Ready(None) if self.current_batch_offset < self.current_batch.num_rows() => {
                    // Ignore Poll::Ready(None) as there are data points in the current buffer.
                }
                other => return self.grid_stream_metrics.baseline_metrics.record_poll(other),
            }
        }

        // While input uses the same batch size as self and each compressed segment is guaranteed to
        // represent one data point, the current batch may not contain enough data points, e.g., if
        // the query contains a very specific predicate that filter out all but a very few segments.
        let remaining_data_points = self.current_batch.num_rows() - self.current_batch_offset;
        let length = usize::min(self.batch_size, remaining_data_points);
        let batch = self.current_batch.slice(self.current_batch_offset, length);
        self.current_batch_offset += batch.num_rows();
        self.grid_stream_metrics
            .baseline_metrics
            .record_poll(Poll::Ready(Some(Ok(batch))))
    }
}

impl RecordBatchStream for GridStream {
    /// Return the schema of the stream.
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// Metrics collected by [`GridStream`] for use by EXPLAIN ANALYZE.
#[derive(Debug)]
struct GridStreamMetrics {
    /// Default set of metrics collected by operators.
    baseline_metrics: BaselineMetrics,
    /// Number of data points reconstructed from segments.
    rows_created: Count,
    /// Number of data points reconstructed from segments grouped by model type.
    rows_created_by_model_type: [Count; MODEL_TYPE_COUNT],
    /// Number of segments with residuals.
    segments_with_residuals: Count,
    /// Number of segments grouped by model type.
    segments_with_model_type: [Count; MODEL_TYPE_COUNT],
    /// Number of regular segments.
    segments_regular: Count,
    /// Number of irregular segments.
    segments_irregular: Count,
}

impl GridStreamMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let baseline_metrics = BaselineMetrics::new(metrics, partition);

        // Create metrics for collecting information about the number of created data points.
        // unwrap() is safe if the size of the arrays in GridStreamMetrics is MODEL_TYPE_COUNT.
        let rows_created = Self::new_counter(metrics, partition, "rows_created");
        let rows_created_by_model_type = MODEL_TYPE_NAMES
            .iter()
            .map(|name| Self::new_counter(metrics, partition, format!("rows_created_by_{name}")))
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();

        // Create metrics for collecting information about the number of segments processed.
        // unwrap() is safe if the size of the arrays in GridStreamMetrics is MODEL_TYPE_COUNT.
        let segments_with_residuals =
            Self::new_counter(metrics, partition, "segments_with_residuals");
        let segments_with_model_type = MODEL_TYPE_NAMES
            .iter()
            .map(|name| Self::new_counter(metrics, partition, format!("segments_with_{name}")))
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();

        let segments_regular = Self::new_counter(metrics, partition, "regular_segments");
        let segments_irregular = Self::new_counter(metrics, partition, "irregular_segments");

        Self {
            baseline_metrics,
            rows_created,
            rows_created_by_model_type,
            segments_with_residuals,
            segments_with_model_type,
            segments_regular,
            segments_irregular,
        }
    }

    /// Return a [`Count`] for `partition` with `counter_name` which is associated with `metrics`.
    fn new_counter(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        counter_name: impl Into<Cow<'static, str>>,
    ) -> Count {
        MetricBuilder::new(metrics)
            .with_partition(partition)
            .global_counter(counter_name)
    }

    /// Calculate all metrics and add them to [`Self`].
    fn add(&self, model_type_id: i8, created_rows: usize, has_residuals: bool, is_regular: bool) {
        self.rows_created.add(created_rows);
        self.rows_created_by_model_type[model_type_id as usize].add(created_rows);
        self.segments_with_residuals.add(has_residuals as usize);
        self.segments_with_model_type[model_type_id as usize].add(1);
        self.segments_regular.add(is_regular as usize);
        self.segments_irregular.add(!is_regular as usize);
    }
}
