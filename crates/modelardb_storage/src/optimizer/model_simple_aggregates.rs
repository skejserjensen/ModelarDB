/* Copyright 2021 The ModelarDB Contributors
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

//! Implementation of a physical optimizer rule that rewrites aggregates that are computed from
//! reconstructed values from a single column without filtering, so they are computed directly from
//! segments instead of the reconstructed values.

#![allow(clippy::unconditional_recursion)]

use std::mem;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{ArrayRef, BinaryArray, Int8Array};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{self, Volatility};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{Accumulator, ExecutionPlan, PhysicalExpr};
use datafusion::scalar::ScalarValue;
use modelardb_types::schemas::QUERY_COMPRESSED_SCHEMA;
use modelardb_types::types::{ArrowValue, TimestampArray, Value, ValueArray};

use crate::query::sorted_join_exec::SortedJoinExec;

/// The expressions extracting the arguments for the model-based aggregate functions from the
/// [`RecordBatches`](RecordBatch). They must match [`MODEL_AGGREGATE_TYPES`].
static MODEL_AGGREGATE_ARGS: LazyLock<Vec<Arc<dyn PhysicalExpr>>> = LazyLock::new(|| {
    QUERY_COMPRESSED_SCHEMA
        .0
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name().as_str(), index));
            column
        })
        .collect()
});

/// The types of the arguments for the model-based aggregate functions. They must match
/// [`MODEL_AGGREGATE_ARGS`].
static MODEL_AGGREGATE_TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| {
    QUERY_COMPRESSED_SCHEMA
        .0
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect()
});

/// [`AggregateFunctionExpr`] computing COUNT directly from segments.
static MODEL_AGGREGATE_COUNT: LazyLock<Arc<AggregateFunctionExpr>> = LazyLock::new(|| {
    let fun = Arc::new(logical_expr::create_udaf(
        "model_count",
        MODEL_AGGREGATE_TYPES.to_vec(),
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(ModelCountAccumulator { count: 0 }))),
        Arc::new(vec![DataType::Int64]),
    ));

    let aggregate_expr_builder = AggregateExprBuilder::new(fun, MODEL_AGGREGATE_ARGS.to_vec())
        .alias("model_count")
        .schema(QUERY_COMPRESSED_SCHEMA.0.clone())
        .build()
        .expect("Failed to create a model-based aggregate function for COUNT.");

    Arc::new(aggregate_expr_builder)
});

/// [`AggregateFunctionExpr`] computing MIN directly from segments.
static MODEL_AGGREGATE_MIN: LazyLock<Arc<AggregateFunctionExpr>> = LazyLock::new(|| {
    let fun = Arc::new(logical_expr::create_udaf(
        "model_min",
        MODEL_AGGREGATE_TYPES.to_vec(),
        Arc::new(ArrowValue::DATA_TYPE),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(ModelMinAccumulator { min: Value::MAX }))),
        Arc::new(vec![ArrowValue::DATA_TYPE]),
    ));

    let aggregate_expr_builder = AggregateExprBuilder::new(fun, MODEL_AGGREGATE_ARGS.to_vec())
        .alias("model_min")
        .schema(QUERY_COMPRESSED_SCHEMA.0.clone())
        .build()
        .expect("Failed to create a model-based aggregate function for MIN.");

    Arc::new(aggregate_expr_builder)
});

/// [`AggregateFunctionExpr`] computing MAX directly from segments.
static MODEL_AGGREGATE_MAX: LazyLock<Arc<AggregateFunctionExpr>> = LazyLock::new(|| {
    let fun = Arc::new(logical_expr::create_udaf(
        "model_max",
        MODEL_AGGREGATE_TYPES.to_vec(),
        Arc::new(ArrowValue::DATA_TYPE),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(ModelMaxAccumulator { max: Value::MIN }))),
        Arc::new(vec![ArrowValue::DATA_TYPE]),
    ));

    let aggregate_expr_builder = AggregateExprBuilder::new(fun, MODEL_AGGREGATE_ARGS.to_vec())
        .alias("model_max")
        .schema(QUERY_COMPRESSED_SCHEMA.0.clone())
        .build()
        .expect("Failed to create a model-based aggregate function for MAX.");

    Arc::new(aggregate_expr_builder)
});

/// [`AggregateFunctionExpr`] computing SUM directly from segments.
static MODEL_AGGREGATE_SUM: LazyLock<Arc<AggregateFunctionExpr>> = LazyLock::new(|| {
    let fun = Arc::new(logical_expr::create_udaf(
        "model_sum",
        MODEL_AGGREGATE_TYPES.to_vec(),
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(ModelSumAccumulator { sum: 0.0 }))),
        Arc::new(vec![DataType::Float64]),
    ));

    let aggregate_expr_builder = AggregateExprBuilder::new(fun, MODEL_AGGREGATE_ARGS.to_vec())
        .alias("model_sum")
        .schema(QUERY_COMPRESSED_SCHEMA.0.clone())
        .build()
        .expect("Failed to create a model-based aggregate function for SUM.");

    Arc::new(aggregate_expr_builder)
});

/// [`AggregateFunctionExpr`] computing AVG directly from segments.
static MODEL_AGGREGATE_AVG: LazyLock<Arc<AggregateFunctionExpr>> = LazyLock::new(|| {
    let fun = Arc::new(logical_expr::create_udaf(
        "model_avg",
        MODEL_AGGREGATE_TYPES.to_vec(),
        Arc::new(ArrowValue::DATA_TYPE),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(ModelAvgAccumulator { sum: 0.0, count: 0 }))),
        Arc::new(vec![DataType::UInt64, DataType::Float64]),
    ));

    let aggregate_expr_builder = AggregateExprBuilder::new(fun, MODEL_AGGREGATE_ARGS.to_vec())
        .alias("model_avg")
        .schema(QUERY_COMPRESSED_SCHEMA.0.clone())
        .build()
        .expect("Failed to create a model-based aggregate function for AVG.");

    Arc::new(aggregate_expr_builder)
});

/// Rewrite aggregates that are computed from reconstructed values from a single column without
/// filtering, so they are computed directly from segments instead of the reconstructed values.
#[derive(Debug)]
pub struct ModelSimpleAggregates {}

impl PhysicalOptimizerRule for ModelSimpleAggregates {
    /// Rewrite `execution_plan` so it computes simple aggregates without filtering from segments
    /// instead of reconstructed values.
    fn optimize(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        execution_plan
            .transform_down(&rewrite_aggregates_to_use_segments)
            .map(|transformed| transformed.data)
    }

    /// Return the name of the [`PhysicalOptimizerRule`].
    fn name(&self) -> &str {
        "model_simple_aggregates"
    }

    /// Specify if the physical planner should validate that the rule does not change the schema.
    fn schema_check(&self) -> bool {
        true
    }
}

/// Matches the pattern [`AggregateExpr`] <- [RepartitionExec] (if it exists) <- [`SortedJoinExec`]
/// <- [`GridExec`](crate::query::grid_exec::GridExec) <- [`DataSourceExec`] and rewrites it to
/// [`AggregateExpr`] <- [`DataSourceExec`] if [`AggregateExpr`] only computes aggregates for a
/// single FIELD column and no filtering is performed by [`DataSourceExec`].
fn rewrite_aggregates_to_use_segments(
    execution_plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Transformed<Arc<dyn ExecutionPlan>>> {
    // The rule tries to match the subtree of execution_plan so execution_plan can be updated.
    let execution_plan_children = execution_plan.children();

    if execution_plan_children.len() == 1
        && let Some(aggregate_exec) = execution_plan_children[0]
            .as_any()
            .downcast_ref::<AggregateExec>()
    {
        // Currently, only aggregates on one FIELD column without predicates are supported.
        let aggregate_exec_children = aggregate_exec.children();
        if aggregate_exec.input_schema().fields.len() == 1
            && *aggregate_exec.input_schema().field(0).data_type() == ArrowValue::DATA_TYPE
            && aggregate_exec.filter_expr().iter().all(Option::is_none)
            && aggregate_exec.group_expr().is_empty()
        {
            // Remove RepartitionExec if added by Apache DataFusion. Both AggregateExec and
            // RepartitionExec can only have one child, so it is not necessary to check it.
            let maybe_repartition_exec = &aggregate_exec_children[0];
            let aggregate_exec_input = if let Some(repartition_exec) = maybe_repartition_exec
                .as_any()
                .downcast_ref::<RepartitionExec>()
            {
                repartition_exec.children()[0].clone()
            } else {
                (*maybe_repartition_exec).clone()
            };

            if let Some(sorted_join_exec) = aggregate_exec_input
                .as_any()
                .downcast_ref::<SortedJoinExec>()
            {
                // Try to create new AggregateExec that compute aggregates directly from segments.
                if let Ok(input) =
                    try_new_aggregate_exec(aggregate_exec, sorted_join_exec.children())
                {
                    return Ok(Transformed::yes(
                        execution_plan.with_new_children(vec![input])?,
                    ));
                };
            }
        }
    }

    Ok(Transformed::no(execution_plan))
}

/// Return an [`AggregateExec`] that computes the same aggregates as `aggregate_exec` if no
/// predicates have been pushed to `inputs` and the aggregates are all computed for the same FIELD
/// column, otherwise [`DataFusionError`] is returned.
fn try_new_aggregate_exec(
    aggregate_exec: &AggregateExec,
    grid_execs: Vec<&Arc<dyn ExecutionPlan>>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    // aggregate_exec.input_schema().fields.len() == 1 should prevent this, but SortedJoinExec can
    // be constructed with multiple inputs so the number of children is checked just to be sure.
    if grid_execs.len() > 1 {
        return Err(DataFusionError::NotImplemented(
            "All aggregates must be for the same FIELD column.".to_owned(),
        ));
    }

    // A GridExec can only have one child, so it is not necessary to check it.
    let grid_exec_child = grid_execs[0].children().remove(0);
    can_rewrite_aggregate(grid_exec_child)?;
    let model_based_aggregate_exprs = try_rewrite_aggregate_exprs(aggregate_exec)?;

    Ok(Arc::new(AggregateExec::try_new(
        *aggregate_exec.mode(),
        aggregate_exec.group_expr().clone(),
        model_based_aggregate_exprs,
        aggregate_exec.filter_expr().to_vec(),
        grid_execs[0].children()[0].clone(),
        aggregate_exec.schema(),
    )?))
}

/// Return [`Ok`] if no predicates have been pushed to `grid_exec_child`, otherwise
/// [`DataFusionError`] is returned.
fn can_rewrite_aggregate(grid_exec_child: &Arc<dyn ExecutionPlan>) -> DataFusionResult<()> {
    if let Some(data_source_exec) = grid_exec_child.as_any().downcast_ref::<DataSourceExec>()
        && let Some(file_scan_config) = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
        && let Some(parquet_source) = file_scan_config
            .file_source
            .as_any()
            .downcast_ref::<ParquetSource>()
        && parquet_source.predicate().is_none()
    {
        return Ok(());
    }

    Err(DataFusionError::Plan(
        "The input to GridExec must be a DataSourceExec without predicates.".to_owned(),
    ))
}

/// Return [`AggregateFunctionExprs`](AggregateFunctionExpr) that computes the same aggregates as
/// `aggregate_exec` if the aggregates are all computed for the same FIELD column, otherwise
/// [`DataFusionError`] is returned.
fn try_rewrite_aggregate_exprs(
    aggregate_exec: &AggregateExec,
) -> DataFusionResult<Vec<Arc<AggregateFunctionExpr>>> {
    let aggregate_function_exprs = aggregate_exec.aggr_expr();
    let mut rewritten_aggregate_exprs: Vec<Arc<AggregateFunctionExpr>> =
        Vec::with_capacity(aggregate_function_exprs.len());

    for aggregate_function_expr in aggregate_function_exprs {
        let name_with_column = aggregate_function_expr.name();
        let name_end = name_with_column.find('(').unwrap_or(name_with_column.len());

        let model_aggregate_function_expr = match &name_with_column[..name_end] {
            "count" => MODEL_AGGREGATE_COUNT.clone(),
            "min" => MODEL_AGGREGATE_MIN.clone(),
            "max" => MODEL_AGGREGATE_MAX.clone(),
            "sum" => MODEL_AGGREGATE_SUM.clone(),
            "avg" => MODEL_AGGREGATE_AVG.clone(),
            name => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Aggregate expression {name} is not supported.",
                )));
            }
        };
        rewritten_aggregate_exprs.push(model_aggregate_function_expr);
    }

    Ok(rewritten_aggregate_exprs)
}

/// [`Accumulator`] that computes `COUNT` directly from segments containing metadata and models.
#[derive(Debug)]
struct ModelCountAccumulator {
    /// Current count stored by the [`Accumulator`].
    count: i64,
}

impl Accumulator for ModelCountAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, arrays: &[ArrayRef]) -> DataFusionResult<()> {
        let start_times = modelardb_types::value!(arrays, 1, TimestampArray);
        let end_times = modelardb_types::value!(arrays, 2, TimestampArray);
        let timestamps = modelardb_types::value!(arrays, 3, BinaryArray);

        for row_index in 0..start_times.len() {
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let timestamps = timestamps.value(row_index);

            self.count += modelardb_compression::len(start_time, end_time, timestamps) as i64;
        }

        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![ScalarValue::Int64(Some(self.count))];
        self.count = 0;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`Accumulator`] that computes `MIN` directly from segments containing metadata and models.
#[derive(Debug)]
struct ModelMinAccumulator {
    /// Current minimum value stored by the [`Accumulator`].
    min: f32,
}

impl Accumulator for ModelMinAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let min_values = modelardb_types::value!(values, 4, ValueArray);
        for row_index in 0..min_values.len() {
            self.min = Value::min(self.min, min_values.value(row_index));
        }

        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![ScalarValue::Float32(Some(self.min))];
        self.min = f32::MAX;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`Accumulator`] that computes `MAX` directly from segments containing metadata and models.
#[derive(Debug)]
struct ModelMaxAccumulator {
    /// Current maximum value stored by the [`Accumulator`].
    max: f32,
}

impl Accumulator for ModelMaxAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, arrays: &[ArrayRef]) -> DataFusionResult<()> {
        let max_values = modelardb_types::value!(arrays, 5, ValueArray);
        for row_index in 0..max_values.len() {
            self.max = Value::max(self.max, max_values.value(row_index));
        }

        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`].
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![ScalarValue::Float32(Some(self.max))];
        self.max = f32::MIN;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`Accumulator`] that computes `SUM` directly from segments containing metadata and models.
#[derive(Debug)]
struct ModelSumAccumulator {
    /// Current sum stored by the [`Accumulator`].
    sum: f64,
}

impl Accumulator for ModelSumAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, arrays: &[ArrayRef]) -> DataFusionResult<()> {
        let model_type_ids = modelardb_types::value!(arrays, 0, Int8Array);
        let start_times = modelardb_types::value!(arrays, 1, TimestampArray);
        let end_times = modelardb_types::value!(arrays, 2, TimestampArray);
        let timestamps = modelardb_types::value!(arrays, 3, BinaryArray);
        let min_values = modelardb_types::value!(arrays, 4, ValueArray);
        let max_values = modelardb_types::value!(arrays, 5, ValueArray);
        let values = modelardb_types::value!(arrays, 6, BinaryArray);
        let residuals = modelardb_types::value!(arrays, 7, BinaryArray);

        for row_index in 0..model_type_ids.len() {
            let model_type_id = model_type_ids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let timestamps = timestamps.value(row_index);
            let min_value = min_values.value(row_index);
            let max_value = max_values.value(row_index);
            let values = values.value(row_index);
            let residuals = residuals.value(row_index);

            self.sum += modelardb_compression::sum(
                model_type_id,
                start_time,
                end_time,
                timestamps,
                min_value,
                max_value,
                values,
                residuals,
            ) as f64;
        }

        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`]. It must match
    /// [SumAccumulator](repository/datafusion/physical-expr/src/aggregate/sum.rs).
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![ScalarValue::Float64(Some(self.sum))];
        self.sum = 0.0;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

/// [`Accumulator`] that computes `AVG` directly from segments containing metadata and models.
#[derive(Debug)]
struct ModelAvgAccumulator {
    /// The current sum stored in the [`Accumulator`].
    sum: f64,
    /// The current count stored in the [`Accumulator`].
    count: u64,
}

impl Accumulator for ModelAvgAccumulator {
    /// Update the [`Accumulators`](Accumulator) state from `values`.
    fn update_batch(&mut self, arrays: &[ArrayRef]) -> DataFusionResult<()> {
        let model_type_ids = modelardb_types::value!(arrays, 0, Int8Array);
        let start_times = modelardb_types::value!(arrays, 1, TimestampArray);
        let end_times = modelardb_types::value!(arrays, 2, TimestampArray);
        let timestamps = modelardb_types::value!(arrays, 3, BinaryArray);
        let min_values = modelardb_types::value!(arrays, 4, ValueArray);
        let max_values = modelardb_types::value!(arrays, 5, ValueArray);
        let values = modelardb_types::value!(arrays, 6, BinaryArray);
        let residuals = modelardb_types::value!(arrays, 7, BinaryArray);

        for row_index in 0..model_type_ids.len() {
            let model_type_id = model_type_ids.value(row_index);
            let start_time = start_times.value(row_index);
            let end_time = end_times.value(row_index);
            let timestamps = timestamps.value(row_index);
            let min_value = min_values.value(row_index);
            let max_value = max_values.value(row_index);
            let values = values.value(row_index);
            let residuals = residuals.value(row_index);

            self.sum += modelardb_compression::sum(
                model_type_id,
                start_time,
                end_time,
                timestamps,
                min_value,
                max_value,
                values,
                residuals,
            ) as f64;

            self.count += modelardb_compression::len(start_time, end_time, timestamps) as u64;
        }

        Ok(())
    }

    /// Panics as this method should never be called.
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DataFusionResult<()> {
        unreachable!()
    }

    /// Return the current state of the [`Accumulator`]. It must match
    /// [AvgAccumulator](repository/datafusion/physical-expr/src/aggregate/average.rs).
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        // After this, the accumulators state should be equivalent to when it was created.
        let state = vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Float64(Some(self.sum)),
        ];
        self.sum = 0.0;
        self.count = 0;
        Ok(state)
    }

    /// Panics as this method should never be called.
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // After this, the accumulators state should be equivalent to when it was created.
        unreachable!()
    }

    /// Return the size of the [`Accumulator`] including `Self`.
    fn size(&self) -> usize {
        mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::any::{Any, TypeId};
    use std::fmt::{Debug, Formatter, Result as FmtResult};

    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::sink::DataSink;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_plan::aggregates::AggregateExec;
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::metrics::MetricsSet;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
    use datafusion::prelude::SessionContext;
    use modelardb_test::table::{self, TIME_SERIES_TABLE_NAME};
    use tempfile::TempDir;
    use tonic::async_trait;

    use crate::delta_lake::DeltaLake;
    use crate::optimizer;
    use crate::query::grid_exec::GridExec;
    use crate::query::time_series_table::TimeSeriesTable;

    // DataSink for testing.
    struct NoOpDataSink {}

    #[async_trait]
    impl DataSink for NoOpDataSink {
        fn as_any(&self) -> &dyn Any {
            unimplemented!();
        }

        fn schema(&self) -> &Arc<Schema> {
            unimplemented!();
        }

        fn metrics(&self) -> Option<MetricsSet> {
            unimplemented!();
        }

        async fn write_all(
            &self,
            _data: SendableRecordBatchStream,
            _context: &Arc<TaskContext>,
        ) -> DataFusionResult<u64> {
            unimplemented!();
        }
    }

    impl Debug for NoOpDataSink {
        fn fmt(&self, _f: &mut Formatter<'_>) -> FmtResult {
            unimplemented!();
        }
    }

    impl DisplayAs for NoOpDataSink {
        fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter<'_>) -> FmtResult {
            unimplemented!();
        }
    }

    // Tests for ModelSimpleAggregates.
    #[tokio::test]
    async fn test_rewrite_aggregate_on_one_column_without_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query = &format!("SELECT COUNT(field_1) FROM {TIME_SERIES_TABLE_NAME}");
        let physical_plan = query_optimized_physical_query_plan(&temp_dir, query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<DataSourceExec>()],
        ];

        assert_eq_physical_plan_expected(physical_plan, &expected_plan);
    }

    #[tokio::test]
    async fn test_rewrite_aggregates_on_one_column_without_predicates() {
        // Apache DataFusion 30 creates two input columns to AggregateExec when both SUM and AVG is
        // computed in the same query, so for now, multiple queries are used for the test.
        let query_no_avg = &format!(
            "SELECT COUNT(field_1), MIN(field_1), MAX(field_1), SUM(field_1) FROM {TIME_SERIES_TABLE_NAME}"
        );
        let query_only_avg = &format!("SELECT AVG(field_1) FROM {TIME_SERIES_TABLE_NAME}");

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<DataSourceExec>()],
        ];

        for query in [query_no_avg, query_only_avg] {
            let temp_dir = tempfile::tempdir().unwrap();

            let physical_plan = query_optimized_physical_query_plan(&temp_dir, query).await;
            assert_eq_physical_plan_expected(physical_plan, &expected_plan);
        }
    }

    #[tokio::test]
    async fn test_do_not_rewrite_aggregate_on_one_column_with_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query =
            &format!("SELECT COUNT(field_1) FROM {TIME_SERIES_TABLE_NAME} WHERE field_1 = 37.0",);
        let physical_plan = query_optimized_physical_query_plan(&temp_dir, query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalesceBatchesExec>()],
            vec![TypeId::of::<FilterExec>()],
            vec![TypeId::of::<RepartitionExec>()],
            vec![TypeId::of::<SortedJoinExec>()],
            vec![TypeId::of::<GridExec>()],
            vec![TypeId::of::<DataSourceExec>()],
        ];

        assert_eq_physical_plan_expected(physical_plan, &expected_plan);
    }

    #[tokio::test]
    async fn test_do_not_rewrite_aggregate_on_multiple_columns_without_predicates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query =
            &format!("SELECT COUNT(field_1), COUNT(field_2) FROM {TIME_SERIES_TABLE_NAME}",);
        let physical_plan = query_optimized_physical_query_plan(&temp_dir, query).await;

        let expected_plan = vec![
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<CoalescePartitionsExec>()],
            vec![TypeId::of::<AggregateExec>()],
            vec![TypeId::of::<RepartitionExec>()],
            vec![TypeId::of::<SortedJoinExec>()],
            vec![TypeId::of::<GridExec>(), TypeId::of::<GridExec>()],
            vec![
                TypeId::of::<DataSourceExec>(),
                TypeId::of::<DataSourceExec>(),
            ],
        ];

        assert_eq_physical_plan_expected(physical_plan, &expected_plan);
    }

    /// Parse, plan, and optimize the `query` for execution on data in `path`.
    async fn query_optimized_physical_query_plan(
        temp_dir: &TempDir,
        query: &str,
    ) -> Arc<dyn ExecutionPlan> {
        // Setup access to data and metadata in data folder.
        let data_folder_path = temp_dir.path();
        let delta_lake = DeltaLake::try_from_local_path(data_folder_path).unwrap();

        // Setup access to Apache DataFusion.
        let mut session_state_builder = SessionStateBuilder::new().with_default_features();

        // Uses the rule method instead of the rules method as the rules method replaces the built-ins.
        for physical_optimizer_rule in optimizer::physical_optimizer_rules() {
            session_state_builder =
                session_state_builder.with_physical_optimizer_rule(physical_optimizer_rule);
        }

        let session_state = session_state_builder.build();
        let session_context = SessionContext::new_with_state(session_state);

        // Create time series table.
        let time_series_table_metadata = table::time_series_table_metadata_arc();

        let delta_table = delta_lake
            .create_time_series_table(&time_series_table_metadata)
            .await
            .unwrap();

        let time_series_table_data_sink = Arc::new(NoOpDataSink {});

        let time_series_table = TimeSeriesTable::new(
            delta_table,
            time_series_table_metadata.clone(),
            time_series_table_data_sink,
        );

        session_context
            .register_table(TIME_SERIES_TABLE_NAME, time_series_table)
            .unwrap();

        session_context
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    /// Assert that `physical_plan` and `expected_plan` contain the same operators. `expected_plan`
    /// only contains the type ids so the tests do not have to construct the actual operators.
    fn assert_eq_physical_plan_expected(
        physical_plan: Arc<dyn ExecutionPlan>,
        expected_plan: &[Vec<TypeId>],
    ) {
        let mut level = 0;
        let mut current_execs = vec![physical_plan];
        let mut next_execs = vec![];

        while !current_execs.is_empty() {
            let expected_execs = &expected_plan[level];
            assert_eq!(current_execs.len(), expected_execs.len());

            for (current, expected) in current_execs.iter().zip(expected_execs) {
                assert_eq!(current.as_any().type_id(), *expected);
                next_execs.extend(current.children().iter().map(|exec| (*exec).clone()));
            }

            level += 1;
            current_execs = next_execs;
            next_execs = vec![];
        }
    }
}
