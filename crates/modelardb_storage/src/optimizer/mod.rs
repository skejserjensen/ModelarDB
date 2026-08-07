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

//! Implementation of physical optimizer rules that rewrites the physical plans produced by Apache
//! Arrow DataFusion to execute queries more efficiently directly on the segments with metadata and
//! models stored in time series tables.

mod model_simple_aggregates;

use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use model_simple_aggregates::ModelSimpleAggregates;

/// Return all physical optimizer rules. Currently, only simple aggregates are executed on segments.
pub fn physical_optimizer_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![Arc::new(ModelSimpleAggregates {})]
}
