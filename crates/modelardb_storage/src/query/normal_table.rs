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

//! Implementation of [`NormalTable`] which allows normal tables to be queried through Apache DataFusion.
//! It wraps a [`DeltaTable`] and forwards most method calls to it. However, for
//! [`TableProvider::scan()`] it updates the [`DeltaTable`] to the latest version and it implements
//! [`TableProvider::insert_into()`] so rows can be inserted with INSERT.

use std::borrow::Cow;
use std::{any::Any, sync::Arc};

use arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::common::Constraints;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use deltalake::DeltaTable;
use tonic::async_trait;

/// A queryable representation of a normal table. [`NormalTable`] wraps the [`TableProvider`]
/// [`DeltaTable`] and passes most methods calls directly to it. Thus, it can be registered with
/// Apache DataFusion. [`DeltaTable`] is extended in two ways, `delta_table` is updated to the
/// latest snapshot when accessed and support for inserting has been added.
#[derive(Debug)]
pub(crate) struct NormalTable {
    /// Access to the Delta Lake table.
    delta_table: DeltaTable,
    /// Where data should be written to.
    data_sink: Arc<dyn DataSink>,
}

impl NormalTable {
    pub(crate) fn new(delta_table: DeltaTable, data_sink: Arc<dyn DataSink>) -> Self {
        Self {
            delta_table,
            data_sink,
        }
    }
}

#[async_trait]
impl TableProvider for NormalTable {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self.delta_table.as_any()
    }

    /// Return the query schema of the normal table registered with Apache DataFusion.
    fn schema(&self) -> Arc<Schema> {
        TableProvider::schema(&self.delta_table)
    }

    /// Return the normal table's constraints.
    fn constraints(&self) -> Option<&Constraints> {
        self.delta_table.constraints()
    }

    /// Specify that normal tables are base tables and not views or temporary tables.
    fn table_type(&self) -> TableType {
        self.delta_table.table_type()
    }

    /// Get the create statement used to create this normal table, if available.
    fn get_table_definition(&self) -> Option<&str> {
        self.delta_table.get_table_definition()
    }

    /// Get the [`LogicalPlan`] of this normal table, if available.
    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.delta_table.get_logical_plan()
    }

    /// Get the default value for a column, if available.
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.delta_table.get_column_default(column)
    }

    /// Create an [`ExecutionPlan`] that will scan the normal table. Returns a
    /// [`DataFusionError::Plan`] if the necessary metadata cannot be retrieved.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Clone the Delta Lake table and update it to the latest version. self.delta_lake.load(
        // &mut self) is not an option due to TypeProvider::scan(&self, ...). Storing the DeltaTable
        // in a Mutex and RwLock is also not an option since most of the methods in TypeProvider
        // return a reference and the locks will be dropped at the end of the method.
        let mut delta_table = self.delta_table.clone();
        delta_table
            .load()
            .await
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;

        delta_table.scan(state, projection, filters, limit).await
    }

    /// Specify that predicate push-down is supported by normal tables.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.delta_table.supports_filters_pushdown(filters)
    }

    /// Get statistics for this normal table, if available.
    fn statistics(&self) -> Option<Statistics> {
        self.delta_table.statistics()
    }

    /// Create an [`ExecutionPlan`] that will insert the result of `input` into the normal table.
    /// Generally, [`arrow_flight::flight_service_server::FlightService::do_put()`] should be used
    /// instead of this method as it is more efficient. Returns a [`DataFusionError::Plan`] if the
    /// necessary metadata cannot be retrieved from the metadata Delta Lake.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let file_sink = Arc::new(DataSinkExec::new(input, self.data_sink.clone(), None));
        Ok(file_sink)
    }
}
