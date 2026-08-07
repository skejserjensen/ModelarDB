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

//! Implementation of [`MetadataTable`] which allows metadata tables to be queried through Apache
//! DataFusion. It wraps a [`DeltaTable`] and forwards most method calls to it. However, for
//! [`TableProvider::scan()`] it updates the [`DeltaTable`] to the latest version.

use std::{any::Any, sync::Arc};

use arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use deltalake::DeltaTable;
use tonic::async_trait;

/// A queryable representation of a metadata table. [`MetadataTable`] wraps the [`TableProvider`] of
/// [`DeltaTable`] and passes most methods calls directly to it. Thus, it can be registered with
/// Apache DataFusion. The only difference from [`DeltaTable`] is that `delta_table` is updated to
/// the latest snapshot when accessed.
#[derive(Debug)]
pub(crate) struct MetadataTable {
    /// Access to the Delta Lake table.
    delta_table: DeltaTable,
}

impl MetadataTable {
    pub(crate) fn new(delta_table: DeltaTable) -> Self {
        Self { delta_table }
    }
}

#[async_trait]
impl TableProvider for MetadataTable {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self.delta_table.as_any()
    }

    /// Return the query schema of the metadata table registered with Apache DataFusion.
    fn schema(&self) -> Arc<Schema> {
        TableProvider::schema(&self.delta_table)
    }

    /// Specify that metadata tables are base tables and not views or temporary tables.
    fn table_type(&self) -> TableType {
        self.delta_table.table_type()
    }

    /// Create an [`ExecutionPlan`] that will scan the metadata table. Returns a
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
}
