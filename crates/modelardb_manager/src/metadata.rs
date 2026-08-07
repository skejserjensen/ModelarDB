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

//! Management of the metadata Delta Lake for the manager. Metadata which is unique to the manager,
//! such as metadata about registered edges, is handled here.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use deltalake::DeltaTableError;
use deltalake::datafusion::logical_expr::{col, lit};
use deltalake::datafusion::prelude::SessionContext;
use modelardb_storage::delta_lake::DeltaLake;
use modelardb_storage::metadata::table_metadata_manager::TableMetadataManager;
use modelardb_storage::{register_metadata_table, sql_and_concat};
use modelardb_types::flight::protocol;
use modelardb_types::types::{Node, ServerMode};
use uuid::Uuid;

use crate::error::Result;

/// Stores the metadata required for reading from and writing to the normal tables and time series tables
/// and persisting edges. The data that needs to be persisted is stored in the metadata Delta Lake.
pub struct MetadataManager {
    /// Delta Lake with functionality to read and write to and from the manager metadata tables.
    delta_lake: DeltaLake,
    /// Metadata manager used to interface with the subset of the manager metadata Delta Lake
    /// related to normal tables and time series tables.
    pub(crate) table_metadata_manager: TableMetadataManager,
    /// Session context used to query the manager metadata Delta Lake tables using Apache DataFusion.
    session_context: Arc<SessionContext>,
}

impl MetadataManager {
    /// Create a new [`MetadataManager`] that saves the metadata to a remote object store given by
    /// `storage_configuration` and initialize the metadata tables. If a connection could not be
    /// made or the metadata tables could not be created, return
    /// [`ModelarDbManagerError`](crate::error::ModelarDbManagerError).
    pub async fn try_from_storage_configuration(
        storage_configuration: protocol::manager_metadata::StorageConfiguration,
    ) -> Result<MetadataManager> {
        let metadata_manager = Self {
            delta_lake: DeltaLake::try_remote_from_storage_configuration(
                storage_configuration.clone(),
            )?,
            table_metadata_manager: TableMetadataManager::try_from_storage_configuration(
                storage_configuration,
            )
            .await?,
            session_context: Arc::new(SessionContext::new()),
        };

        // Create the necessary tables in the metadata Delta Lake.
        metadata_manager
            .create_and_register_manager_metadata_delta_lake_tables()
            .await?;

        Ok(metadata_manager)
    }

    /// If they do not already exist, create the tables that are specific to the manager metadata
    /// Delta Lake and register them with the Apache DataFusion session context.
    /// * The `manager_metadata` table contains metadata for the manager itself. It is assumed that
    ///   this table will only have a single row since there can only be a single manager.
    /// * The `nodes` table contains metadata for each node that is controlled by the manager.
    ///
    /// If the tables exist or were created, return [`Ok`], otherwise return
    /// [`ModelarDbManagerError`](crate::error::ModelarDbManagerError).
    async fn create_and_register_manager_metadata_delta_lake_tables(&self) -> Result<()> {
        // Create and register the manager_metadata table if it does not exist.
        let delta_table = self
            .delta_lake
            .create_metadata_table(
                "manager_metadata",
                &Schema::new(vec![Field::new("key", DataType::Utf8, false)]),
            )
            .await?;

        register_metadata_table(&self.session_context, "manager_metadata", delta_table)?;

        // Create and register the nodes table if it does not exist.
        let delta_table = self
            .delta_lake
            .create_metadata_table(
                "nodes",
                &Schema::new(vec![
                    Field::new("url", DataType::Utf8, false),
                    Field::new("mode", DataType::Utf8, false),
                ]),
            )
            .await?;

        register_metadata_table(&self.session_context, "nodes", delta_table)?;

        Ok(())
    }

    /// Retrieve the key for the manager from the `manager_metadata` table. If a key does not
    /// already exist, create one and save it to the Delta Lake. If a key could not be retrieved
    /// or created, return [`ModelarDbManagerError`](crate::error::ModelarDbManagerError).
    pub async fn manager_key(&self) -> Result<Uuid> {
        let sql = "SELECT key FROM manager_metadata";
        let batch = sql_and_concat(&self.session_context, sql).await?;

        let keys = modelardb_types::array!(batch, 0, StringArray);
        if keys.is_empty() {
            let manager_key = Uuid::new_v4();

            // Add a new row to the manager_metadata table to persist the key.
            self.delta_lake
                .write_columns_to_metadata_table(
                    "manager_metadata",
                    vec![Arc::new(StringArray::from(vec![manager_key.to_string()]))],
                )
                .await?;

            Ok(manager_key)
        } else {
            let manager_key: String = keys.value(0).to_owned();

            Ok(manager_key
                .parse()
                .map_err(|error: uuid::Error| DeltaTableError::Generic(error.to_string()))?)
        }
    }

    /// Save the node to the metadata Delta Lake and return [`Ok`]. If the node could not be saved,
    /// return [`ModelarDbManagerError`](crate::error::ModelarDbManagerError).
    pub async fn save_node(&self, node: Node) -> Result<()> {
        self.delta_lake
            .write_columns_to_metadata_table(
                "nodes",
                vec![
                    Arc::new(StringArray::from(vec![node.url])),
                    Arc::new(StringArray::from(vec![node.mode.to_string()])),
                ],
            )
            .await?;

        Ok(())
    }

    /// Remove the row in the `nodes` table that corresponds to the node with `url` and return
    /// [`Ok`]. If the row could not be removed, return
    /// [`ModelarDbManagerError`](crate::error::ModelarDbManagerError).
    pub async fn remove_node(&self, url: &str) -> Result<()> {
        let delta_ops = self.delta_lake.metadata_delta_ops("nodes").await?;

        delta_ops
            .delete()
            .with_predicate(col("url").eq(lit(url)))
            .await?;

        Ok(())
    }

    /// Return the nodes currently controlled by the manager that have been persisted to the
    /// metadata Delta Lake. If the nodes could not be retrieved,
    /// [`ModelarDbManagerError`](crate::error::ModelarDbManagerError) is returned.
    pub async fn nodes(&self) -> Result<Vec<Node>> {
        let mut nodes: Vec<Node> = vec![];

        let sql = "SELECT url, mode FROM nodes";
        let batch = sql_and_concat(&self.session_context, sql).await?;

        let url_array = modelardb_types::array!(batch, 0, StringArray);
        let mode_array = modelardb_types::array!(batch, 1, StringArray);

        for row_index in 0..batch.num_rows() {
            let url = url_array.value(row_index).to_owned();
            let mode = mode_array.value(row_index).to_owned();

            let server_mode = ServerMode::from_str(&mode)
                .map_err(|error| DeltaTableError::Generic(error.to_string()))?;

            nodes.push(Node::new(url, server_mode));
        }

        Ok(nodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    // Tests for MetadataManager.
    #[tokio::test]
    async fn test_create_manager_metadata_delta_lake_tables() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        // Verify that the tables were created, registered, and has the expected columns.
        assert!(
            metadata_manager
                .session_context
                .sql("SELECT key FROM manager_metadata")
                .await
                .is_ok()
        );

        assert!(
            metadata_manager
                .session_context
                .sql("SELECT url, mode FROM nodes")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_new_manager_key() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        // Verify that the manager key is created and saved correctly.
        let manager_key = metadata_manager.manager_key().await.unwrap();

        let sql = "SELECT key FROM manager_metadata";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![manager_key.to_string()])
        );
    }

    #[tokio::test]
    async fn test_existing_manager_key() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        // Verify that only a single key is created and saved when retrieving multiple times.
        let manager_key_1 = metadata_manager.manager_key().await.unwrap();
        let manager_key_2 = metadata_manager.manager_key().await.unwrap();

        let sql = "SELECT key FROM manager_metadata";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(manager_key_1, manager_key_2);
        assert_eq!(batch.column(0).len(), 1);
    }

    #[tokio::test]
    async fn test_save_node() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        metadata_manager.save_node(node_1.clone()).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        metadata_manager.save_node(node_2.clone()).await.unwrap();

        // Verify that the nodes are saved correctly.
        let sql = "SELECT url, mode FROM nodes";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![node_2.url.clone(), node_1.url.clone()])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec![node_2.mode.to_string(), node_1.mode.to_string()])
        );
    }

    #[tokio::test]
    async fn test_remove_node() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        metadata_manager.save_node(node_1.clone()).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        metadata_manager.save_node(node_2.clone()).await.unwrap();

        metadata_manager.remove_node(&node_1.url).await.unwrap();

        // Verify that node_1 is removed correctly.
        let sql = "SELECT url, mode FROM nodes";
        let batch = sql_and_concat(&metadata_manager.session_context, sql)
            .await
            .unwrap();

        assert_eq!(
            **batch.column(0),
            StringArray::from(vec![node_2.url.clone()])
        );
        assert_eq!(
            **batch.column(1),
            StringArray::from(vec![node_2.mode.to_string()])
        );
    }

    #[tokio::test]
    async fn test_nodes() {
        let (_temp_dir, metadata_manager) = create_metadata_manager().await;

        let node_1 = Node::new("url_1".to_string(), ServerMode::Edge);
        metadata_manager.save_node(node_1.clone()).await.unwrap();

        let node_2 = Node::new("url_2".to_string(), ServerMode::Edge);
        metadata_manager.save_node(node_2.clone()).await.unwrap();

        let nodes = metadata_manager.nodes().await.unwrap();

        assert_eq!(nodes, vec![node_2, node_1]);
    }

    async fn create_metadata_manager() -> (TempDir, MetadataManager) {
        let temp_dir = tempfile::tempdir().unwrap();

        let table_metadata_manager = TableMetadataManager::try_from_path(temp_dir.path())
            .await
            .unwrap();

        let metadata_manager = MetadataManager {
            delta_lake: DeltaLake::try_from_local_path(temp_dir.path()).unwrap(),
            table_metadata_manager,
            session_context: Arc::new(SessionContext::new()),
        };

        metadata_manager
            .create_and_register_manager_metadata_delta_lake_tables()
            .await
            .unwrap();

        (temp_dir, metadata_manager)
    }
}
