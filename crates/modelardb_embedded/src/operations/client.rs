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

//! Operations for interacting with ModelarDB Apache Arrow Flight servers.

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::str;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::convert;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Criteria, FlightDescriptor, Ticket};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt, stream};
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::error::{ModelarDbEmbeddedError, Result};
use crate::operations::{
    Operations, generate_read_time_series_table_sql, try_new_time_series_table_metadata,
};
use crate::{Aggregate, TableType};

/// Types of nodes that can be connected to by [`Client`].
#[derive(Clone)]
pub enum Node {
    /// The Apache Arrow Flight server URL of a ModelarDB server node.
    Server(String),
    /// The Apache Arrow Flight server URL of a ModelarDB manager node.
    Manager(String),
}

impl Node {
    /// Returns the URL of the node.
    pub fn url(&self) -> &str {
        match self {
            Node::Server(url) => url,
            Node::Manager(url) => url,
        }
    }

    /// Returns the type of the node.
    pub fn node_type(&self) -> &str {
        match self {
            Node::Server(_) => "server",
            Node::Manager(_) => "manager",
        }
    }
}

/// Client for connecting to ModelarDB Apache Arrow Flight servers.
#[derive(Clone)]
pub struct Client {
    /// The node that the client is connected to.
    pub(crate) node: Node,
    /// Apache Arrow Flight client connected to the Apache Arrow Flight server of the ModelarDB node.
    pub(crate) flight_client: FlightServiceClient<Channel>,
}

impl Client {
    /// Create a new [`Client`] that is connected to the node with the URL in `node`. If a
    /// connection to the node could not be established or if the actual type of the node does not
    /// match `node`, [`ModelarDbEmbeddedError`] is returned.
    pub async fn connect(node: Node) -> Result<Client> {
        // Retrieve the actual type of the node to ensure it matches the expected type.
        let mut flight_client = FlightServiceClient::connect(node.url().to_owned()).await?;

        let action = Action {
            r#type: "NodeType".to_owned(),
            body: vec![].into(),
        };

        let response = flight_client.do_action(action).await?;

        if let Some(response_message) = response.into_inner().message().await? {
            let actual_node_type = str::from_utf8(&response_message.body)?;
            let expected_node_type = node.node_type();

            if actual_node_type == expected_node_type {
                Ok(Client {
                    node,
                    flight_client,
                })
            } else {
                Err(ModelarDbEmbeddedError::InvalidArgument(format!(
                    "The actual node type '{actual_node_type}' does not match the expected node type '{expected_node_type}'."
                )))
            }
        } else {
            Err(ModelarDbEmbeddedError::from(Status::internal(
                "Could not retrieve the node type from the node.",
            )))
        }
    }

    /// Returns the client that should be used to execute the given command. If the client is
    /// connected to a server, this will be the same client. If the client is connected to a
    /// manager, this will be a cloud node in the cluster. If the cluster does not have at least one
    /// cloud node or if a cloud node could not be retrieved, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn client_for_command(&mut self, command: &str) -> Result<FlightServiceClient<Channel>> {
        match self.node {
            Node::Server(_) => Ok(self.flight_client.clone()),
            Node::Manager(_) => {
                let request = FlightDescriptor::new_cmd(command.to_owned());
                let flight_info = self.flight_client.get_flight_info(request).await?;

                // Retrieve the location in the endpoint from the returned flight info.
                if let [endpoint] = flight_info.into_inner().endpoint.as_slice() {
                    if let [location] = endpoint.location.as_slice() {
                        Ok(FlightServiceClient::connect(location.uri.clone()).await?)
                    } else {
                        Err(ModelarDbEmbeddedError::from(Status::internal(
                            "Endpoint did not contain exactly one location.",
                        )))
                    }
                } else {
                    Err(ModelarDbEmbeddedError::from(Status::internal(
                        "Flight info did not contain exactly one endpoint.",
                    )))
                }
            }
        }
    }
}

#[async_trait]
impl Operations for Client {
    /// Return `self` as [`Any`] so it can be downcast.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a table with the name in `table_name` and the information in `table_type`. If the
    /// table already exists or if the table could not be created, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn create(&mut self, table_name: &str, table_type: TableType) -> Result<()> {
        // Convert the table metadata to a protobuf message that can be sent to ModelarDB.
        let protobuf_bytes = match table_type {
            TableType::NormalTable(schema) => {
                modelardb_types::flight::encode_and_serialize_normal_table_metadata(
                    table_name, &schema,
                )?
            }
            TableType::TimeSeriesTable(schema, error_bounds, generated_columns) => {
                let time_series_table_metadata = try_new_time_series_table_metadata(
                    table_name,
                    schema.clone(),
                    error_bounds,
                    generated_columns,
                )?;

                modelardb_types::flight::encode_and_serialize_time_series_table_metadata(
                    &time_series_table_metadata,
                )?
            }
        };

        let action = Action {
            r#type: "CreateTables".to_owned(),
            body: protobuf_bytes.into(),
        };

        self.flight_client.do_action(action).await?;

        Ok(())
    }

    /// Returns the name of all the tables. If the table names could not be retrieved,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn tables(&mut self) -> Result<Vec<String>> {
        let criteria = Criteria {
            expression: vec![].into(),
        };
        let request = Request::new(criteria);
        let response = self.flight_client.list_flights(request).await?;

        if let Some(flight_info) = response.into_inner().message().await? {
            let mut table_names = vec![];
            if let Some(flight_descriptor) = flight_info.flight_descriptor {
                for table in flight_descriptor.path {
                    table_names.push(table);
                }
            }

            Ok(table_names)
        } else {
            Err(ModelarDbEmbeddedError::from(Status::internal(
                "Could not retrieve table names from the node.",
            )))
        }
    }

    /// Returns the schema of the table with the name in `table_name`. If the table does not exist,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn schema(&mut self, table_name: &str) -> Result<Schema> {
        let flight_descriptor = FlightDescriptor::new_path(vec![table_name.to_owned()]);
        let request = Request::new(flight_descriptor);

        let schema_result = self.flight_client.get_schema(request).await?.into_inner();
        Ok(convert::try_schema_from_ipc_buffer(&schema_result.schema)?)
    }

    /// Writes the data in `uncompressed_data` to the table with the table name in `table_name`. If
    /// the schema of `uncompressed_data` does not match the schema of the table, or the data could
    /// not be written to the table, [`ModelarDbEmbeddedError`] is returned.
    async fn write(&mut self, table_name: &str, uncompressed_data: RecordBatch) -> Result<()> {
        let mut write_client = self.client_for_command("WRITE").await?;

        // Include the table name in the flight descriptor.
        let flight_descriptor = FlightDescriptor::new_path(vec![table_name.to_owned()]);

        let flight_data_encoder = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(stream::iter(vec![Ok(uncompressed_data)]));

        let flight_data_stream = flight_data_encoder.map(|maybe_flight_data| {
            // unwrap() is safe since the stream is created above.
            maybe_flight_data.unwrap()
        });

        write_client.do_put(flight_data_stream).await?;

        Ok(())
    }

    /// Executes the SQL in `sql` and returns the result as a [`RecordBatchStream`]. If the SQL
    /// could not be executed, [`ModelarDbEmbeddedError`] is returned.
    async fn read(&mut self, sql: &str) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        let mut sql_client = self.client_for_command(sql).await?;

        let ticket = Ticket::new(sql.to_owned());
        let stream = sql_client.do_get(ticket).await?.into_inner();

        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            // Convert tonic::Status to FlightError.
            stream.map_err(|error| error.into()),
        );

        // The schema cannot be extracted from the record batch stream so we use an empty schema.
        // The schema can be empty since the schema is extracted from the record batches in
        // record_batch_stream_to_record_batch and not from the stream itself.
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            record_batch_stream.map_err(|error| DataFusionError::Execution(error.to_string())),
        )))
    }

    /// Executes the SQL in `sql` and writes the result to the normal table with the name in
    /// `target_table_name` in `target`. Note that if copying data to a time series table, the
    /// data is compressed again. If the SQL could not be executed or the data could not be written
    /// to the table, [`ModelarDbEmbeddedError`] is returned.
    async fn copy(
        &mut self,
        sql: &str,
        target: &mut dyn Operations,
        target_table_name: &str,
    ) -> Result<()> {
        let mut record_batch_stream = self.read(sql).await?;

        while let Some(record_batch) = record_batch_stream.next().await {
            target.write(target_table_name, record_batch?).await?;
        }

        Ok(())
    }

    /// Reads data from the time series table with the table name in `table_name` and returns it as a
    /// [`RecordBatchStream`]. The remaining parameters optionally specify which subset of the data
    /// to read. If the data could not be read from the table, [`ModelarDbEmbeddedError`] is
    /// returned.
    async fn read_time_series_table(
        &mut self,
        table_name: &str,
        columns: &[(String, Aggregate)],
        group_by: &[String],
        maybe_start_time: Option<&str>,
        maybe_end_time: Option<&str>,
        tags: HashMap<String, String>,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
        let schema = self.schema(table_name).await?;

        let sql = generate_read_time_series_table_sql(
            table_name,
            &schema,
            columns,
            group_by,
            maybe_start_time,
            maybe_end_time,
            tags,
        );

        self.read(&sql).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_time_series_table(
        &self,
        _source_table_name: &str,
        _target: &dyn Operations,
        _target_table_name: &str,
        _maybe_start_time: Option<&str>,
        _maybe_end_time: Option<&str>,
        _tags: HashMap<String, String>,
    ) -> Result<()> {
        Err(ModelarDbEmbeddedError::Unimplemented(
            "The ModelarDB client does not support copying time series tables.".to_owned(),
        ))
    }

    async fn r#move(
        &mut self,
        _source_table_name: &str,
        _target: &dyn Operations,
        _target_table_name: &str,
    ) -> Result<()> {
        Err(ModelarDbEmbeddedError::Unimplemented(
            "The ModelarDB client does not support moving tables.".to_owned(),
        ))
    }

    /// Truncate the table with the name in `table_name`. If the table could not be truncated,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn truncate(&mut self, table_name: &str) -> Result<()> {
        let ticket = Ticket::new(format!("TRUNCATE TABLE {table_name}"));
        self.flight_client.do_get(ticket).await?;

        Ok(())
    }

    /// Drop the table with the name in `table_name`. If the table could not be dropped,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn drop(&mut self, table_name: &str) -> Result<()> {
        let ticket = Ticket::new(format!("DROP TABLE {table_name}"));
        self.flight_client.do_get(ticket).await?;

        Ok(())
    }

    /// Vacuum the table with the name in `table_name`. If the table could not be vacuumed,
    /// [`ModelarDbEmbeddedError`] is returned.
    async fn vacuum(&mut self, table_name: &str) -> Result<()> {
        let ticket = Ticket::new(format!("VACUUM {table_name}"));
        self.flight_client.do_get(ticket).await?;

        Ok(())
    }
}
