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

//! Implementation of a request handler for Apache Arrow Flight in the form of
//! [`FlightServiceHandler`]. An Apache Arrow Flight server that process requests
//! using [`FlightServiceHandler`] can be started with [`start_apache_arrow_flight_server()`].

use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::str;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaAsIpc,
    SchemaResult, Ticket, utils,
};
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{EmptyRecordBatchStream, SendableRecordBatchStream};
use deltalake::arrow::datatypes::Schema;
use futures::StreamExt;
use futures::stream::{self, BoxStream, SelectAll};
use modelardb_storage::parser::{self, ModelarDbStatement};
use modelardb_types::flight::protocol;
use modelardb_types::functions;
use modelardb_types::types::TimeSeriesTableMetadata;
use prost::Message;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, Sender};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};

use crate::ClusterMode;
use crate::context::Context;
use crate::error::{ModelarDbServerError, Result};

/// Start an Apache Arrow Flight server on 0.0.0.0:`port` that passes `context` to the methods that
/// process the requests through [`FlightServiceHandler`].
pub fn start_apache_arrow_flight_server(
    context: Arc<Context>,
    runtime: &Arc<Runtime>,
    port: u16,
) -> Result<()> {
    let localhost_with_port = "0.0.0.0:".to_owned() + &port.to_string();
    let localhost_with_port: SocketAddr = localhost_with_port.parse().map_err(|error| {
        ModelarDbServerError::InvalidArgument(format!(
            "Unable to parse {localhost_with_port}: {error}"
        ))
    })?;
    let handler = FlightServiceHandler::new(context);

    // Increase the maximum message size from 4 MiB to 16 MiB to allow bulk-loading larger batches.
    let flight_service_server =
        FlightServiceServer::new(handler).max_decoding_message_size(16777216);

    info!("Starting Apache Arrow Flight on {}.", localhost_with_port);
    runtime
        .block_on(async {
            Server::builder()
                .add_service(flight_service_server)
                .serve(localhost_with_port)
                .await
        })
        .map_err(|error| error.into())
}

/// Execute `sql` at `addresses` and union their result with `local_sendable_record_batch_stream`.
/// Returns a [`ModelarDbServerError`] if `sql` does not contain an INCLUDE clause, if `sql` cannot
/// be executed at one of the `addresses`, or if the [`Schema`] of the result cannot be retrieved.
async fn execute_query_at_addresses_and_union(
    sql: &str,
    addresses: Vec<String>,
    local_sendable_record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
    // Remove INCLUDE address+ as sqlparser seems incapable of doing so.
    let no_addresses_error_fn =
        || ModelarDbServerError::InvalidArgument("Missing INCLUDE address.".to_owned());
    let last_address = addresses.last().ok_or_else(no_addresses_error_fn)?;
    let start_of_address = sql.rfind(last_address).ok_or_else(no_addresses_error_fn)?;
    let end_of_address = start_of_address + last_address.len() + 1;
    let sql_select = &sql[end_of_address..];

    // Execute queries at all addresses and union all of the result streams.
    let mut unioned_sendable_record_batch_streams = SelectAll::new();
    let schema = local_sendable_record_batch_stream.schema();
    unioned_sendable_record_batch_streams.push(local_sendable_record_batch_stream);

    for address in addresses {
        let remote_sendable_record_batch_stream =
            execute_query_at_address(sql_select.to_owned(), address.to_owned()).await?;
        unioned_sendable_record_batch_streams.push(remote_sendable_record_batch_stream);
    }

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        unioned_sendable_record_batch_streams,
    )))
}

/// Execute `sql` at `address`. Returns a [`ModelarDbServerError`] if `sql` cannot be executed at
/// `address` or if the [`Schema`] of the result cannot be retrieved.
async fn execute_query_at_address(
    sql: String,
    address: String,
) -> Result<Pin<Box<dyn RecordBatchStream + Send>>> {
    // Connect and execute query.
    let mut flight_client = FlightServiceClient::connect(address.clone())
        .await
        .map_err(error_to_status_invalid_argument)?;

    let ticket = Ticket { ticket: sql.into() };
    let mut stream = flight_client.do_get(ticket).await?.into_inner();

    // Read schema of record batches.
    let flight_data = stream.message().await?.ok_or_else(|| {
        ModelarDbServerError::InvalidState(format!("Failed to retrieve schema from {address}."))
    })?;
    let schema = Arc::new(Schema::try_from(&flight_data)?);

    // Create a copy of the Arc so the schema can be used after the closure.
    let schema_for_stream = schema.clone();

    // Create single dictionaries_by_id that can be reused for conversion,
    // FlightServiceHandler.dictionaries_by_id is not used to simplify lifetimes.
    let dictionaries_by_id = HashMap::new();

    // Create SendableRecordBatchStream.
    let record_batch_stream = stream.map(move |maybe_flight_data| {
        let flight_data = match maybe_flight_data {
            Ok(flight_data) => flight_data,
            Err(error) => return Err(DataFusionError::External(Box::new(error))),
        };

        let maybe_record_batch =
            utils::flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_id);

        maybe_record_batch.map_err(|error| error.into())
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema_for_stream,
        record_batch_stream,
    )))
}

/// Read [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) from `query_result_stream`
/// and send them one at a time to [`FlightService`] using `sender`. Returns [`Status`] with the
/// code [`tonic::Code::Internal`] if the result cannot be sent through `sender`.
async fn send_query_result(
    mut query_result_stream: SendableRecordBatchStream,
    sender: Sender<StdResult<FlightData, Status>>,
) -> StdResult<(), Status> {
    // Serialize and send the schema.
    let schema = query_result_stream.schema();
    let options = IpcWriteOptions::default();
    let schema_as_flight_data = SchemaAsIpc::new(&schema, &options).into();
    send_flight_data(&sender, Ok(schema_as_flight_data)).await?;

    // Serialize and send the query result.
    let data_generator = IpcDataGenerator::default();
    let writer_options = IpcWriteOptions::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);

    while let Some(maybe_record_batch) = query_result_stream.next().await {
        // If a record batch is not returned the client is informed about the error.
        let record_batch = match maybe_record_batch {
            Ok(record_batch) => record_batch,
            Err(error) => {
                let status = Status::invalid_argument(error.to_string());
                return send_flight_data(&sender, Err(status)).await;
            }
        };

        // unwrap() is safe as the result is produced by Apache DataFusion.
        let (encoded_dictionaries, encoded_batch) = data_generator
            .encoded_batch(&record_batch, &mut dictionary_tracker, &writer_options)
            .unwrap();

        for encoded_dictionary in encoded_dictionaries {
            send_flight_data(&sender, Ok(encoded_dictionary.into())).await?;
        }
        send_flight_data(&sender, Ok(encoded_batch.into())).await?;
    }

    Ok(())
}

/// Send `flight_data_or_error` to [`FlightService`] using `sender`. Returns [`Status`] with the
/// code [`tonic::Code::Internal`] if the result cannot be send through `sender`.
async fn send_flight_data(
    sender: &Sender<StdResult<FlightData, Status>>,
    flight_data_or_error: StdResult<FlightData, Status>,
) -> StdResult<(), Status> {
    sender
        .send(flight_data_or_error)
        .await
        .map_err(error_to_status_internal)
}

/// Convert `flight_data` to a [`RecordBatch`]. If `schema` does not match the data or `flight_data`
/// could not be converted, [`Status`] is returned.
pub fn flight_data_to_record_batch(
    flight_data: &FlightData,
    schema: &Arc<Schema>,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
) -> StdResult<RecordBatch, Status> {
    debug_assert_eq!(flight_data.flight_descriptor, None);

    utils::flight_data_to_arrow_batch(flight_data, schema.clone(), dictionaries_by_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

/// Return the table stored as the first element in [`FlightDescriptor.path`], otherwise a
/// [`Status`] that specifies that the table name is missing.
pub fn table_name_from_flight_descriptor(
    flight_descriptor: &FlightDescriptor,
) -> StdResult<&String, Status> {
    flight_descriptor
        .path
        .first()
        .ok_or_else(|| Status::invalid_argument("No table name in FlightDescriptor.path."))
}

/// Return an empty stream of [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) that
/// can be returned when a SQL command has been successfully executed but did not produce any rows
/// to return.
fn empty_record_batch_stream() -> SendableRecordBatchStream {
    Box::pin(EmptyRecordBatchStream::new(Arc::new(Schema::empty())))
}

/// Convert an `error` to a [`Status`] with [`tonic::Code::InvalidArgument`] as the code and `error`
/// converted to a [`String`] as the message.
pub fn error_to_status_invalid_argument(error: impl Error) -> Status {
    Status::invalid_argument(error.to_string())
}

/// Convert an `error` to a [`Status`] with [`tonic::Code::Internal`] as the code and `error`
/// converted to a [`String`] as the message.
pub fn error_to_status_internal(error: impl Error) -> Status {
    Status::internal(error.to_string())
}

/// Handler for processing Apache Arrow Flight requests.
/// [`FlightServiceHandler`] is based on the [Apache Arrow Flight examples]
/// published under Apache2.
///
/// [Apache Arrow Flight examples]: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples
struct FlightServiceHandler {
    /// Singleton that provides access to the system's components.
    context: Arc<Context>,
    /// Pre-allocated static argument for
    /// [`flight_data_to_arrow_batch`](arrow_flight::utils::flight_data_to_arrow_batch).
    /// For more information about the use of dictionaries in Apache Arrow see
    /// the [Arrow Columnar Format].
    ///
    /// [Arrow Columnar Format]: https://arrow.apache.org/docs/format/Columnar.html
    dictionaries_by_id: HashMap<i64, ArrayRef>,
}

impl FlightServiceHandler {
    pub fn new(context: Arc<Context>) -> FlightServiceHandler {
        Self {
            context,
            dictionaries_by_id: HashMap::new(),
        }
    }

    /// While there is still more data to receive, ingest the data into the normal table.
    async fn ingest_into_normal_table(
        &self,
        table_name: &str,
        schema: &Arc<Schema>,
        flight_data_stream: &mut Streaming<FlightData>,
    ) -> StdResult<(), Status> {
        // Retrieve the data until the request does not contain any more data.
        while let Some(flight_data) = flight_data_stream.next().await {
            let record_batch =
                flight_data_to_record_batch(&flight_data?, schema, &self.dictionaries_by_id)?;
            let storage_engine = self.context.storage_engine.write().await;

            // Write record_batch to the normal table with table_name as a compressed Apache Parquet file.
            storage_engine
                .insert_record_batch(table_name, record_batch)
                .await
                .map_err(|error| {
                    Status::internal(format!(
                        "Data could not be ingested into {table_name}: {error}"
                    ))
                })?;
        }

        Ok(())
    }

    /// While there is still more data to receive, ingest the data into the storage engine.
    async fn ingest_into_time_series_table(
        &self,
        time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
        flight_data_stream: &mut Streaming<FlightData>,
    ) -> StdResult<(), Status> {
        // Retrieve the data until the request does not contain any more data.
        while let Some(flight_data) = flight_data_stream.next().await {
            let data_points = flight_data_to_record_batch(
                &flight_data?,
                &time_series_table_metadata.schema,
                &self.dictionaries_by_id,
            )?;
            let mut storage_engine = self.context.storage_engine.write().await;

            // Note that the storage engine returns when the data is stored in memory, which means
            // the data could be lost if the system crashes right after ingesting the data.
            storage_engine
                .insert_data_points(time_series_table_metadata.clone(), data_points)
                .await
                .map_err(|error| {
                    Status::internal(format!("Data could not be ingested: {error}"))
                })?;
        }

        Ok(())
    }

    /// If the server was started with a manager, validate the request by checking that the key in
    /// the request metadata matches the key of the manager. If the request is invalid, return a
    /// [`Status`] with the code [`tonic::Code::Unauthenticated`].
    async fn validate_request(&self, request_metadata: &MetadataMap) -> StdResult<(), Status> {
        let configuration_manager = self.context.configuration_manager.read().await;

        if let ClusterMode::MultiNode(manager) = &configuration_manager.cluster_mode {
            manager
                .validate_request(request_metadata)
                .map_err(|error| Status::unauthenticated(error.to_string()))
        } else {
            Ok(())
        }
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceHandler {
    type HandshakeStream = BoxStream<'static, StdResult<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, StdResult<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, StdResult<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, StdResult<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, StdResult<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, StdResult<FlightResult, Status>>;
    type ListActionsStream = BoxStream<'static, StdResult<ActionType, Status>>;

    /// Not implemented.
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> StdResult<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Provide the name of all tables in the catalog.
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> StdResult<Response<Self::ListFlightsStream>, Status> {
        let table_names = self
            .context
            .default_database_schema()
            .map_err(error_to_status_internal)?
            .table_names();
        let flight_descriptor = FlightDescriptor::new_path(table_names);
        let flight_info = FlightInfo::new().with_descriptor(flight_descriptor);

        let output = stream::once(async { Ok(flight_info) });
        Ok(Response::new(Box::pin(output)))
    }

    /// Not implemented.
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> StdResult<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Not implemented.
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> StdResult<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Provide the schema of a table in the catalog. The name of the table must be provided as the
    /// first element in `FlightDescriptor.path`.
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> StdResult<Response<SchemaResult>, Status> {
        let flight_descriptor = request.into_inner();
        let table_name = table_name_from_flight_descriptor(&flight_descriptor)?;
        let schema = self
            .context
            .schema_of_table_in_default_database_schema(table_name)
            .await
            .map_err(error_to_status_invalid_argument)?;

        let options = IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_result = schema_as_ipc.try_into().map_err(error_to_status_internal)?;
        Ok(Response::new(schema_result))
    }

    /// Execute a SQL statement provided in UTF-8 and return the schema of the result followed by
    /// the result itself. Currently, CREATE TABLE, CREATE TIME SERIES TABLE, EXPLAIN, INCLUDE,
    /// SELECT, INSERT, TRUNCATE TABLE, DROP TABLE, and VACUUM are supported.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> StdResult<Response<Self::DoGetStream>, Status> {
        let ticket = request.get_ref();

        // Extract the query.
        let sql = str::from_utf8(&ticket.ticket)
            .map_err(error_to_status_invalid_argument)?
            .to_owned();

        // Parse the statement.
        info!("Received SQL: '{}'.", sql);

        let modelardb_statement = parser::tokenize_and_parse_sql_statement(&sql)
            .map_err(error_to_status_invalid_argument)?;

        // Execute the query.
        info!("Executing SQL: '{}'.", sql);

        let sendable_record_batch_stream = match modelardb_statement {
            ModelarDbStatement::CreateNormalTable { name, schema } => {
                self.validate_request(request.metadata()).await?;

                self.context
                    .create_normal_table(&name, &schema)
                    .await
                    .map_err(error_to_status_invalid_argument)?;

                Ok(empty_record_batch_stream())
            }
            ModelarDbStatement::CreateTimeSeriesTable(time_series_table_metadata) => {
                self.validate_request(request.metadata()).await?;

                self.context
                    .create_time_series_table(&time_series_table_metadata)
                    .await
                    .map_err(error_to_status_invalid_argument)?;

                Ok(empty_record_batch_stream())
            }
            ModelarDbStatement::Statement(statement) => {
                modelardb_storage::execute_statement(&self.context.session_context, statement)
                    .await
                    .map_err(|error| error.into())
            }
            ModelarDbStatement::IncludeSelect(statement, addresses) => {
                let local_sendable_record_batch_stream =
                    modelardb_storage::execute_statement(&self.context.session_context, statement)
                        .await
                        .map_err(error_to_status_internal)?;

                execute_query_at_addresses_and_union(
                    &sql,
                    addresses,
                    local_sendable_record_batch_stream,
                )
                .await
            }
            ModelarDbStatement::TruncateTable(table_names) => {
                for table_name in table_names {
                    self.context
                        .truncate_table(&table_name)
                        .await
                        .map_err(error_to_status_invalid_argument)?;
                }

                Ok(empty_record_batch_stream())
            }
            ModelarDbStatement::DropTable(table_names) => {
                self.validate_request(request.metadata()).await?;

                for table_name in table_names {
                    self.context
                        .drop_table(&table_name)
                        .await
                        .map_err(error_to_status_invalid_argument)?;
                }

                Ok(empty_record_batch_stream())
            }
            ModelarDbStatement::Vacuum(mut table_names) => {
                // Vacuum all tables if no table names are provided.
                if table_names.is_empty() {
                    table_names = self
                        .context
                        .default_database_schema()
                        .map_err(error_to_status_internal)?
                        .table_names();
                };

                for table_name in table_names {
                    self.context
                        .vacuum_table(&table_name)
                        .await
                        .map_err(error_to_status_invalid_argument)?;
                }

                Ok(empty_record_batch_stream())
            }
        }
        .map_err(error_to_status_internal)?;

        // Send the result using a channel, a channel is needed as sync is not implemented for
        // SendableRecordBatchStream. A buffer size of two is used based on Apache DataFusion.
        let (sender, receiver) = mpsc::channel(2);

        task::spawn(async move {
            // Errors cannot be sent to the client if there is an error with the channel, if such an
            // error occurs it is logged using error!(). Simply calling await! on the JoinHandle
            // returned by task::spawn is also not an option as it waits until send_query_result()
            // returns and thus creates a deadlock since the results are never read from receiver.
            if let Err(error) = send_query_result(sendable_record_batch_stream, sender).await {
                error!("Failed to send the result for '{}' due to: {}.", sql, error);
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    /// Insert data points into a table. The name of the table must be provided as the first element
    /// of `FlightDescriptor.path` and the schema of the data points must match the schema of the
    /// table. If the data points are all inserted an empty stream is returned as confirmation,
    /// otherwise, a `Status` specifying what error occurred is returned.
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> StdResult<Response<Self::DoPutStream>, Status> {
        let mut flight_data_stream = request.into_inner();

        // Extract the table name and schema.
        let flight_data = flight_data_stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Missing FlightData."))??;

        let flight_descriptor = flight_data
            .flight_descriptor
            .ok_or_else(|| Status::invalid_argument("Missing FlightDescriptor."))?;
        let table_name = table_name_from_flight_descriptor(&flight_descriptor)?;
        let normalized_table_name = functions::normalize_name(table_name);

        // Handle the data based on whether it is a normal table or a time series table.
        if let Some(time_series_table_metadata) = self
            .context
            .time_series_table_metadata_from_default_database_schema(&normalized_table_name)
            .await
            .map_err(error_to_status_invalid_argument)?
        {
            debug!(
                "Writing data to time series table '{}'.",
                normalized_table_name
            );
            self.ingest_into_time_series_table(time_series_table_metadata, &mut flight_data_stream)
                .await?;
        } else {
            debug!("Writing data to normal table '{}'.", normalized_table_name);
            let schema = self
                .context
                .schema_of_table_in_default_database_schema(&normalized_table_name)
                .await
                .map_err(error_to_status_invalid_argument)?;
            self.ingest_into_normal_table(&normalized_table_name, &schema, &mut flight_data_stream)
                .await?;
        }

        // Confirm the data was received.
        Ok(Response::new(Box::pin(stream::empty())))
    }

    /// Not implemented.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> StdResult<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented."))
    }

    /// Perform a specific action based on the type of the action in `request`. Currently, the
    /// following actions are supported:
    /// * `CreateTables`: Create the tables given in the [`TableMetadata`](protocol::TableMetadata)
    /// protobuf message in the action body.
    /// * `FlushMemory`: Flush all data that is currently in memory to disk. This compresses the
    /// uncompressed data currently in memory and then flushes all compressed data in the storage
    /// engine to disk.
    /// * `FlushNode`: An extension of the `FlushMemory` action that first flushes all data that is
    /// currently in memory to disk and then flushes all compressed data on disk to the remote
    /// object store. Note that data is only transferred to the remote object store if one was
    /// provided when starting the server.
    /// * `KillNode`: An extension of the `FlushNode` action that first flushes all data to disk,
    /// then flushes all compressed data to the remote object store, and finally kills the process
    /// that is running the server. Note that since the process is killed, a conventional response
    /// cannot be returned.
    /// * `GetConfiguration`: Get the current server configuration. The value of each setting in the
    /// configuration is returned in a [`Configuration`](protocol::Configuration) protobuf message.
    /// * `UpdateConfiguration`: Update a single setting in the configuration. The setting to update
    /// and the new value are provided in the [`UpdateConfiguration`](protocol::UpdateConfiguration)
    /// protobuf message in the action body.
    /// * `NodeType`: Get the type of the node. The type is always `server`. The type of the node
    /// is returned as a string.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> StdResult<Response<Self::DoActionStream>, Status> {
        let action = request.get_ref();
        info!("Received request to perform action '{}'.", action.r#type);

        if action.r#type == "CreateTables" {
            self.validate_request(request.metadata()).await?;

            self.context
                .create_tables_from_bytes(action.body.clone().into())
                .await
                .map_err(error_to_status_invalid_argument)?;

            // Confirm the tables were created.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "FlushMemory" {
            self.context
                .storage_engine
                .write()
                .await
                .flush()
                .await
                .map_err(error_to_status_internal)?;

            // Confirm the data was flushed.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "FlushNode" {
            let mut storage_engine = self.context.storage_engine.write().await;
            storage_engine
                .flush()
                .await
                .map_err(error_to_status_internal)?;
            storage_engine
                .transfer()
                .await
                .map_err(error_to_status_internal)?;

            // Confirm the data was flushed.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "KillNode" {
            self.validate_request(request.metadata()).await?;

            let mut storage_engine = self.context.storage_engine.write().await;
            storage_engine
                .flush()
                .await
                .map_err(error_to_status_internal)?;
            storage_engine
                .transfer()
                .await
                .map_err(error_to_status_internal)?;

            // Since the process is killed, a conventional response cannot be given. If the action
            // returns a "Stream removed" message, the edge was successfully flushed and killed.
            std::process::exit(0);
        } else if action.r#type == "GetConfiguration" {
            // Extract the configuration data from the configuration manager.
            let configuration_manager = self.context.configuration_manager.read().await;
            let protobuf_bytes = configuration_manager.encode_and_serialize();

            // Return the configuration as an encoded and serialized protobuf message.
            Ok(Response::new(Box::pin(stream::once(async {
                Ok(FlightResult {
                    body: protobuf_bytes.into(),
                })
            }))))
        } else if action.r#type == "UpdateConfiguration" {
            let update_configuration = protocol::UpdateConfiguration::decode(action.body.clone())
                .map_err(error_to_status_internal)?;

            let setting = update_configuration.setting;
            let new_value = update_configuration
                .new_value
                .map(|new_value| new_value as usize);

            let mut configuration_manager = self.context.configuration_manager.write().await;
            let storage_engine = self.context.storage_engine.clone();

            let invalid_null_error =
                Status::invalid_argument(format!("New value for {setting} cannot be null."));

            match protocol::update_configuration::Setting::try_from(setting) {
                Ok(protocol::update_configuration::Setting::MultivariateReservedMemoryInBytes) => {
                    let new_value = new_value.ok_or(invalid_null_error)?;

                    configuration_manager
                        .set_multivariate_reserved_memory_in_bytes(new_value, storage_engine)
                        .await;

                    Ok(())
                }
                Ok(protocol::update_configuration::Setting::UncompressedReservedMemoryInBytes) => {
                    let new_value = new_value.ok_or(invalid_null_error)?;

                    configuration_manager
                        .set_uncompressed_reserved_memory_in_bytes(new_value, storage_engine)
                        .await
                        .map_err(error_to_status_internal)
                }
                Ok(protocol::update_configuration::Setting::CompressedReservedMemoryInBytes) => {
                    let new_value = new_value.ok_or(invalid_null_error)?;

                    configuration_manager
                        .set_compressed_reserved_memory_in_bytes(new_value, storage_engine)
                        .await
                        .map_err(error_to_status_internal)
                }
                Ok(protocol::update_configuration::Setting::TransferBatchSizeInBytes) => {
                    configuration_manager
                        .set_transfer_batch_size_in_bytes(new_value, storage_engine)
                        .await
                        .map_err(error_to_status_internal)
                }
                Ok(protocol::update_configuration::Setting::TransferTimeInSeconds) => {
                    configuration_manager
                        .set_transfer_time_in_seconds(new_value, storage_engine)
                        .await
                        .map_err(error_to_status_internal)
                }
                Ok(protocol::update_configuration::Setting::RetentionPeriodInSeconds) => {
                    let new_value = new_value.ok_or(invalid_null_error)?;

                    configuration_manager.set_retention_period_in_seconds(new_value);

                    Ok(())
                }
                _ => Err(Status::unimplemented(format!(
                    "{setting} is not an updatable setting in the server configuration."
                ))),
            }?;

            // Confirm the configuration was updated.
            Ok(Response::new(Box::pin(stream::empty())))
        } else if action.r#type == "NodeType" {
            let flight_result = FlightResult {
                body: "server".bytes().collect(),
            };

            Ok(Response::new(Box::pin(stream::once(async {
                Ok(flight_result)
            }))))
        } else {
            Err(Status::unimplemented("Action not implemented."))
        }
    }

    /// Return all available actions, including both a name and a description for each action.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> StdResult<Response<Self::ListActionsStream>, Status> {
        let create_tables_action = ActionType {
            r#type: "CreateTables".to_owned(),
            description: "Create the tables given in the protobuf message in the action body."
                .to_owned(),
        };

        let flush_memory_action = ActionType {
            r#type: "FlushMemory".to_owned(),
            description: "Flush the uncompressed data to disk by compressing and saving the data."
                .to_owned(),
        };

        let flush_node_action = ActionType {
            r#type: "FlushNode".to_owned(),
            description: "Flush uncompressed data to disk by compressing and saving the data and \
                          transfer all compressed data to the remote object store."
                .to_owned(),
        };

        let kill_node_action = ActionType {
            r#type: "KillNode".to_owned(),
            description: "Flush uncompressed data to disk by compressing and saving the data, \
                          transfer all compressed data to the remote object store, and kill the \
                          process running the server."
                .to_owned(),
        };

        let get_configuration_action = ActionType {
            r#type: "GetConfiguration".to_owned(),
            description: "Get the current server configuration.".to_owned(),
        };

        let update_configuration_action = ActionType {
            r#type: "UpdateConfiguration".to_owned(),
            description: "Update a specific setting in the server configuration.".to_owned(),
        };

        let node_type_action = ActionType {
            r#type: "NodeType".to_owned(),
            description: "Get the type of the node.".to_owned(),
        };

        let output = stream::iter(vec![
            Ok(create_tables_action),
            Ok(flush_memory_action),
            Ok(flush_node_action),
            Ok(kill_node_action),
            Ok(get_configuration_action),
            Ok(update_configuration_action),
            Ok(node_type_action),
        ]);

        Ok(Response::new(Box::pin(output)))
    }
}
