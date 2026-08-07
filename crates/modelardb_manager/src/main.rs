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

//! Implementation of ModelarDB manager's main function.

mod cluster;
mod error;
mod metadata;
mod remote;

use std::sync::{Arc, LazyLock};
use std::{env, process};

use modelardb_storage::delta_lake::DeltaLake;
use modelardb_types::flight::protocol;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, MetadataValue};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::cluster::Cluster;
use crate::error::{ModelarDbManagerError, Result};
use crate::metadata::MetadataManager;
use crate::remote::start_apache_arrow_flight_server;

/// The port of the Apache Arrow Flight Server. If the environment variable is not set, 9998 is used.
pub static PORT: LazyLock<u16> =
    LazyLock::new(|| env::var("MODELARDBM_PORT").map_or(9998, |value| value.parse().unwrap()));

/// Stores the storage configuration with the remote data folder to ensure that the information
/// is consistent with the remote data folder.
pub struct RemoteDataFolder {
    /// Storage configuration encoded as a [`StorageConfiguration`](protocol::manager_metadata::StorageConfiguration)
    /// protobuf message to make it possible to transfer the configuration using Apache Arrow Flight.
    storage_configuration: protocol::manager_metadata::StorageConfiguration,
    /// Remote object store for storing data and metadata in Apache Parquet files.
    delta_lake: Arc<DeltaLake>,
    /// Manager for the access to the metadata Delta Lake.
    metadata_manager: Arc<MetadataManager>,
}

impl RemoteDataFolder {
    pub fn new(
        storage_configuration: protocol::manager_metadata::StorageConfiguration,
        delta_lake: Arc<DeltaLake>,
        metadata_manager: Arc<MetadataManager>,
    ) -> Self {
        Self {
            storage_configuration,
            delta_lake,
            metadata_manager,
        }
    }

    /// Create a [`RemoteDataFolder`] from `remote_data_folder_str`. If `remote_data_folder_str`
    /// cannot be parsed or a connection to the object store cannot be created,
    /// [`ModelarDbManagerError`] is returned.
    async fn try_new(remote_data_folder_str: &str) -> Result<Self> {
        let storage_configuration =
            modelardb_types::flight::argument_to_storage_configuration(remote_data_folder_str)?;

        let delta_lake =
            DeltaLake::try_remote_from_storage_configuration(storage_configuration.clone())?;

        let metadata_manager =
            MetadataManager::try_from_storage_configuration(storage_configuration.clone()).await?;

        Ok(Self::new(
            storage_configuration,
            Arc::new(delta_lake),
            Arc::new(metadata_manager),
        ))
    }
}

/// Provides access to the managers components.
pub struct Context {
    /// Folder for storing metadata and data in Apache Parquet files in a remote object store.
    pub remote_data_folder: RemoteDataFolder,
    /// Cluster of nodes currently controlled by the manager.
    pub cluster: RwLock<Cluster>,
    /// Key used to identify requests coming from the manager.
    pub key: MetadataValue<Ascii>,
}

/// Parse the command line arguments to extract the remote object store and start an Apache Arrow
/// Flight server. Returns [`ModelarDbManagerError`] if the command line arguments cannot be parsed,
/// if the metadata cannot be read from the Delta Lake, or if the Apache Arrow Flight server cannot
/// be started.
fn main() -> Result<()> {
    // Initialize a tracing layer that logs events to stdout.
    let stdout_log = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(stdout_log).init();

    // Create a Tokio runtime for executing asynchronous tasks.
    let runtime = Arc::new(Runtime::new()?);

    let user_arguments = collect_command_line_arguments(3);
    let user_arguments: Vec<&str> = user_arguments.iter().map(|arg| arg.as_str()).collect();
    let remote_data_folder_str = match user_arguments.as_slice() {
        &[remote_data_folder_str] => remote_data_folder_str,
        _ => print_usage_and_exit_with_error("remote_data_folder"),
    };

    let context = runtime.block_on(async {
        let remote_data_folder = RemoteDataFolder::try_new(remote_data_folder_str).await?;

        let nodes = remote_data_folder.metadata_manager.nodes().await?;

        let mut cluster = Cluster::new();
        for node in nodes {
            cluster.register_node(node)?;
        }

        // Retrieve and parse the key to a tonic metadata value since it is used in tonic requests.
        let key = remote_data_folder
            .metadata_manager
            .manager_key()
            .await?
            .to_string()
            .parse()
            .map_err(|error: InvalidMetadataValue| {
                ModelarDbManagerError::InvalidArgument(error.to_string())
            })?;

        // Create the Context.
        Ok::<Arc<Context>, ModelarDbManagerError>(Arc::new(Context {
            remote_data_folder,
            cluster: RwLock::new(cluster),
            key,
        }))
    })?;

    start_apache_arrow_flight_server(context, &runtime, *PORT)
}

/// Collect the command line arguments that this program was started with.
pub fn collect_command_line_arguments(maximum_arguments: usize) -> Vec<String> {
    let mut args = std::env::args();
    args.next(); // Skip the executable.

    // Collect at most the maximum number of command line arguments plus one. The plus one argument
    // is collected to trigger the default pattern when parsing the command line arguments with
    // pattern matching, making it possible to handle errors caused by too many arguments.
    args.by_ref().take(maximum_arguments + 1).collect()
}

/// Prints a usage message with `parameters` appended to the name of the binary executing this
/// function to stderr and exits with status code one to indicate that an error has occurred.
pub fn print_usage_and_exit_with_error(parameters: &str) -> ! {
    // The errors are consciously ignored as the program is terminating.
    let binary_path = std::env::current_exe().unwrap();
    let binary_name = binary_path.file_name().unwrap().to_str().unwrap();

    // Punctuation at the end does not seem to be common in the usage message of Unix tools.
    eprintln!("Usage: {binary_name} {parameters}");
    process::exit(1);
}
