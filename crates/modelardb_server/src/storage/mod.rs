/* Copyright 2022 The ModelarDB Contributors
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

//! Ingests data points into temporary in-memory buffers that can be spilled to immutable Apache
//! Parquet files if necessary, uses the compression component to compress these buffers when they
//! are full or [`StorageEngine::flush()`] is called, stores the resulting data points compressed as
//! metadata and models in in-memory buffers to batch them before saving them to immutable Apache
//! Parquet files. The path to the Apache Parquet files containing relevant compressed data points
//! for a query can be retrieved by the query engine using
//! [`DeltaLake`](modelardb_storage::delta_lake::DeltaLake).

mod compressed_data_buffer;
mod compressed_data_manager;
pub(super) mod data_sinks; // pub(super) so it can be used in context.rs.
mod data_transfer;
mod types;
mod uncompressed_data_buffer;
mod uncompressed_data_manager;

use std::env;
use std::sync::{Arc, LazyLock};
use std::thread::{self, JoinHandle};

use datafusion::arrow::record_batch::RecordBatch;
use modelardb_types::types::TimeSeriesTableMetadata;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::error;

use crate::configuration::ConfigurationManager;
use crate::context::Context;
use crate::data_folders::DataFolders;
use crate::error::{ModelarDbServerError, Result};
use crate::storage::compressed_data_manager::CompressedDataManager;
use crate::storage::data_transfer::DataTransfer;
use crate::storage::types::{Channels, MemoryPool, Message};
use crate::storage::uncompressed_data_buffer::IngestedDataBuffer;
use crate::storage::uncompressed_data_manager::UncompressedDataManager;

/// The folder storing spilled uncompressed data buffers in the local data folder.
const UNCOMPRESSED_DATA_FOLDER: &str = "buffers";

/// The capacity of each uncompressed data buffer as the number of elements in the buffer where each
/// element is a [`Timestamp`](modelardb_types::types::Timestamp) and a
/// [`Value`](modelardb_types::types::Value). Note that the resulting size of the buffer has to be a
/// multiple of 64 bytes to avoid the actual capacity being larger than the requested due to
/// internal alignment when allocating memory for the two array builders.
pub static UNCOMPRESSED_DATA_BUFFER_CAPACITY: LazyLock<usize> = LazyLock::new(|| {
    env::var("MODELARDBD_UNCOMPRESSED_DATA_BUFFER_CAPACITY").map_or(64 * 1024, |value| {
        let parsed = value.parse::<usize>().unwrap();

        if parsed >= 64 && parsed % 64 == 0 {
            parsed
        } else {
            panic!("MODELARDBD_UNCOMPRESSED_DATA_BUFFER_CAPACITY must be a multiple of 64.")
        }
    })
});

/// Manages all uncompressed and compressed data, both while being stored in memory during ingestion
/// and when persisted to disk afterward.
pub struct StorageEngine {
    /// Manager that contains and controls all uncompressed data.
    uncompressed_data_manager: Arc<UncompressedDataManager>,
    /// Manager that contains and controls all compressed data.
    compressed_data_manager: Arc<CompressedDataManager>,
    /// Track how much memory is left for storing uncompressed and compressed data.
    memory_pool: Arc<MemoryPool>,
    /// Threads used for ingestion, compression, and writing.
    join_handles: Vec<JoinHandle<()>>,
    /// Unbounded channels used by the threads to communicate.
    channels: Arc<Channels>,
}

impl StorageEngine {
    /// Return [`StorageEngine`] that writes ingested data to `local_data_folder` and optionally
    /// transfers compressed data to `remote_data_folder` if it is given. Returns
    /// [`ModelarDbServerError`] if `remote_data_folder` is given but [`DataTransfer`] cannot not be
    /// created.
    pub(super) async fn try_new(
        runtime: Arc<Runtime>,
        data_folders: DataFolders,
        configuration_manager: &Arc<RwLock<ConfigurationManager>>,
    ) -> Result<Self> {
        // Create shared memory pool.
        let configuration_manager = configuration_manager.read().await;
        let memory_pool = Arc::new(MemoryPool::new(
            configuration_manager.multivariate_reserved_memory_in_bytes(),
            configuration_manager.uncompressed_reserved_memory_in_bytes(),
            configuration_manager.compressed_reserved_memory_in_bytes(),
        ));

        // Create threads and shared channels.
        let mut join_handles = vec![];
        let channels = Arc::new(Channels::new());

        // Create the uncompressed data manager.
        let uncompressed_data_manager = Arc::new(UncompressedDataManager::new(
            data_folders.local_data_folder.clone(),
            memory_pool.clone(),
            channels.clone(),
        ));

        {
            let runtime = runtime.clone();
            let uncompressed_data_manager = uncompressed_data_manager.clone();

            Self::start_threads(
                configuration_manager.ingestion_threads,
                "Ingestion",
                move || {
                    if let Err(error) =
                        uncompressed_data_manager.process_uncompressed_messages(runtime)
                    {
                        error!("Failed to receive uncompressed message due to: {}", error);
                    };
                },
                &mut join_handles,
            )?;
        }

        {
            let runtime = runtime.clone();
            let uncompressed_data_manager = uncompressed_data_manager.clone();

            Self::start_threads(
                configuration_manager.compression_threads,
                "Compression",
                move || {
                    if let Err(error) =
                        uncompressed_data_manager.process_compressor_messages(runtime)
                    {
                        error!("Failed to receive compressor message due to: {}", error);
                    };
                },
                &mut join_handles,
            )?;
        }

        // Create the compressed data manager.
        let data_transfer = if let Some(remote_data_folder) = data_folders.maybe_remote_data_folder
        {
            let data_transfer = DataTransfer::try_new(
                data_folders.local_data_folder.clone(),
                remote_data_folder,
                configuration_manager.transfer_batch_size_in_bytes(),
            )
            .await?;

            Some(data_transfer)
        } else {
            None
        };

        let data_transfer_is_some = data_transfer.is_some();
        let compressed_data_manager = Arc::new(CompressedDataManager::new(
            Arc::new(RwLock::new(data_transfer)),
            data_folders.local_data_folder,
            channels.clone(),
            memory_pool.clone(),
        ));

        {
            let runtime = runtime.clone();
            let compressed_data_manager = compressed_data_manager.clone();

            Self::start_threads(
                configuration_manager.writer_threads,
                "Writer",
                move || {
                    if let Err(error) = compressed_data_manager.process_compressed_messages(runtime)
                    {
                        error!("Failed to receive compressed message due to: {}", error);
                    };
                },
                &mut join_handles,
            )?;
        }

        let mut storage_engine = Self {
            uncompressed_data_manager,
            compressed_data_manager,
            memory_pool,
            join_handles,
            channels,
        };

        // Start the task that transfers data periodically if a remote data folder is given and
        // time-based data transfer is enabled.
        if data_transfer_is_some {
            storage_engine
                .set_transfer_time_in_seconds(configuration_manager.transfer_time_in_seconds())
                .await?;
        }

        Ok(storage_engine)
    }

    /// Start `num_threads` threads with `name` that executes `function` and whose [`JoinHandle`] is
    /// added to `join_handles.
    fn start_threads<F>(
        num_threads: usize,
        name: &str,
        function: F,
        join_handles: &mut Vec<JoinHandle<()>>,
    ) -> Result<()>
    where
        F: FnOnce() + Send + Clone + 'static,
    {
        for thread_number in 0..num_threads {
            let join_handle = thread::Builder::new()
                .name(format!("{name} {thread_number}"))
                .spawn(function.clone())?;

            join_handles.push(join_handle);
        }

        Ok(())
    }

    /// Add references to the
    /// [`UncompressedDataBuffers`](uncompressed_data_buffer::UncompressedDataBuffer) currently on
    /// disk to [`UncompressedDataManager`] which immediately will start compressing them.
    pub(super) async fn initialize(&self, context: &Context) -> Result<()> {
        self.uncompressed_data_manager.initialize(context).await
    }

    /// Pass `record_batch` to [`CompressedDataManager`]. Return [`Ok`] if `record_batch` was
    /// successfully written to an Apache Parquet file, otherwise return [`ModelarDbServerError`].
    pub(super) async fn insert_record_batch(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<()> {
        self.compressed_data_manager
            .insert_record_batch(table_name, record_batch)
            .await
    }

    /// Pass `data_points` to [`UncompressedDataManager`]. Return [`Ok`] if all of the data points
    /// were successfully inserted, otherwise return [`ModelarDbServerError`].
    pub(super) async fn insert_data_points(
        &mut self,
        time_series_table_metadata: Arc<TimeSeriesTableMetadata>,
        multivariate_data_points: RecordBatch,
    ) -> Result<()> {
        // TODO: write to a WAL and use it to ensure termination never duplicates or loses data.
        self.memory_pool
            .wait_for_ingested_memory(multivariate_data_points.get_array_memory_size());

        self.channels
            .ingested_data_sender
            .send(Message::Data(IngestedDataBuffer::new(
                time_series_table_metadata,
                multivariate_data_points,
            )))
            .map_err(|error| error.into())
    }

    /// Flush all the data the [`StorageEngine`] is currently storing in memory to disk. If all the
    /// data is successfully flushed to disk, return [`Ok`], otherwise return
    /// [`ModelarDbServerError`].
    pub(super) async fn flush(&self) -> Result<()> {
        self.channels.ingested_data_sender.send(Message::Flush)?;

        // Wait until all the data in the storage engine has been flushed.
        self.channels.result_receiver.recv()?
    }

    /// Transfer all the compressed data the [`StorageEngine`] is managing to the remote object store.
    pub(super) async fn transfer(&mut self) -> Result<()> {
        if let Some(data_transfer) = &*self.compressed_data_manager.data_transfer.read().await {
            data_transfer.transfer_larger_than_threshold(0).await
        } else {
            Err(ModelarDbServerError::InvalidState(
                "No remote object store available.".to_owned(),
            ))
        }
    }

    /// Flush all the data the [`StorageEngine`] is currently storing in memory to disk and stop all
    /// the threads. If all the data is successfully flushed to disk and all the threads stopped,
    /// return [`Ok`], otherwise return [`ModelarDbServerError`]. This method is purposely `&mut
    /// self` instead of `self` so it can be called through an Arc.
    pub(super) fn close(&mut self) -> Result<()> {
        self.channels.ingested_data_sender.send(Message::Stop)?;

        // Wait until all the data in the storage engine has been flushed.
        self.channels.result_receiver.recv()??;

        // unwrap() is safe as join() only returns an error if the thread panicked.
        self.join_handles
            .drain(..)
            .for_each(|join_handle: JoinHandle<()>| join_handle.join().unwrap());

        Ok(())
    }

    /// Change the amount of memory for multivariate data in bytes according to `value_change`.
    pub(super) async fn adjust_multivariate_remaining_memory_in_bytes(&self, value_change: isize) {
        self.memory_pool.adjust_ingested_memory(value_change)
    }

    /// Change the amount of memory for uncompressed data in bytes according to `value_change`.
    /// Returns [`ModelarDbServerError`] if the memory cannot be updated because a buffer cannot be
    /// spilled.
    pub(super) async fn adjust_uncompressed_remaining_memory_in_bytes(
        &self,
        value_change: isize,
    ) -> Result<()> {
        self.uncompressed_data_manager
            .adjust_uncompressed_remaining_memory_in_bytes(value_change)
            .await
    }

    /// Change the amount of memory for compressed data in bytes according to `value_change`. If the
    /// value is changed successfully return [`Ok`], otherwise return [`ModelarDbServerError`].
    pub(super) async fn adjust_compressed_remaining_memory_in_bytes(
        &self,
        value_change: isize,
    ) -> Result<()> {
        self.compressed_data_manager
            .adjust_compressed_remaining_memory_in_bytes(value_change)
            .await
    }

    /// Mark the table with `table_name` as dropped in the data transfer component. This will prevent
    /// data related to the table from being transferred to the remote data folder.
    pub(super) async fn mark_table_as_dropped(&self, table_name: &str) {
        if let Some(ref mut data_transfer) =
            *self.compressed_data_manager.data_transfer.write().await
        {
            data_transfer.mark_table_as_dropped(table_name)
        }
    }

    /// Remove the table with `table_name` from the tables that are marked as dropped and clear the
    /// size of the table in the data transfer component. Return the number of bytes that were cleared.
    pub(super) async fn clear_table(&self, table_name: &str) -> usize {
        if let Some(ref mut data_transfer) =
            *self.compressed_data_manager.data_transfer.write().await
        {
            data_transfer.clear_table(table_name)
        } else {
            0
        }
    }

    /// Set the transfer batch size in the data transfer component to `new_value` if it exists. If a
    /// data transfer component does not exist, or the value could not be changed, return
    /// [`ModelarDbServerError`].
    pub(super) async fn set_transfer_batch_size_in_bytes(
        &self,
        new_value: Option<usize>,
    ) -> Result<()> {
        if let Some(ref mut data_transfer) =
            *self.compressed_data_manager.data_transfer.write().await
        {
            data_transfer
                .set_transfer_batch_size_in_bytes(new_value)
                .await
        } else {
            Err(ModelarDbServerError::InvalidState(
                "Storage engine is not configured to transfer data.".to_owned(),
            ))
        }
    }

    /// Set the transfer time in the data transfer component to `new_value` if it exists. If a data
    /// transfer component does not exist, return [`ModelarDbServerError`].
    pub(super) async fn set_transfer_time_in_seconds(
        &mut self,
        new_value: Option<usize>,
    ) -> Result<()> {
        if let Some(ref mut data_transfer) =
            *self.compressed_data_manager.data_transfer.write().await
        {
            data_transfer.set_transfer_time_in_seconds(
                new_value,
                self.compressed_data_manager.data_transfer.clone(),
            );

            Ok(())
        } else {
            Err(ModelarDbServerError::InvalidState(
                "Storage engine is not configured to transfer data.".to_owned(),
            ))
        }
    }
}
