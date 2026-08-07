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

//! Support for managing all compressed data that is inserted into the
//! [`StorageEngine`](crate::storage::StorageEngine).

use std::sync::Arc;

use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use datafusion::arrow::record_batch::RecordBatch;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::data_folders::DataFolder;
use crate::error::Result;
use crate::storage::compressed_data_buffer::{CompressedDataBuffer, CompressedSegmentBatch};
use crate::storage::data_transfer::DataTransfer;
use crate::storage::types::Message;
use crate::storage::types::{Channels, MemoryPool};

/// Stores data points compressed as segments containing metadata and models in memory to batch the
/// compressed segments before saving them to Apache Parquet files.
pub(super) struct CompressedDataManager {
    /// Component that transfers saved compressed data to the remote data folder when it is necessary.
    pub(super) data_transfer: Arc<RwLock<Option<DataTransfer>>>,
    /// Folder containing all compressed data managed by the [`StorageEngine`](crate::storage::StorageEngine).
    pub(crate) local_data_folder: DataFolder,
    /// The compressed segments before they are saved to persistent storage. The key is the name of
    /// the time series table the compressed segments represents data points for so the Apache Parquet
    /// files can be partitioned by table.
    pub(super) compressed_data_buffers: DashMap<String, CompressedDataBuffer>,
    /// FIFO queue of time series table names referring to [`CompressedDataBuffers`](CompressedDataBuffer)
    /// that can be saved to persistent storage.
    compressed_queue: SegQueue<String>,
    /// Channels used by the storage engine's threads to communicate.
    channels: Arc<Channels>,
    /// Track how much memory is left for storing uncompressed and compressed data.
    memory_pool: Arc<MemoryPool>,
}

impl CompressedDataManager {
    pub(super) fn new(
        data_transfer: Arc<RwLock<Option<DataTransfer>>>,
        local_data_folder: DataFolder,
        channels: Arc<Channels>,
        memory_pool: Arc<MemoryPool>,
    ) -> Self {
        Self {
            data_transfer,
            local_data_folder,
            compressed_data_buffers: DashMap::new(),
            compressed_queue: SegQueue::new(),
            channels,
            memory_pool,
        }
    }

    /// Write `record_batch` to the normal table with `table_name` as a compressed Apache Parquet file.
    pub(super) async fn insert_record_batch(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<()> {
        debug!(
            "Received record batch with {} rows for the normal table '{}'.",
            record_batch.num_rows(),
            table_name
        );

        // Disk space use is over approximated as Apache Parquet applies lossless compression. The
        // actual size is not computed as DeltaTable seems to have no support for listing the files
        // added in a version without iterating through all of the Add actions from file_actions().
        let record_batch_size_in_bytes = record_batch.get_array_memory_size();

        self.local_data_folder
            .delta_lake
            .write_record_batches_to_normal_table(table_name, vec![record_batch])
            .await?;

        // Inform the data transfer component about the new data if a remote data folder was
        // provided. If the total size of the data related to table_name have reached the transfer
        // threshold, all of the data is transferred to the remote object store.
        if let Some(data_transfer) = &*self.data_transfer.read().await {
            data_transfer
                .increase_table_size(table_name, record_batch_size_in_bytes)
                .await?;
        }

        Ok(())
    }

    /// Read and process messages received from the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) to either insert compressed
    /// data, flush buffers, or stop.
    pub(super) fn process_compressed_messages(&self, runtime: Arc<Runtime>) -> Result<()> {
        loop {
            let message = self.channels.compressed_data_receiver.recv()?;

            match message {
                Message::Data(compressed_segment_batch) => {
                    runtime.block_on(self.insert_compressed_segments(compressed_segment_batch))?;
                }
                Message::Flush => {
                    self.flush_and_log_errors(&runtime);
                    self.channels.result_sender.send(Ok(()))?;
                }
                Message::Stop => {
                    self.flush_and_log_errors(&runtime);
                    self.channels.result_sender.send(Ok(()))?;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Insert `compressed_segment_batch` into the in-memory [`CompressedDataBuffer`] for the time
    /// series table. If inserting `compressed_segment_batch` exceeded the reserved memory limit, save
    /// compressed data to disk until enough memory is available. If compressed data could not be
    /// saved to disk, return [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    async fn insert_compressed_segments(
        &self,
        compressed_segment_batch: CompressedSegmentBatch,
    ) -> Result<()> {
        let time_series_table_name = compressed_segment_batch.time_series_table_name();
        debug!("Inserting batch into compressed data buffer for table '{time_series_table_name}'.");

        // Since the compressed segments are already in memory, insert the segments into the
        // compressed data buffer first and check if the reserved memory limit is exceeded after.
        let segments_size = if let Some(mut compressed_data_buffer) =
            self.compressed_data_buffers.get_mut(time_series_table_name)
        {
            debug!("Found existing compressed data buffer for table '{time_series_table_name}'.",);

            compressed_data_buffer
                .append_compressed_segments(compressed_segment_batch.compressed_segments)
        } else {
            // A String is created as two copies are required for compressed_data_buffer and
            // compressed_queue anyway and compressed_segments cannot be moved out of
            // compressed_segment_batch if time_series_table_name is actively being borrowed.
            let time_series_table_name = time_series_table_name.to_owned();
            debug!(
                "Creating compressed data buffer for table '{time_series_table_name}' as none exist.",
            );

            let mut compressed_data_buffer =
                CompressedDataBuffer::new(compressed_segment_batch.time_series_table_metadata);
            let segment_size = compressed_data_buffer
                .append_compressed_segments(compressed_segment_batch.compressed_segments);

            self.compressed_data_buffers
                .insert(time_series_table_name.clone(), compressed_data_buffer);
            self.compressed_queue.push(time_series_table_name);

            segment_size
        }?;

        // Update the remaining memory for compressed data. If the reserved memory limit is
        // exceeded, save compressed data to disk.
        while !self
            .memory_pool
            .try_reserve_compressed_memory(segments_size)
        {
            self.save_compressed_data_to_free_memory(segments_size)
                .await?;
        }

        Ok(())
    }

    /// Save [`CompressedDataBuffers`](CompressedDataBuffer) to disk until at least `size_in_bytes`
    /// bytes of memory is available. If all the data is saved successfully, return [`Ok`],
    /// otherwise return [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    async fn save_compressed_data_to_free_memory(&self, size_in_bytes: usize) -> Result<()> {
        debug!("Out of memory for compressed data. Saving compressed data to disk.");

        while self.memory_pool.remaining_compressed_memory_in_bytes() < size_in_bytes as isize {
            let table_name = self
                .compressed_queue
                .pop()
                .expect("Not enough compressed data to free up the required memory.");
            self.save_compressed_data(&table_name).await?;
        }
        Ok(())
    }

    /// Flush the data that the [`CompressedDataManager`] is currently managing. Writes a log
    /// message if some of the data cannot be flushed.
    fn flush_and_log_errors(&self, runtime: &Runtime) {
        runtime.block_on(async {
            if let Err(error) = self.flush().await {
                error!(
                    "Failed to flush data in compressed data manager due to: {}",
                    error
                );
            }
        });
    }

    /// Flush the data that the [`CompressedDataManager`] is currently managing. Returns an
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError) if the data cannot be flushed.
    async fn flush(&self) -> Result<()> {
        info!(
            "Flushing the remaining {} compressed data buffers.",
            self.compressed_queue.len()
        );

        while !self.compressed_queue.is_empty() {
            let table_name = self.compressed_queue.pop().unwrap();
            self.save_compressed_data(&table_name).await?;
        }

        Ok(())
    }

    /// Save the compressed data that belongs to the time series table with `table_name` The size of
    /// the saved compressed data is added back to the remaining compressed memory. If the data is
    /// written successfully to disk, return [`Ok`], otherwise return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    async fn save_compressed_data(&self, table_name: &str) -> Result<()> {
        debug!("Saving compressed segments to disk for {table_name}.");

        // unwrap() is safe as table_name is read from compressed_queue.
        let (_table_name, compressed_data_buffer) =
            self.compressed_data_buffers.remove(table_name).unwrap();

        // Disk space use is over approximated as Apache Parquet applies lossless compression. The
        // actual size is not computed as DeltaTable seems to have no support for listing the files
        // added in a version without iterating through all of the Add actions from file_actions().
        let compressed_data_buffer_size_in_bytes = compressed_data_buffer.size_in_bytes;
        let compressed_segments = compressed_data_buffer.record_batches();
        self.local_data_folder
            .delta_lake
            .write_compressed_segments_to_time_series_table(table_name, compressed_segments)
            .await?;

        // Inform the data transfer component about the new data if a remote data folder was
        // provided. If the total size of the data related to table_name have reached the transfer
        // threshold, all of the data is transferred to the remote object store.
        if let Some(data_transfer) = &*self.data_transfer.read().await {
            data_transfer
                .increase_table_size(table_name, compressed_data_buffer_size_in_bytes)
                .await?;
        }

        // Update the remaining memory for compressed data.
        self.memory_pool
            .adjust_compressed_memory(compressed_data_buffer_size_in_bytes as isize);

        debug!(
            "Saved {} bytes of compressed data to disk. Remaining reserved bytes: {}.",
            compressed_data_buffer_size_in_bytes,
            self.memory_pool.remaining_compressed_memory_in_bytes()
        );

        Ok(())
    }

    /// Change the amount of memory for compressed data in bytes according to `value_change`. If
    /// less than zero bytes remain, save compressed data to free memory. If all the data is saved
    /// successfully return [`Ok`], otherwise return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub(super) async fn adjust_compressed_remaining_memory_in_bytes(
        &self,
        value_change: isize,
    ) -> Result<()> {
        self.memory_pool.adjust_compressed_memory(value_change);
        self.save_compressed_data_to_free_memory(0).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Array, Int8Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use modelardb_test::table::{self, NORMAL_TABLE_NAME, TIME_SERIES_TABLE_NAME};
    use modelardb_test::{
        COMPRESSED_RESERVED_MEMORY_IN_BYTES, COMPRESSED_SEGMENTS_SIZE,
        INGESTED_RESERVED_MEMORY_IN_BYTES, UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
    };
    use tempfile::{self, TempDir};

    const COLUMN_INDEX: i16 = 1;

    // Tests for insert_record_batch().
    #[tokio::test]
    async fn test_insert_record_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "the_column",
            DataType::Int8,
            false,
        )]));
        let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int8Array::from(vec![37, 73]))];
        let record_batch = RecordBatch::try_new(schema, columns).unwrap();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let local_data_folder = data_manager.local_data_folder.clone();

        let mut delta_table = local_data_folder
            .delta_lake
            .create_normal_table(NORMAL_TABLE_NAME, &record_batch.schema())
            .await
            .unwrap();
        assert_eq!(delta_table.get_files_count(), 0);

        data_manager
            .insert_record_batch(NORMAL_TABLE_NAME, record_batch)
            .await
            .unwrap();
        delta_table.load().await.unwrap();
        assert_eq!(delta_table.get_files_count(), 1);
    }

    // Tests for insert_compressed_segments().
    #[tokio::test]
    async fn test_can_insert_compressed_segment_into_new_compressed_data_buffer() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let key = TIME_SERIES_TABLE_NAME;

        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        assert!(data_manager.compressed_data_buffers.contains_key(key));
        assert_eq!(data_manager.compressed_queue.pop().unwrap(), key);
        assert!(
            data_manager
                .compressed_data_buffers
                .get(key)
                .unwrap()
                .size_in_bytes
                > 0
        );
    }

    #[tokio::test]
    async fn test_can_insert_compressed_segment_into_existing_compressed_data_buffer() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        data_manager
            .insert_compressed_segments(segments.clone())
            .await
            .unwrap();
        let previous_size = data_manager
            .compressed_data_buffers
            .get(TIME_SERIES_TABLE_NAME)
            .unwrap()
            .size_in_bytes;

        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        assert!(
            data_manager
                .compressed_data_buffers
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap()
                .size_in_bytes
                > previous_size
        );
    }

    #[tokio::test]
    async fn test_save_first_compressed_data_buffer_if_out_of_memory() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let local_data_folder = data_manager.local_data_folder.clone();

        let mut delta_table = local_data_folder
            .delta_lake
            .create_time_series_table(&table::time_series_table_metadata())
            .await
            .unwrap();

        let compressed_segment_batch = compressed_segments_record_batch();
        let reserved_memory = data_manager
            .memory_pool
            .remaining_compressed_memory_in_bytes() as usize;

        // Insert compressed data into the storage engine until data is saved to Apache Parquet.
        let compressed_buffer_size = compressed_segment_batch
            .time_series_table_metadata
            .field_column_indices
            .len()
            * COMPRESSED_SEGMENTS_SIZE;
        let max_compressed_segments = reserved_memory / compressed_buffer_size;
        for _ in 0..max_compressed_segments + 1 {
            data_manager
                .insert_compressed_segments(compressed_segment_batch.clone())
                .await
                .unwrap();
        }

        // The compressed data should be saved to the table_name folder in the compressed folder.
        delta_table.load().await.unwrap();
        let parquet_files = delta_table.get_files_iter().unwrap().collect::<Vec<_>>();

        // One Apache Parquet file is created for each field column in the batch.
        assert_eq!(parquet_files.len(), 2);
    }

    #[tokio::test]
    async fn test_remaining_bytes_decremented_when_inserting_compressed_segments() {
        let segments = compressed_segments_record_batch();
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let reserved_memory = data_manager
            .memory_pool
            .remaining_compressed_memory_in_bytes();

        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        assert!(
            reserved_memory
                > data_manager
                    .memory_pool
                    .remaining_compressed_memory_in_bytes()
        );
    }

    #[tokio::test]
    async fn test_remaining_memory_incremented_when_saving_compressed_segments() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let local_data_folder = data_manager.local_data_folder.clone();

        let segments = compressed_segments_record_batch();
        local_data_folder
            .delta_lake
            .create_time_series_table(&segments.time_series_table_metadata)
            .await
            .unwrap();

        data_manager
            .insert_compressed_segments(segments.clone())
            .await
            .unwrap();

        // Set the remaining memory to a negative value since data is only saved when out of memory.
        data_manager.memory_pool.adjust_compressed_memory(
            -(data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes()
                + 1),
        );

        data_manager
            .save_compressed_data_to_free_memory(0)
            .await
            .unwrap();

        assert!(
            -1 < data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes()
        );
    }

    // Tests for adjust_compressed_remaining_memory_in_bytes().
    #[tokio::test]
    async fn test_increase_compressed_remaining_memory_in_bytes() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(10000)
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes(),
            COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize + 10000
        )
    }

    #[tokio::test]
    async fn test_decrease_compressed_remaining_memory_in_bytes() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;
        let local_data_folder = data_manager.local_data_folder.clone();

        // Insert data that should be saved when the remaining memory is decreased.
        let segments = compressed_segments_record_batch();
        local_data_folder
            .delta_lake
            .create_time_series_table(&segments.time_series_table_metadata)
            .await
            .unwrap();
        data_manager
            .insert_compressed_segments(segments)
            .await
            .unwrap();

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(
                -(COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize),
            )
            .await
            .unwrap();

        assert_eq!(
            data_manager
                .memory_pool
                .remaining_compressed_memory_in_bytes(),
            1565
        );

        // There should no longer be any compressed data in memory.
        assert_eq!(data_manager.compressed_data_buffers.len(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "Not enough compressed data to free up the required memory.")]
    async fn test_panic_if_decreasing_compressed_remaining_memory_in_bytes_below_zero() {
        let (_temp_dir, data_manager) = create_compressed_data_manager().await;

        data_manager
            .adjust_compressed_remaining_memory_in_bytes(
                -((COMPRESSED_RESERVED_MEMORY_IN_BYTES + 1) as isize),
            )
            .await
            .unwrap();
    }

    /// Create a [`CompressedDataManager`] with a folder that is deleted once the test is finished
    /// and a metadata manager with a single time series table.
    async fn create_compressed_data_manager() -> (TempDir, CompressedDataManager) {
        let temp_dir = tempfile::tempdir().unwrap();
        let channels = Arc::new(Channels::new());

        let memory_pool = Arc::new(MemoryPool::new(
            INGESTED_RESERVED_MEMORY_IN_BYTES,
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        ));

        // Create a local data folder and save a single time series table to the metadata Delta Lake.
        let temp_dir_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::try_from_local_url(temp_dir_url).await.unwrap();

        let time_series_table_metadata = table::time_series_table_metadata();
        local_data_folder
            .table_metadata_manager
            .save_time_series_table_metadata(&time_series_table_metadata)
            .await
            .unwrap();

        (
            temp_dir,
            CompressedDataManager::new(
                Arc::new(RwLock::new(None)),
                local_data_folder,
                channels,
                memory_pool,
            ),
        )
    }

    /// Return a [`CompressedSegmentBatch`] containing three compressed segments per field column.
    fn compressed_segments_record_batch() -> CompressedSegmentBatch {
        compressed_segment_batch_with_time(0, 0.0)
    }

    /// Return a [`CompressedSegmentBatch`] containing compressed segments for a time series table
    /// with two field columns. For each of the field columns the batch contains three compressed
    /// segments. The compressed segments time range is from `time_ms` to `time_ms` + 3, while the
    /// value range is from `offset` + 5.2 to `offset` + 34.2.
    fn compressed_segment_batch_with_time(time_ms: i64, offset: f32) -> CompressedSegmentBatch {
        CompressedSegmentBatch::new(
            table::time_series_table_metadata_arc(),
            vec![
                table::compressed_segments_record_batch_with_time(COLUMN_INDEX, time_ms, offset),
                table::compressed_segments_record_batch_with_time(
                    COLUMN_INDEX + 1,
                    time_ms,
                    offset,
                ),
            ],
        )
    }
}
