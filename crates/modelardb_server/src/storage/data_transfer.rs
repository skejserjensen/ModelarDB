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

//! Support for efficiently transferring data to a remote object store. Data saved locally on disk
//! is managed here until it is of a sufficient size to be transferred efficiently.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use deltalake::arrow::array::RecordBatch;
use futures::TryStreamExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle as TaskJoinHandle;
use tracing::debug;

use crate::data_folders::DataFolder;
use crate::error::Result;

// TODO: Handle the case where a connection can not be established when transferring data.
// TODO: Handle deleting the files after the transfer is complete in a safe way to avoid
//       transferring the same data multiple times.

pub struct DataTransfer {
    /// The data folder containing all compressed data managed by the
    /// [`StorageEngine`](crate::storage::StorageEngine).
    local_data_folder: DataFolder,
    /// The data folder that the data should be transferred to.
    remote_data_folder: DataFolder,
    /// Map from table names to the current size of the table in bytes.
    table_size_in_bytes: DashMap<String, usize>,
    /// The number of bytes that are required before transferring a batch of data to the remote
    /// Delta Lake. If [`None`], data is not transferred based on batch size.
    transfer_batch_size_in_bytes: Option<usize>,
    /// Handle to the task that transfers data periodically to the remote object store. If [`None`],
    /// data is not transferred based on time.
    transfer_scheduler_handle: Option<TaskJoinHandle<()>>,
    /// Tables that have been dropped and should not be transferred to the remote data folder.
    dropped_tables: HashSet<String>,
}

impl DataTransfer {
    /// Create a new data transfer instance and initialize it with the data already in
    /// `local_data_folder_path`. If `local_data_folder_path` or a path within
    /// `local_data_folder_path` could not be read, return
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    pub async fn try_new(
        local_data_folder: DataFolder,
        remote_data_folder: DataFolder,
        transfer_batch_size_in_bytes: Option<usize>,
    ) -> Result<Self> {
        let table_names = local_data_folder
            .table_metadata_manager
            .table_names()
            .await?;

        // The size of tables is computed manually as datafusion_table_statistics() is not exact.
        let table_size_in_bytes = DashMap::with_capacity(table_names.len());
        for table_name in table_names {
            let delta_table = local_data_folder
                .delta_lake
                .delta_table(&table_name)
                .await?;

            let mut table_size_in_bytes = table_size_in_bytes.entry(table_name).or_insert(0);

            let object_store = delta_table.object_store();
            for file_path in delta_table.get_files_iter()? {
                let object_meta = object_store.head(&file_path).await?;
                *table_size_in_bytes += object_meta.size as usize;
            }
        }

        let data_transfer = Self {
            local_data_folder,
            remote_data_folder,
            table_size_in_bytes: table_size_in_bytes.clone(),
            transfer_batch_size_in_bytes,
            transfer_scheduler_handle: None,
            dropped_tables: HashSet::new(),
        };

        // Check if data should be transferred immediately.
        for table_name_size_in_bytes in table_size_in_bytes.iter() {
            let table_name = &table_name_size_in_bytes.key();
            let size_in_bytes = table_name_size_in_bytes.value();

            if transfer_batch_size_in_bytes.is_some_and(|batch_size| size_in_bytes >= &batch_size) {
                data_transfer.transfer_data(table_name).await?;
            }
        }

        Ok(data_transfer)
    }

    /// Increase the size of the table with `table_name` by `size_in_bytes`.
    pub async fn increase_table_size(&self, table_name: &str, size_in_bytes: usize) -> Result<()> {
        // entry() is not used as it would require the allocation of a new String for each lookup as
        // it must be given as a K, while get_mut() accepts the key as a &K so one K can be used.
        if !self.table_size_in_bytes.contains_key(table_name) {
            self.table_size_in_bytes.insert(table_name.to_owned(), 0);
        }
        *self.table_size_in_bytes.get_mut(table_name).unwrap() += size_in_bytes;

        // If the combined size of the files is larger than the batch size, transfer the data to the
        // remote object store. unwrap() is safe as key has just been added to the map.
        if self.transfer_batch_size_in_bytes.is_some_and(|batch_size| {
            self.table_size_in_bytes.get(table_name).unwrap().value() >= &batch_size
        }) {
            self.transfer_data(table_name).await?;
        }

        Ok(())
    }

    /// Mark the table with `table_name` as dropped. This will prevent data related to the table
    /// from being transferred to the remote data folder.
    pub(super) fn mark_table_as_dropped(&mut self, table_name: &str) {
        self.dropped_tables.insert(table_name.to_owned());
    }

    /// Remove the table with `table_name` from the tables that are marked as dropped and clear the
    /// size of the table. Return the number of bytes that were cleared.
    pub(super) fn clear_table(&mut self, table_name: &str) -> usize {
        self.dropped_tables.remove(table_name);

        // Return 0 if compressed data has not been saved for the table yet to avoid returning
        // an error when a table is dropped before any data is saved.
        self.table_size_in_bytes
            .remove(table_name)
            .map_or(0, |(_table_name, size_in_bytes)| size_in_bytes)
    }

    /// Set the transfer batch size to `new_value`. For each table that compressed data is saved
    /// for, check if the amount of data exceeds `new_value` and transfer all the data if it does.
    /// If the value is changed successfully return [`Ok`], otherwise return
    /// (`ModelarDbServerError`)[crate::error::ModelarDbServerError].
    pub(super) async fn set_transfer_batch_size_in_bytes(
        &mut self,
        new_value: Option<usize>,
    ) -> Result<()> {
        if let Some(new_batch_size) = new_value {
            self.transfer_larger_than_threshold(new_batch_size).await?;
        }

        self.transfer_batch_size_in_bytes = new_value;

        Ok(())
    }

    /// If a new transfer time is given, stop the existing task transferring data periodically,
    /// if there is one, and start a new task. If `new_value` is [`None`], the task is just stopped.
    /// `data_transfer` is needed as an argument instead of using `self` so it can be moved into the
    /// periodic task.
    pub(super) fn set_transfer_time_in_seconds(
        &mut self,
        new_value: Option<usize>,
        data_transfer: Arc<RwLock<Option<Self>>>,
    ) {
        // Stop the current task periodically transferring data if there is one.
        if let Some(task) = &self.transfer_scheduler_handle {
            task.abort();
        }

        // If a transfer time was given, create the task that periodically transfers data.
        self.transfer_scheduler_handle = if let Some(transfer_time) = new_value {
            let join_handle = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(transfer_time as u64)).await;

                    data_transfer
                        .write()
                        .await
                        .as_ref()
                        .unwrap()
                        .transfer_larger_than_threshold(0)
                        .await
                        .expect("Periodic data transfer failed.");
                }
            });

            Some(join_handle)
        } else {
            None
        };
    }

    /// Transfer all compressed files from tables currently using more than `threshold` bytes in the
    /// data folder to the remote object store. Return [`Ok`] if all files were transferred
    /// successfully, otherwise [`ModelarDbServerError`](crate::error::ModelarDbServerError). Note
    /// that if the function fails, some of the compressed files may still have been transferred.
    /// Since the data is transferred separately for each table, the function can be called again if
    /// it failed.
    pub(crate) async fn transfer_larger_than_threshold(&self, threshold: usize) -> Result<()> {
        // The clone is performed to not create a deadlock with transfer_data().
        for table_name_size_in_bytes in self.table_size_in_bytes.clone().iter() {
            let table_name = table_name_size_in_bytes.key();
            let size_in_bytes = table_name_size_in_bytes.value();

            if size_in_bytes > &threshold {
                self.transfer_data(table_name).await?;
            }
        }

        Ok(())
    }

    /// Transfer the data stored locally for the table with `table_name` to the remote object store.
    /// Once successfully transferred, the data is deleted from local storage. Return [`Ok`] if the
    /// files were transferred successfully, otherwise
    /// [`ModelarDbServerError`](crate::error::ModelarDbServerError).
    async fn transfer_data(&self, table_name: &str) -> Result<()> {
        // Check if the table has been dropped and should not be transferred.
        if self.dropped_tables.contains(table_name) {
            debug!("Table '{table_name}' has been dropped and data will not be transferred.");
            return Ok(());
        }

        // All data will be transferred so the current size in bytes is also the amount of data that
        // will be transferred. unwrap() is safe as the table contains data.
        let current_size_in_bytes = *self.table_size_in_bytes.get(table_name).unwrap().value();
        let local_delta_ops = self
            .local_data_folder
            .delta_lake
            .delta_ops(table_name)
            .await?;

        // Read the data that is currently stored for the table with table_name.
        let (_table, stream) = local_delta_ops.load().await?;
        let record_batches: Vec<RecordBatch> = stream.try_collect().await?;

        debug!("Transferring {current_size_in_bytes} bytes for the table '{table_name}'.",);

        // Write the data to the remote Delta Lake.
        if self
            .local_data_folder
            .table_metadata_manager
            .is_time_series_table(table_name)
            .await?
        {
            self.remote_data_folder
                .delta_lake
                .write_compressed_segments_to_time_series_table(table_name, record_batches)
                .await?;
        } else {
            self.remote_data_folder
                .delta_lake
                .write_record_batches_to_normal_table(table_name, record_batches)
                .await?;
        }

        // Delete the data that has been transferred to the remote Delta Lake.
        self.local_data_folder
            .delta_lake
            .truncate_table(table_name)
            .await?;

        // Remove the transferred data from the in-memory tracking of compressed files.
        *self.table_size_in_bytes.get_mut(table_name).unwrap() -= current_size_in_bytes;

        debug!("Transferred {current_size_in_bytes} bytes to the remote table '{table_name}'.");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::table::{self, NORMAL_TABLE_NAME, TIME_SERIES_TABLE_NAME};
    use tempfile::{self, TempDir};

    const EXPECTED_TIME_SERIES_TABLE_FILE_SIZE: usize = 2038;

    // Tests for data transfer component.
    #[tokio::test]
    async fn test_include_existing_files_on_start_up() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_tables().await;

        let (normal_table_files_size, time_series_table_files_size) =
            write_batches_to_tables(&local_data_folder, 1).await;
        let (_target_dir, data_transfer) = create_data_transfer_component(local_data_folder).await;

        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(NORMAL_TABLE_NAME)
                .unwrap(),
            normal_table_files_size
        );

        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            time_series_table_files_size
        );
    }

    #[tokio::test]
    async fn test_add_batch_to_new_table() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_tables().await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(local_data_folder.clone()).await;
        let (_normal_table_files_size, time_series_table_files_size) =
            write_batches_to_tables(&local_data_folder, 1).await;

        assert!(
            data_transfer
                .increase_table_size(TIME_SERIES_TABLE_NAME, time_series_table_files_size)
                .await
                .is_ok()
        );

        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            EXPECTED_TIME_SERIES_TABLE_FILE_SIZE
        );
    }

    #[tokio::test]
    async fn test_add_batch_to_existing_table() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_tables().await;

        let (_target_dir, data_transfer) =
            create_data_transfer_component(local_data_folder.clone()).await;
        let (_normal_table_files_size, time_series_table_files_size) =
            write_batches_to_tables(&local_data_folder, 1).await;

        data_transfer
            .increase_table_size(TIME_SERIES_TABLE_NAME, time_series_table_files_size)
            .await
            .unwrap();
        data_transfer
            .increase_table_size(TIME_SERIES_TABLE_NAME, time_series_table_files_size)
            .await
            .unwrap();

        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            EXPECTED_TIME_SERIES_TABLE_FILE_SIZE * 2
        );
    }

    #[tokio::test]
    async fn test_mark_table_as_dropped() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_tables().await;

        let (_target_dir, mut data_transfer) =
            create_data_transfer_component(local_data_folder.clone()).await;

        data_transfer.mark_table_as_dropped(TIME_SERIES_TABLE_NAME);
        assert!(
            data_transfer
                .dropped_tables
                .contains(TIME_SERIES_TABLE_NAME)
        );
    }

    #[tokio::test]
    async fn test_clear_table_size() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_tables().await;

        let (_target_dir, mut data_transfer) =
            create_data_transfer_component(local_data_folder.clone()).await;
        let (_normal_table_files_size, time_series_table_files_size) =
            write_batches_to_tables(&local_data_folder, 1).await;

        data_transfer
            .increase_table_size(TIME_SERIES_TABLE_NAME, time_series_table_files_size)
            .await
            .unwrap();

        data_transfer.mark_table_as_dropped(TIME_SERIES_TABLE_NAME);

        assert_eq!(
            data_transfer.clear_table(TIME_SERIES_TABLE_NAME),
            EXPECTED_TIME_SERIES_TABLE_FILE_SIZE
        );

        // The table should be removed from the in-memory tracking of compressed files and removed
        // from the dropped tables.
        assert!(
            !data_transfer
                .table_size_in_bytes
                .contains_key(TIME_SERIES_TABLE_NAME)
        );

        assert!(data_transfer.dropped_tables.is_empty());
    }

    #[tokio::test]
    async fn test_increase_transfer_batch_size_in_bytes() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_tables().await;

        write_batches_to_tables(&local_data_folder, 2).await;
        let (_, mut data_transfer) = create_data_transfer_component(local_data_folder).await;

        data_transfer
            .set_transfer_batch_size_in_bytes(Some(EXPECTED_TIME_SERIES_TABLE_FILE_SIZE * 10))
            .await
            .unwrap();

        assert_eq!(
            data_transfer.transfer_batch_size_in_bytes,
            Some(EXPECTED_TIME_SERIES_TABLE_FILE_SIZE * 10)
        );

        // Data should not have been transferred.
        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            EXPECTED_TIME_SERIES_TABLE_FILE_SIZE * 2
        );
    }

    #[tokio::test]
    async fn test_set_transfer_batch_size_in_bytes_to_none() {
        let (_temp_dir, local_data_folder) = create_local_data_folder_with_tables().await;

        write_batches_to_tables(&local_data_folder, 2).await;
        let (_, mut data_transfer) = create_data_transfer_component(local_data_folder).await;

        assert_eq!(
            data_transfer.transfer_batch_size_in_bytes,
            Some(EXPECTED_TIME_SERIES_TABLE_FILE_SIZE * 3 - 1)
        );

        data_transfer
            .set_transfer_batch_size_in_bytes(None)
            .await
            .unwrap();

        assert_eq!(data_transfer.transfer_batch_size_in_bytes, None);

        // Data should not have been transferred.
        assert_eq!(
            *data_transfer
                .table_size_in_bytes
                .get(TIME_SERIES_TABLE_NAME)
                .unwrap(),
            EXPECTED_TIME_SERIES_TABLE_FILE_SIZE * 2
        );
    }

    /// Create a [`DataFolder`] in a local [`TempDir`] and create a single normal table and a
    /// single time series table in it.
    async fn create_local_data_folder_with_tables() -> (TempDir, DataFolder) {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_url = temp_dir.path().to_str().unwrap();
        let local_data_folder = DataFolder::try_from_local_url(temp_dir_url).await.unwrap();

        // Create a normal table.
        local_data_folder
            .delta_lake
            .create_normal_table(NORMAL_TABLE_NAME, &table::normal_table_schema())
            .await
            .unwrap();

        local_data_folder
            .table_metadata_manager
            .save_normal_table_metadata(NORMAL_TABLE_NAME)
            .await
            .unwrap();

        // Create a time series table.
        let time_series_table_metadata = table::time_series_table_metadata();
        local_data_folder
            .delta_lake
            .create_time_series_table(&time_series_table_metadata)
            .await
            .unwrap();

        local_data_folder
            .table_metadata_manager
            .save_time_series_table_metadata(&time_series_table_metadata)
            .await
            .unwrap();

        (temp_dir, local_data_folder)
    }

    /// Write `batch_write_count` batches to the normal table and the time series table in the local
    /// data folder and return the total size of the files in each table.
    async fn write_batches_to_tables(
        local_data_folder: &DataFolder,
        batch_write_count: usize,
    ) -> (usize, usize) {
        for _ in 0..batch_write_count {
            // Write to the normal table.
            local_data_folder
                .delta_lake
                .write_record_batches_to_normal_table(
                    NORMAL_TABLE_NAME,
                    vec![table::normal_table_record_batch()],
                )
                .await
                .unwrap();

            // Write to the time series table.
            local_data_folder
                .delta_lake
                .write_compressed_segments_to_time_series_table(
                    TIME_SERIES_TABLE_NAME,
                    vec![table::compressed_segments_record_batch()],
                )
                .await
                .unwrap();
        }

        (
            table_files_size(local_data_folder, NORMAL_TABLE_NAME).await,
            table_files_size(local_data_folder, TIME_SERIES_TABLE_NAME).await,
        )
    }

    /// Return the total size of the files in the table with `table_name` in `local_data_folder`.
    async fn table_files_size(local_data_folder: &DataFolder, table_name: &str) -> usize {
        let delta_table = local_data_folder
            .delta_lake
            .delta_table(table_name)
            .await
            .unwrap();

        let mut files_size = 0;
        for file_path in delta_table.get_files_iter().unwrap() {
            let object_meta = delta_table.object_store().head(&file_path).await;
            files_size += object_meta.unwrap().size;
        }

        files_size as usize
    }

    /// Create a data transfer component with a target object store that is deleted once the test is finished.
    async fn create_data_transfer_component(
        local_data_folder: DataFolder,
    ) -> (TempDir, DataTransfer) {
        let target_dir = tempfile::tempdir().unwrap();
        let target_dir_url = target_dir.path().to_str().unwrap();
        let remote_data_folder = DataFolder::try_from_local_url(target_dir_url)
            .await
            .unwrap();

        // Set the transfer batch size so that data is transferred if three batches are written.
        let data_transfer = DataTransfer::try_new(
            local_data_folder,
            remote_data_folder,
            Some(EXPECTED_TIME_SERIES_TABLE_FILE_SIZE * 3 - 1),
        )
        .await
        .unwrap();

        (target_dir, data_transfer)
    }
}
