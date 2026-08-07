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

//! The minor types used throughout the [`StorageEngine`](crate::storage::StorageEngine).

use std::sync::Condvar;
use std::sync::Mutex;

use crossbeam_channel::{Receiver, Sender};

use crate::error::Result;
use crate::storage::compressed_data_buffer::CompressedSegmentBatch;
use crate::storage::uncompressed_data_buffer::{IngestedDataBuffer, UncompressedDataBuffer};

/// Resizeable pool of memory for tracking and limiting the amount of memory used by the
/// [`StorageEngine`](crate::storage::StorageEngine). Signed integers are used to simplify updating
/// the amount of available memory at runtime. By using signed integers for the amount of available
/// memory it can simply be decreased without checking the current value as any attempts to reserve
/// additional memory will be rejected while the amount of available memory is negative. Thus,
/// [`StorageEngine`](crate::storage::StorageEngine) will decrease its memory usage.
pub(super) struct MemoryPool {
    /// Condition variable that allows threads to wait for more ingested memory to be released.
    wait_for_ingested_memory: Condvar,
    /// How many bytes of memory that are left for storing
    /// [`RecordBatches`](datafusion::arrow::record_batch::RecordBatch) containing ingested time
    /// series with metadata.
    remaining_ingested_memory_in_bytes: Mutex<isize>,
    /// Condition variable that allows threads to wait for more uncompressed memory to be released.
    wait_for_uncompressed_memory: Condvar,
    /// How many bytes of memory that are left for storing
    /// [`UncompressedDataBuffers`](UncompressedDataBuffer) containing time series without
    /// metadata.
    remaining_uncompressed_memory_in_bytes: Mutex<isize>,
    /// How many bytes of memory that are left for storing
    /// [`CompressedDataBuffers`](crate::storage::compressed_data_buffer::CompressedDataBuffer).
    remaining_compressed_memory_in_bytes: Mutex<isize>,
}

/// The pool of memory the [`StorageEngine`](super::StorageEngine) can use for ingested data, the
/// [`UncompressedDataManager`](crate::storage::uncompressed_data_manager::UncompressedDataManager)
/// can use for uncompressed data, and the
/// [`CompressedDataManager`](crate::storage::CompressedDataManager) can use for compressed data.
impl MemoryPool {
    /// Create a new [`MemoryPool`] with at most [`i64::MAX`] bytes of memory for ingested,
    /// uncompressed, and compressed data.
    pub(super) fn new(
        ingested_memory_in_bytes: usize,
        uncompressed_memory_in_bytes: usize,
        compressed_memory_in_bytes: usize,
    ) -> Self {
        // unwrap() is safe as i64::MAX is 8192 PiB and the value is from ConfigurationManager.
        Self {
            wait_for_ingested_memory: Condvar::new(),
            remaining_ingested_memory_in_bytes: Mutex::new(
                ingested_memory_in_bytes.try_into().unwrap(),
            ),
            wait_for_uncompressed_memory: Condvar::new(),
            remaining_uncompressed_memory_in_bytes: Mutex::new(
                uncompressed_memory_in_bytes.try_into().unwrap(),
            ),
            remaining_compressed_memory_in_bytes: Mutex::new(
                compressed_memory_in_bytes.try_into().unwrap(),
            ),
        }
    }

    /// Change the amount of memory available for ingested data by `size_in_bytes`.
    pub(super) fn adjust_ingested_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_ingested_memory_in_bytes.lock().unwrap() += size_in_bytes;
        self.wait_for_ingested_memory.notify_all();
    }

    /// Return the amount of memory available for ingested data in bytes.
    #[cfg(test)]
    #[must_use]
    pub(super) fn remaining_ingested_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_ingested_memory_in_bytes.lock().unwrap()
    }

    /// Wait until `size_in_bytes` bytes of memory is available for ingested data and then reserve
    /// it.
    pub(super) fn wait_for_ingested_memory(&self, size_in_bytes: usize) {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        let mut memory_in_bytes = self.remaining_ingested_memory_in_bytes.lock().unwrap();

        while *memory_in_bytes < size_in_bytes as isize {
            // unwrap() is safe as wait() only returns an error if the mutex is poisoned.
            memory_in_bytes = self.wait_for_ingested_memory.wait(memory_in_bytes).unwrap();
        }

        *memory_in_bytes -= size_in_bytes as isize;
    }

    /// Change the amount of memory available for uncompressed data by `size_in_bytes`.
    pub(super) fn adjust_uncompressed_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.lock().unwrap() += size_in_bytes;
        self.wait_for_uncompressed_memory.notify_all();
    }

    /// Return the amount of memory available for uncompressed data in bytes.
    #[must_use]
    pub(super) fn remaining_uncompressed_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_uncompressed_memory_in_bytes.lock().unwrap()
    }

    /// Wait until `size_in_bytes` bytes of memory is available for uncompressed data or `stop_if`
    /// returns [`true`]. Note that `stop_if` is never evaluated while the thread is waiting.
    /// Returns [`true`] if the memory was reserved and [`false`] if not.
    #[must_use]
    pub(super) fn wait_for_uncompressed_memory_until<F: Fn() -> bool>(
        &self,
        size_in_bytes: usize,
        stop_if: F,
    ) -> bool {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        let mut memory_in_bytes = self.remaining_uncompressed_memory_in_bytes.lock().unwrap();

        while *memory_in_bytes < size_in_bytes as isize {
            // There is still not enough memory available, but it is no longer sensible to wait.
            if stop_if() {
                return false;
            }

            // unwrap() is safe as wait() only returns an error if the mutex is poisoned.
            memory_in_bytes = self
                .wait_for_uncompressed_memory
                .wait(memory_in_bytes)
                .unwrap();
        }

        *memory_in_bytes -= size_in_bytes as isize;
        true
    }

    /// Change the amount of memory available for storing compressed data by `size_in_bytes`.
    pub(super) fn adjust_compressed_memory(&self, size_in_bytes: isize) {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_compressed_memory_in_bytes.lock().unwrap() += size_in_bytes;
    }

    /// Return the amount of memory available for storing compressed data in bytes.
    #[must_use]
    pub(super) fn remaining_compressed_memory_in_bytes(&self) -> isize {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        *self.remaining_compressed_memory_in_bytes.lock().unwrap()
    }

    /// Try to reserve `size_in_bytes` bytes of memory for storing compressed data. Returns [`true`]
    /// if the reservation succeeds and [`false`] otherwise.
    #[must_use]
    pub(super) fn try_reserve_compressed_memory(&self, size_in_bytes: usize) -> bool {
        // unwrap() is safe as lock() only returns an error if the mutex is poisoned.
        let mut remaining_compressed_memory_in_bytes =
            self.remaining_compressed_memory_in_bytes.lock().unwrap();

        let size_in_bytes = size_in_bytes as isize;

        if size_in_bytes <= *remaining_compressed_memory_in_bytes {
            *remaining_compressed_memory_in_bytes -= size_in_bytes;
            true
        } else {
            false
        }
    }
}

/// Messages that can be sent between the components of [`StorageEngine`](super::StorageEngine).
pub(super) enum Message<T> {
    Data(T),
    Flush,
    Stop,
}

/// Channels used by the threads in the storage engine to communicate.
pub(super) struct Channels {
    /// Sender of [`IngestedDataBuffers`](IngestedDataBuffer) with data points from one or more
    /// time series from the [`StorageEngine`](super::StorageEngine) to the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) where they are partitioned by
    /// tags into buffers of static length.
    pub(super) ingested_data_sender: Sender<Message<IngestedDataBuffer>>,
    /// Receiver of [`IngestedDataBuffers`](IngestedDataBuffer) with data points from one or more
    /// time series from the [`StorageEngine`](super::StorageEngine) to the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) where they are partitioned by
    /// tags into buffers of static length.
    pub(super) ingested_data_receiver: Receiver<Message<IngestedDataBuffer>>,
    /// Sender of [`UncompressedDataBuffers`](UncompressedDataBuffer) with parts of a time series
    /// from the [`UncompressedDataManager`](super::UncompressedDataManager) to the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) where they are compressed into
    /// compressed segments.
    pub(super) uncompressed_data_sender: Sender<Message<UncompressedDataBuffer>>,
    /// Receiver of [`UncompressedDataBuffers`](UncompressedDataBuffer) with parts of a time series
    /// from the [`UncompressedDataManager`](super::UncompressedDataManager) in the
    /// [`UncompressedDataManager`](super::UncompressedDataManager) where they are compressed into
    /// compressed segments.
    pub(super) uncompressed_data_receiver: Receiver<Message<UncompressedDataBuffer>>,
    /// Sender of [`CompressedSegmentBatches`](CompressedSegmentBatch) with compressed segments from
    /// the [`UncompressedDataManager`](super::UncompressedDataManager) to the
    /// [`CompressedDataManager`](super::CompressedDataManager) where they are written to a local
    /// data folder and later, possibly, a remote data folder.
    pub(super) compressed_data_sender: Sender<Message<CompressedSegmentBatch>>,
    /// Receiver of [`CompressedSegmentBatches`](CompressedSegmentBatch) with compressed segments
    /// from the [`UncompressedDataManager`](super::UncompressedDataManager) in the
    /// [`CompressedDataManager`](super::CompressedDataManager) where they are written to a local
    /// data folder and later, possibly, a remote data folder.
    pub(super) compressed_data_receiver: Receiver<Message<CompressedSegmentBatch>>,
    /// Sender of [`Results`](Result) from
    /// [`UncompressedDataManager`](super::UncompressedDataManager) or
    /// [`CompressedDataManager`](super::CompressedDataManager) to indicate that an asynchronous
    /// process has succeeded or failed to [`StorageEngine`](super::StorageEngine).
    pub(super) result_sender: Sender<Result<()>>,
    /// Receiver of [`Results`](Result) from
    /// [`UncompressedDataManager`](super::UncompressedDataManager) or
    /// [`CompressedDataManager`](super::CompressedDataManager) to indicate that an asynchronous
    /// process has succeeded or failed to [`StorageEngine`](super::StorageEngine).
    pub(super) result_receiver: Receiver<Result<()>>,
}

impl Channels {
    pub(super) fn new() -> Self {
        let (ingested_data_sender, ingested_data_receiver) = crossbeam_channel::unbounded();
        let (uncompressed_data_sender, uncompressed_data_receiver) = crossbeam_channel::unbounded();
        let (compressed_data_sender, compressed_data_receiver) = crossbeam_channel::unbounded();
        let (result_sender, result_receiver) = crossbeam_channel::unbounded();

        Self {
            ingested_data_sender,
            ingested_data_receiver,
            uncompressed_data_sender,
            uncompressed_data_receiver,
            compressed_data_sender,
            compressed_data_receiver,
            result_sender,
            result_receiver,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::{
        COMPRESSED_RESERVED_MEMORY_IN_BYTES, COMPRESSED_SEGMENTS_SIZE,
        INGESTED_RESERVED_MEMORY_IN_BYTES, UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
    };

    // Tests for MemoryPool.
    #[test]
    fn test_adjust_multivariate_memory_increase() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            INGESTED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_ingested_memory(COMPRESSED_SEGMENTS_SIZE as isize);

        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            (INGESTED_RESERVED_MEMORY_IN_BYTES + COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_multivariate_memory_decrease_above_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            INGESTED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_ingested_memory(-(COMPRESSED_SEGMENTS_SIZE as isize));

        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            (INGESTED_RESERVED_MEMORY_IN_BYTES - COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_multivariate_memory_decrease_below_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            INGESTED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_ingested_memory(-2 * INGESTED_RESERVED_MEMORY_IN_BYTES as isize);

        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            -(INGESTED_RESERVED_MEMORY_IN_BYTES as isize)
        );
    }

    #[test]
    fn test_reserve_available_multivariate_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_ingested_memory_in_bytes(),
            INGESTED_RESERVED_MEMORY_IN_BYTES as isize
        );

        // Blocks if memory cannot be reserved, thus forcing the test to never succeed.
        memory_pool.wait_for_ingested_memory(INGESTED_RESERVED_MEMORY_IN_BYTES);

        assert_eq!(memory_pool.remaining_ingested_memory_in_bytes(), 0);
    }

    #[test]
    fn test_adjust_uncompressed_memory_increase() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_uncompressed_memory(COMPRESSED_SEGMENTS_SIZE as isize);

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            (UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES + COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_uncompressed_memory_decrease_above_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_uncompressed_memory(-(COMPRESSED_SEGMENTS_SIZE as isize));

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            (UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES - COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_uncompressed_memory_decrease_below_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_uncompressed_memory(-2 * UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize);

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            -(UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize)
        );
    }

    #[test]
    fn test_wait_for_available_uncompressed_memory_with_stop_if_false() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(
            memory_pool
                .wait_for_uncompressed_memory_until(UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES, || {
                    false
                })
        );

        assert_eq!(memory_pool.remaining_uncompressed_memory_in_bytes(), 0);
    }

    #[test]
    fn test_wait_for_available_uncompressed_memory_with_stop_if_true() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(
            memory_pool
                .wait_for_uncompressed_memory_until(UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES, || true)
        );

        assert_eq!(memory_pool.remaining_uncompressed_memory_in_bytes(), 0);
    }

    #[test]
    fn test_wait_for_unavailable_uncompressed_memory_with_stop_if_true() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(
            !memory_pool.wait_for_uncompressed_memory_until(
                2 * UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
                || true
            )
        );

        assert_eq!(
            memory_pool.remaining_uncompressed_memory_in_bytes(),
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_increase() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_compressed_memory(COMPRESSED_SEGMENTS_SIZE as isize);

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            (COMPRESSED_RESERVED_MEMORY_IN_BYTES + COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_decrease_above_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_compressed_memory(-(COMPRESSED_SEGMENTS_SIZE as isize));

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            (COMPRESSED_RESERVED_MEMORY_IN_BYTES - COMPRESSED_SEGMENTS_SIZE) as isize
        );
    }

    #[test]
    fn test_adjust_compressed_memory_decrease_below_zero() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        memory_pool.adjust_compressed_memory(-2 * COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize);

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            -(COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize)
        );
    }

    #[test]
    fn test_try_reserve_available_compressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(memory_pool.try_reserve_compressed_memory(COMPRESSED_RESERVED_MEMORY_IN_BYTES));

        assert_eq!(memory_pool.remaining_compressed_memory_in_bytes(), 0);
    }

    #[test]
    fn test_try_reserve_unavailable_compressed_memory() {
        let memory_pool = create_memory_pool();
        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );

        assert!(
            !memory_pool.try_reserve_compressed_memory(2 * COMPRESSED_RESERVED_MEMORY_IN_BYTES)
        );

        assert_eq!(
            memory_pool.remaining_compressed_memory_in_bytes(),
            COMPRESSED_RESERVED_MEMORY_IN_BYTES as isize
        );
    }

    fn create_memory_pool() -> MemoryPool {
        MemoryPool::new(
            INGESTED_RESERVED_MEMORY_IN_BYTES,
            UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
            COMPRESSED_RESERVED_MEMORY_IN_BYTES,
        )
    }
}
