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

//! Implementation of functions and types used throughout ModelarDB for testing purposes.

pub mod data_generation;
pub mod table;

/// Expected size of the ingested data buffer produced in the tests.
pub const INGESTED_BUFFER_SIZE: usize = 1438392;

/// Expected size of the uncompressed data buffers produced in the tests.
pub const UNCOMPRESSED_BUFFER_SIZE: usize = 1048576;

/// Expected size of the compressed segments produced in the tests.
pub const COMPRESSED_SEGMENTS_SIZE: usize = 1565;

/// Number of bytes reserved for ingested data in tests.
pub const INGESTED_RESERVED_MEMORY_IN_BYTES: usize = 5 * 1024 * 1024; // 5 MiB

/// Number of bytes reserved for uncompressed data in tests.
pub const UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5 * 1024 * 1024; // 5 MiB

/// Number of bytes reserved for compressed data in tests.
pub const COMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5 * 1024 * 1024; // 5 MiB

/// Named error bound with the value 0.0 to make tests more readable.
pub const ERROR_BOUND_ZERO: f32 = 0.0;

/// Named error bound with the value 1.0 to make tests more readable.
pub const ERROR_BOUND_ONE: f32 = 1.0;

/// Named error bound with the value 5.0 to make tests more readable.
pub const ERROR_BOUND_FIVE: f32 = 5.0;

/// Named error bound with the value 10.0 to make tests more readable.
pub const ERROR_BOUND_TEN: f32 = 10.0;

/// Named error bound with the value f32::MAX to make tests more readable.
pub const ERROR_BOUND_ABSOLUTE_MAX: f32 = f32::MAX;

/// Named error bound with the value 100.0 to make tests more readable.
pub const ERROR_BOUND_RELATIVE_MAX: f32 = 100.0;
