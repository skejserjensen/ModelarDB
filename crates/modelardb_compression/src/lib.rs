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

//! Compress batches of sorted data points to produce compressed segments containing metadata and
//! models, compute aggregates directly from the segments, and reconstruct the data points for each
//! compressed segment.

#![allow(clippy::too_many_arguments)]

mod compression;
pub mod error;
mod models;
mod types;

// Re-export the few functions and types users are meant to use.
pub use compression::try_compress;
pub use models::grid;
pub use models::is_value_within_error_bound;
pub use models::len;
pub use models::sum;
pub use models::timestamps::are_compressed_timestamps_regular;
pub use models::{MODEL_TYPE_COUNT, MODEL_TYPE_NAMES};
