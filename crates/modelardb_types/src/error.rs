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

//! The [`Error`] and [`Result`] types used throughout `modelardb_types`.

use std::env::VarError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::result::Result as StdResult;

use arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use prost::DecodeError;

/// Result type used throughout `modelardb_types`.
pub type Result<T> = StdResult<T, ModelarDbTypesError>;

/// Error type used throughout `modelardb_types`.
#[derive(Debug)]
pub enum ModelarDbTypesError {
    /// Error returned by Apache Arrow.
    Arrow(ArrowError),
    /// Error returned by Apache DataFusion.
    DataFusion(DataFusionError),
    /// Error returned by environment variables.
    EnvironmentVar(VarError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned by Prost when decoding a message that is not valid.
    ProstDecode(DecodeError),
}

impl Display for ModelarDbTypesError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Arrow(reason) => write!(f, "Arrow Error: {reason}"),
            Self::DataFusion(reason) => write!(f, "DataFusion Error: {reason}"),
            Self::EnvironmentVar(reason) => write!(f, "Environment Variable Error: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "Invalid Argument Error: {reason}"),
            Self::ProstDecode(reason) => write!(f, "Prost Decode Error: {reason}"),
        }
    }
}

impl Error for ModelarDbTypesError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Return the error that caused self to occur if one exists.
        match self {
            Self::Arrow(reason) => Some(reason),
            Self::DataFusion(reason) => Some(reason),
            Self::EnvironmentVar(reason) => Some(reason),
            Self::InvalidArgument(_reason) => None,
            Self::ProstDecode(reason) => Some(reason),
        }
    }
}

impl From<ArrowError> for ModelarDbTypesError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow(error)
    }
}

impl From<DataFusionError> for ModelarDbTypesError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

impl From<VarError> for ModelarDbTypesError {
    fn from(error: VarError) -> Self {
        Self::EnvironmentVar(error)
    }
}

impl From<DecodeError> for ModelarDbTypesError {
    fn from(error: DecodeError) -> Self {
        Self::ProstDecode(error)
    }
}
