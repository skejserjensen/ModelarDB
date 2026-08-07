/* Copyright 2024 The ModelarDB Contributors
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

//! The [`Error`] and [`Result`] types used throughout `modelardb_manager`.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use std::result::Result as StdResult;

use deltalake::errors::DeltaTableError;
use modelardb_storage::error::ModelarDbStorageError;
use modelardb_types::error::ModelarDbTypesError;
use tonic::Status as TonicStatusError;
use tonic::transport::Error as TonicTransportError;

/// Result type used throughout `modelardb_manager`.
pub type Result<T> = StdResult<T, ModelarDbManagerError>;

/// Error type used throughout `modelardb_manager`.
#[derive(Debug)]
pub enum ModelarDbManagerError {
    /// Error returned by Delta Lake.
    DeltaLake(DeltaTableError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned when an invalid state is encountered.
    InvalidState(String),
    /// Error returned from IO operations.
    Io(IoError),
    /// Error returned by modelardb_storage.
    ModelarDbStorage(ModelarDbStorageError),
    /// Error returned by modelardb_types.
    ModelarDbTypes(ModelarDbTypesError),
    /// Status returned by Tonic.
    TonicStatus(TonicStatusError),
    /// Error returned by Tonic.
    TonicTransport(TonicTransportError),
}

impl Display for ModelarDbManagerError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::DeltaLake(reason) => write!(f, "Delta Lake Error: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "Invalid Argument Error: {reason}"),
            Self::InvalidState(reason) => write!(f, "Invalid State Error: {reason}"),
            Self::Io(reason) => write!(f, "Io Error: {reason}"),
            Self::ModelarDbStorage(reason) => write!(f, "ModelarDB Storage Error: {reason}"),
            Self::ModelarDbTypes(reason) => write!(f, "ModelarDB Types Error: {reason}"),
            Self::TonicStatus(reason) => write!(f, "Tonic Status Error: {reason}"),
            Self::TonicTransport(reason) => write!(f, "Tonic Transport Error: {reason}"),
        }
    }
}

impl Error for ModelarDbManagerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Return the error that caused self to occur if one exists.
        match self {
            Self::DeltaLake(reason) => Some(reason),
            Self::InvalidArgument(_reason) => None,
            Self::InvalidState(_reason) => None,
            Self::Io(reason) => Some(reason),
            Self::ModelarDbStorage(reason) => Some(reason),
            Self::ModelarDbTypes(reason) => Some(reason),
            Self::TonicStatus(reason) => Some(reason),
            Self::TonicTransport(reason) => Some(reason),
        }
    }
}

impl From<DeltaTableError> for ModelarDbManagerError {
    fn from(error: DeltaTableError) -> Self {
        Self::DeltaLake(error)
    }
}

impl From<IoError> for ModelarDbManagerError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<ModelarDbStorageError> for ModelarDbManagerError {
    fn from(error: ModelarDbStorageError) -> Self {
        Self::ModelarDbStorage(error)
    }
}

impl From<ModelarDbTypesError> for ModelarDbManagerError {
    fn from(error: ModelarDbTypesError) -> Self {
        Self::ModelarDbTypes(error)
    }
}

impl From<TonicStatusError> for ModelarDbManagerError {
    fn from(error: TonicStatusError) -> Self {
        Self::TonicStatus(error)
    }
}

impl From<TonicTransportError> for ModelarDbManagerError {
    fn from(error: TonicTransportError) -> Self {
        Self::TonicTransport(error)
    }
}
