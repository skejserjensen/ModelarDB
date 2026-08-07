/* Copyright 2025 The ModelarDB Contributors
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

//! The [`Error`] and [`Result`] types used throughout `modelardb_embedded`.

use std::env::VarError;
use std::error::Error;
use std::fmt::Result as FmtResult;
use std::fmt::{Display, Formatter};
use std::result::Result as StdResult;
use std::str::Utf8Error;

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use deltalake::{DeltaTableError, ObjectStoreError};
use modelardb_storage::error::ModelarDbStorageError;
use modelardb_types::error::ModelarDbTypesError;
use tonic::Status as TonicStatusError;
use tonic::transport::Error as TonicTransportError;

/// Result type used throughout `modelardb_embedded`.
pub type Result<T> = StdResult<T, ModelarDbEmbeddedError>;

/// Error type used throughout `modelardb_embedded`.
#[derive(Debug)]
pub enum ModelarDbEmbeddedError {
    /// Error returned by Apache Arrow.
    Arrow(ArrowError),
    /// Error returned by Apache DataFusion.
    DataFusion(DataFusionError),
    /// Error returned by Delta Lake.
    DeltaLake(DeltaTableError),
    /// Error returned by environment variables.
    EnvironmentVar(VarError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned by modelardb_storage.
    ModelarDbStorage(ModelarDbStorageError),
    /// Error returned by modelardb_types.
    ModelarDbTypes(ModelarDbTypesError),
    /// Error returned by ObjectStore.
    ObjectStore(ObjectStoreError),
    /// Error returned by Apache Parquet.
    Parquet(ParquetError),
    /// Status returned by Tonic.
    TonicStatus(TonicStatusError),
    /// Error returned by Tonic.
    TonicTransport(TonicTransportError),
    /// Error returned when a feature is unimplemented.
    Unimplemented(String),
    /// Error returned by UTF-8.
    Utf8(Utf8Error),
}

impl Display for ModelarDbEmbeddedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Arrow(reason) => write!(f, "Arrow Error: {reason}"),
            Self::DataFusion(reason) => write!(f, "DataFusion Error: {reason}"),
            Self::DeltaLake(reason) => write!(f, "Delta Lake Error: {reason}"),
            Self::EnvironmentVar(reason) => write!(f, "Environment Variable Error: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "Invalid Argument Error: {reason}"),
            Self::ModelarDbStorage(reason) => write!(f, "ModelarDB Storage Error: {reason}"),
            Self::ModelarDbTypes(reason) => write!(f, "ModelarDB Types Error: {reason}"),
            Self::ObjectStore(reason) => write!(f, "Object Store Error: {reason}"),
            Self::Parquet(reason) => write!(f, "Parquet Error: {reason}"),
            Self::TonicStatus(reason) => write!(f, "Tonic Status Error: {reason}"),
            Self::TonicTransport(reason) => write!(f, "Tonic Transport Error: {reason}"),
            Self::Unimplemented(reason) => write!(f, "Unimplemented Error: {reason}"),
            Self::Utf8(reason) => write!(f, "UTF-8 Error: {reason}"),
        }
    }
}

impl Error for ModelarDbEmbeddedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arrow(reason) => Some(reason),
            Self::DataFusion(reason) => Some(reason),
            Self::DeltaLake(reason) => Some(reason),
            Self::EnvironmentVar(reason) => Some(reason),
            Self::InvalidArgument(_reason) => None,
            Self::ModelarDbStorage(reason) => Some(reason),
            Self::ModelarDbTypes(reason) => Some(reason),
            Self::ObjectStore(reason) => Some(reason),
            Self::Parquet(reason) => Some(reason),
            Self::TonicStatus(reason) => Some(reason),
            Self::TonicTransport(reason) => Some(reason),
            Self::Unimplemented(_reason) => None,
            Self::Utf8(reason) => Some(reason),
        }
    }
}

impl From<ArrowError> for ModelarDbEmbeddedError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow(error)
    }
}

impl From<DataFusionError> for ModelarDbEmbeddedError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

impl From<DeltaTableError> for ModelarDbEmbeddedError {
    fn from(error: DeltaTableError) -> Self {
        Self::DeltaLake(error)
    }
}

impl From<VarError> for ModelarDbEmbeddedError {
    fn from(error: VarError) -> Self {
        Self::EnvironmentVar(error)
    }
}

impl From<ModelarDbStorageError> for ModelarDbEmbeddedError {
    fn from(error: ModelarDbStorageError) -> Self {
        Self::ModelarDbStorage(error)
    }
}

impl From<ModelarDbTypesError> for ModelarDbEmbeddedError {
    fn from(error: ModelarDbTypesError) -> Self {
        Self::ModelarDbTypes(error)
    }
}

impl From<ObjectStoreError> for ModelarDbEmbeddedError {
    fn from(error: ObjectStoreError) -> Self {
        Self::ObjectStore(error)
    }
}

impl From<ParquetError> for ModelarDbEmbeddedError {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}

impl From<TonicStatusError> for ModelarDbEmbeddedError {
    fn from(error: TonicStatusError) -> Self {
        Self::TonicStatus(error)
    }
}

impl From<TonicTransportError> for ModelarDbEmbeddedError {
    fn from(error: TonicTransportError) -> Self {
        Self::TonicTransport(error)
    }
}

impl From<Utf8Error> for ModelarDbEmbeddedError {
    fn from(error: Utf8Error) -> Self {
        Self::Utf8(error)
    }
}
