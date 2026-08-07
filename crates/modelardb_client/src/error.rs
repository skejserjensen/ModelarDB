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

//! The [`Error`] and [`Result`] types used throughout `modelardb_client`.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use std::result::Result as StdResult;

use arrow::error::ArrowError;
use rustyline::error::ReadlineError as RustyLineError;
use tonic::Status as TonicStatusError;
use tonic::transport::Error as TonicTransportError;

/// Result type used throughout `modelardb_client`.
pub type Result<T> = StdResult<T, ModelarDbClientError>;

/// Error type used throughout `modelardb_client`.
#[derive(Debug)]
pub enum ModelarDbClientError {
    /// Error returned by Apache Arrow.
    Arrow(ArrowError),
    /// Error returned when an invalid argument was passed.
    InvalidArgument(String),
    /// Error returned from IO operations.
    Io(IoError),
    /// Error returned by RustyLine.
    RustyLine(RustyLineError),
    /// Status returned by Tonic.
    TonicStatus(TonicStatusError),
    /// Error returned by Tonic.
    TonicTransport(TonicTransportError),
}

impl Display for ModelarDbClientError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Arrow(reason) => write!(f, "Arrow Error: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "Invalid Argument Error: {reason}"),
            Self::Io(reason) => write!(f, "Io Error: {reason}"),
            Self::RustyLine(reason) => write!(f, "RustyLine Error: {reason}"),
            Self::TonicStatus(reason) => write!(f, "Tonic Status Error: {reason}"),
            Self::TonicTransport(reason) => write!(f, "Tonic Transport Error: {reason}"),
        }
    }
}

impl Error for ModelarDbClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Return the error that caused self to occur if one exists.
        match self {
            Self::Arrow(reason) => Some(reason),
            Self::InvalidArgument(_reason) => None,
            Self::Io(reason) => Some(reason),
            Self::RustyLine(reason) => Some(reason),
            Self::TonicStatus(reason) => Some(reason),
            Self::TonicTransport(reason) => Some(reason),
        }
    }
}

impl From<ArrowError> for ModelarDbClientError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow(error)
    }
}

impl From<IoError> for ModelarDbClientError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<RustyLineError> for ModelarDbClientError {
    fn from(error: RustyLineError) -> Self {
        Self::RustyLine(error)
    }
}

impl From<TonicStatusError> for ModelarDbClientError {
    fn from(error: TonicStatusError) -> Self {
        Self::TonicStatus(error)
    }
}

impl From<TonicTransportError> for ModelarDbClientError {
    fn from(error: TonicTransportError) -> Self {
        Self::TonicTransport(error)
    }
}
