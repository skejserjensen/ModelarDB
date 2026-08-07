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

//! Implementation of helper functions to encode and decode types to and from the protobuf messages
//! defined in `flight/protocol.proto`. The module also provides functions to serialize and
//! deserialize encoded messages to and from bytes.

use std::env;
use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::logical_expr::Expr;
use datafusion_proto::bytes::Serializeable;
use prost::Message;
use prost::bytes::Bytes;

use crate::error::{ModelarDbTypesError, Result};
use crate::functions::{try_convert_bytes_to_schema, try_convert_schema_to_bytes};
use crate::types::{ErrorBound, GeneratedColumn, Node, ServerMode, TimeSeriesTableMetadata};

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/modelardb.flight.protocol.rs"));
}

/// Parse `argument` and encode it into a [`StorageConfiguration`](protocol::manager_metadata::StorageConfiguration)
/// protobuf message. If `argument` is not a valid remote data folder, return [`ModelarDbTypesError`].
pub fn argument_to_storage_configuration(
    argument: &str,
) -> Result<protocol::manager_metadata::StorageConfiguration> {
    match argument.split_once("://") {
        Some(("s3", bucket_name)) => {
            let endpoint = env::var("AWS_ENDPOINT")?;
            let access_key_id = env::var("AWS_ACCESS_KEY_ID")?;
            let secret_access_key = env::var("AWS_SECRET_ACCESS_KEY")?;

            Ok(
                protocol::manager_metadata::StorageConfiguration::S3Configuration(
                    protocol::manager_metadata::S3Configuration {
                        endpoint,
                        bucket_name: bucket_name.to_owned(),
                        access_key_id,
                        secret_access_key,
                    },
                ),
            )
        }
        Some(("azureblobstorage", container_name)) => {
            let account_name = env::var("AZURE_STORAGE_ACCOUNT_NAME")?;
            let access_key = env::var("AZURE_STORAGE_ACCESS_KEY")?;

            Ok(
                protocol::manager_metadata::StorageConfiguration::AzureConfiguration(
                    protocol::manager_metadata::AzureConfiguration {
                        account_name,
                        access_key,
                        container_name: container_name.to_owned(),
                    },
                ),
            )
        }
        _ => Err(ModelarDbTypesError::InvalidArgument(
            "Remote data folder must be s3://bucket-name or azureblobstorage://container-name."
                .to_owned(),
        )),
    }
}

/// Encode `node` into a [`NodeMetadata`](protocol::NodeMetadata) protobuf message.
pub fn encode_node(node: &Node) -> Result<protocol::NodeMetadata> {
    let server_mode = match node.mode {
        ServerMode::Edge => protocol::node_metadata::ServerMode::Edge,
        ServerMode::Cloud => protocol::node_metadata::ServerMode::Cloud,
    };

    Ok(protocol::NodeMetadata {
        url: node.url.clone(),
        server_mode: server_mode as i32,
    })
}

/// Decode a [`NodeMetadata`](protocol::NodeMetadata) protobuf message into a [`Node`].
pub fn decode_node_metadata(node_metadata: &protocol::NodeMetadata) -> Result<Node> {
    let server_mode = match protocol::node_metadata::ServerMode::try_from(node_metadata.server_mode)
    {
        Ok(protocol::node_metadata::ServerMode::Edge) => ServerMode::Edge,
        Ok(protocol::node_metadata::ServerMode::Cloud) => ServerMode::Cloud,
        _ => {
            return Err(ModelarDbTypesError::InvalidArgument(format!(
                "Unknown server mode: {}.",
                node_metadata.server_mode
            )));
        }
    };

    Ok(Node::new(node_metadata.url.clone(), server_mode))
}

/// Serialize the table metadata into a [`TableMetadata`](protocol::TableMetadata) protobuf message.
pub fn serialize_table_metadata(
    normal_table_metadata: Vec<protocol::table_metadata::NormalTableMetadata>,
    time_series_table_metadata: Vec<protocol::table_metadata::TimeSeriesTableMetadata>,
) -> Vec<u8> {
    let table_metadata = protocol::TableMetadata {
        normal_tables: normal_table_metadata,
        time_series_tables: time_series_table_metadata,
    };

    table_metadata.encode_to_vec()
}

/// Encode the metadata for a normal table into a [`TableMetadata`](protocol::TableMetadata)
/// protobuf message and serialize it. If the schema cannot be converted to bytes, return
/// [`ModelarDbTypesError`].
pub fn encode_and_serialize_normal_table_metadata(
    table_name: &str,
    schema: &Schema,
) -> Result<Vec<u8>> {
    let normal_table_metadata = encode_normal_table_metadata(table_name, schema)?;
    let table_metadata = protocol::TableMetadata {
        normal_tables: vec![normal_table_metadata],
        time_series_tables: vec![],
    };

    Ok(table_metadata.encode_to_vec())
}

/// If `schema` can be converted to bytes, encode the normal table metadata into a
/// [`NormalTableMetadata`](protocol::table_metadata::NormalTableMetadata) protobuf message,
/// otherwise return [`ModelarDbTypesError`].
pub fn encode_normal_table_metadata(
    table_name: &str,
    schema: &Schema,
) -> Result<protocol::table_metadata::NormalTableMetadata> {
    Ok(protocol::table_metadata::NormalTableMetadata {
        name: table_name.to_string(),
        schema: try_convert_schema_to_bytes(schema)?,
    })
}

/// Encode the metadata for a time series table into a [`TableMetadata`](protocol::TableMetadata)
/// protobuf message and serialize it. If the schema cannot be converted to bytes, return
/// [ModelarDbTypesError].
pub fn encode_and_serialize_time_series_table_metadata(
    time_series_table_metadata: &TimeSeriesTableMetadata,
) -> Result<Vec<u8>> {
    let time_series_table_metadata = encode_time_series_table_metadata(time_series_table_metadata)?;
    let table_metadata = protocol::TableMetadata {
        normal_tables: vec![],
        time_series_tables: vec![time_series_table_metadata],
    };

    Ok(table_metadata.encode_to_vec())
}

/// If `schema` can be converted to bytes, encode `time_series_table_metadata` into a
/// [`TimeSeriesTableMetadata`](protocol::table_metadata::TimeSeriesTableMetadata) protobuf message,
/// otherwise return [`ModelarDbTypesError`].
pub fn encode_time_series_table_metadata(
    time_series_table_metadata: &TimeSeriesTableMetadata,
) -> Result<protocol::table_metadata::TimeSeriesTableMetadata> {
    let mut generated_column_expressions =
        Vec::with_capacity(time_series_table_metadata.query_schema.fields.len());
    for generated_column in &time_series_table_metadata.generated_columns {
        if let Some(generated_column) = generated_column {
            let expr_bytes = generated_column.expr.to_bytes()?;
            generated_column_expressions.push(expr_bytes.to_vec());
        } else {
            // Empty bytes are used to represent columns that are not generated.
            generated_column_expressions.push(Bytes::new().to_vec());
        }
    }

    Ok(protocol::table_metadata::TimeSeriesTableMetadata {
        name: time_series_table_metadata.name.clone(),
        schema: try_convert_schema_to_bytes(&time_series_table_metadata.query_schema)?,
        error_bounds: encode_error_bounds(time_series_table_metadata),
        generated_column_expressions,
    })
}

/// Return a vector of serializable protobuf messages for the error bounds of
/// `time_series_table_metadata`.
fn encode_error_bounds(
    time_series_table_metadata: &TimeSeriesTableMetadata,
) -> Vec<protocol::table_metadata::time_series_table_metadata::ErrorBound> {
    // Since the time series table metadata does not include error bounds for the generated columns,
    // lossless error bounds are added for each generated column.
    let mut error_bounds_all =
        Vec::with_capacity(time_series_table_metadata.query_schema.fields().len());

    let absolute_error_bound =
        protocol::table_metadata::time_series_table_metadata::error_bound::Type::Absolute as i32;
    let relative_error_bound =
        protocol::table_metadata::time_series_table_metadata::error_bound::Type::Relative as i32;

    let lossless = protocol::table_metadata::time_series_table_metadata::ErrorBound {
        r#type: absolute_error_bound,
        value: 0.0,
    };

    for field in time_series_table_metadata.query_schema.fields() {
        if let Ok(field_index) = time_series_table_metadata.schema.index_of(field.name()) {
            let (error_bound_type, value) =
                match time_series_table_metadata.error_bounds[field_index] {
                    ErrorBound::Absolute(value) => (absolute_error_bound, value),
                    ErrorBound::Relative(value) => (relative_error_bound, value),
                };

            error_bounds_all.push(
                protocol::table_metadata::time_series_table_metadata::ErrorBound {
                    r#type: error_bound_type,
                    value,
                },
            );
        } else {
            error_bounds_all.push(lossless);
        }
    }

    error_bounds_all
}

/// Deserialize and extract the table metadata from `bytes` and return the table metadata as a tuple
/// of `(normal_table_metadata, time_series_table_metadata)`. `normal_table_metadata` is a vector of
/// tuples containing the table name and schema of the normal tables. If `bytes` cannot be
/// deserialized, return [`ModelarDbTypesError`].
#[allow(clippy::type_complexity)]
pub fn deserialize_and_extract_table_metadata(
    bytes: &[u8],
) -> Result<(Vec<(String, Schema)>, Vec<TimeSeriesTableMetadata>)> {
    let table_metadata = protocol::TableMetadata::decode(bytes)?;

    let mut normal_table_metadata = Vec::new();
    let mut time_series_table_metadata = Vec::new();

    for normal_table in table_metadata.normal_tables {
        let schema = try_convert_bytes_to_schema(normal_table.schema)?;
        normal_table_metadata.push((normal_table.name, schema));
    }

    for time_series_table in table_metadata.time_series_tables {
        let schema = Arc::new(try_convert_bytes_to_schema(time_series_table.schema)?);
        let metadata = TimeSeriesTableMetadata::try_new(
            time_series_table.name,
            schema.clone(),
            decode_error_bounds(&time_series_table.error_bounds)?,
            decode_generated_column_expressions(
                &time_series_table.generated_column_expressions,
                &schema.to_dfschema()?,
            )?,
        )?;

        time_series_table_metadata.push(metadata);
    }

    Ok((normal_table_metadata, time_series_table_metadata))
}

/// Decode the protobuf encoded error bounds into a vector of [`ErrorBound`]. Return
/// [`ModelarDbTypesError`] if the error bound type is unknown or the value is invalid.
fn decode_error_bounds(
    encoded_error_bounds: &[protocol::table_metadata::time_series_table_metadata::ErrorBound],
) -> Result<Vec<ErrorBound>> {
    let mut error_bounds = Vec::with_capacity(encoded_error_bounds.len());

    for error_bound in encoded_error_bounds {
        match protocol::table_metadata::time_series_table_metadata::error_bound::Type::try_from(
            error_bound.r#type,
        ) {
            Ok(
                protocol::table_metadata::time_series_table_metadata::error_bound::Type::Absolute,
            ) => error_bounds.push(ErrorBound::Absolute(error_bound.value)),
            Ok(
                protocol::table_metadata::time_series_table_metadata::error_bound::Type::Relative,
            ) => error_bounds.push(ErrorBound::Relative(error_bound.value)),
            _ => {
                return Err(ModelarDbTypesError::InvalidArgument(format!(
                    "Unknown error bound type: {}.",
                    error_bound.r#type
                )));
            }
        };
    }

    Ok(error_bounds)
}

/// Decode the generated column expressions from a vector of byte expressions into a vector of
/// optional [`GeneratedColumn`]. Return [`ModelarDbTypesError`] if the expression is invalid.
fn decode_generated_column_expressions(
    generated_column_expressions: &[Vec<u8>],
    df_schema: &DFSchema,
) -> Result<Vec<Option<GeneratedColumn>>> {
    let mut expressions = Vec::with_capacity(generated_column_expressions.len());

    for maybe_expr_bytes in generated_column_expressions {
        // If the column is not generated, the bytes will be empty.
        if maybe_expr_bytes.is_empty() {
            expressions.push(None);
        } else {
            let expr = Expr::from_bytes(maybe_expr_bytes)?;
            expressions.push(Some(GeneratedColumn::try_from_expr(expr, df_schema)?));
        }
    }

    Ok(expressions)
}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::{LazyLock, Mutex};

    use modelardb_test::table::{self, NORMAL_TABLE_NAME, TIME_SERIES_TABLE_NAME};

    /// Lock used for env::set_var() as it is not guaranteed to be thread-safe.
    static SET_VAR_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    // Tests for argument_to_storage_configuration().
    #[test]
    fn test_s3_argument_to_storage_configuration() {
        // env::set_var is safe to call in a single-threaded program.
        unsafe {
            let _mutex_guard = SET_VAR_LOCK.lock();
            env::set_var("AWS_ENDPOINT", "test_endpoint");
            env::set_var("AWS_ACCESS_KEY_ID", "test_access_key_id");
            env::set_var("AWS_SECRET_ACCESS_KEY", "test_secret_access_key");
        }

        let storage_configuration =
            argument_to_storage_configuration("s3://test_bucket_name").unwrap();

        match storage_configuration {
            protocol::manager_metadata::StorageConfiguration::S3Configuration(s3_configuration) => {
                assert_eq!(s3_configuration.endpoint, "test_endpoint");
                assert_eq!(s3_configuration.bucket_name, "test_bucket_name");
                assert_eq!(s3_configuration.access_key_id, "test_access_key_id");
                assert_eq!(s3_configuration.secret_access_key, "test_secret_access_key");
            }
            _ => panic!("Expected S3 connection type."),
        }
    }

    #[test]
    fn test_azureblobstorage_argument_to_storage_configuration() {
        // env::set_var is safe to call in a single-threaded program.
        unsafe {
            let _mutex_guard = SET_VAR_LOCK.lock();
            env::set_var("AZURE_STORAGE_ACCOUNT_NAME", "test_storage_account_name");
            env::set_var("AZURE_STORAGE_ACCESS_KEY", "test_storage_access_key");
        }

        let storage_configuration =
            argument_to_storage_configuration("azureblobstorage://test_container_name").unwrap();

        match storage_configuration {
            protocol::manager_metadata::StorageConfiguration::AzureConfiguration(
                azure_configuration,
            ) => {
                assert_eq!(
                    azure_configuration.account_name,
                    "test_storage_account_name"
                );
                assert_eq!(azure_configuration.access_key, "test_storage_access_key");
                assert_eq!(azure_configuration.container_name, "test_container_name");
            }
            _ => panic!("Expected Azure connection type."),
        }
    }

    #[test]
    fn test_invalid_argument_to_storage_configuration() {
        let result = argument_to_storage_configuration("googlecloudstorage://test");

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid Argument Error: Remote data folder must be s3://bucket-name or azureblobstorage://container-name."
                .to_owned()
        );
    }

    // Test for encode_node() and decode_node_metadata().
    #[test]
    fn test_encode_and_decode_node_metadata() {
        let node = Node::new("grpc://server:9999".to_string(), ServerMode::Edge);

        let encoded_node_metadata = encode_node(&node).unwrap();
        let decoded_node = decode_node_metadata(&encoded_node_metadata).unwrap();

        assert_eq!(node.url, decoded_node.url);
        assert_eq!(node.mode, decoded_node.mode);
    }

    // Test for encoding and decoding table metadata.
    #[test]
    fn test_encode_and_decode_table_metadata() {
        let protobuf_bytes = table::table_metadata_protobuf_bytes();

        // Deserialize and extract the table metadata.
        let (normal_tables, time_series_tables) =
            deserialize_and_extract_table_metadata(&protobuf_bytes).unwrap();

        assert_eq!(normal_tables[0].0, NORMAL_TABLE_NAME);
        assert_eq!(normal_tables[0].1, table::normal_table_schema());

        assert_eq!(time_series_tables[0].name, TIME_SERIES_TABLE_NAME);
        assert_eq!(
            time_series_tables[0].query_schema,
            table::time_series_table_metadata().query_schema
        );
    }
}
