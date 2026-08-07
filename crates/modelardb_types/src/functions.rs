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

//! Implementation of helper functions to operate on the types used through ModelarDB.

use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{IpcMessage, SchemaAsIpc};

use crate::error::Result;

/// Normalize `name` to allow direct comparisons between names.
pub fn normalize_name(name: &str) -> String {
    name.to_lowercase()
}

/// Return the Apache Arrow schema as bytes if `schema` can be converted to [`Vec<u8>`], otherwise
/// return [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn try_convert_schema_to_bytes(schema: &Schema) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let schema_as_ipc = SchemaAsIpc::new(schema, &options);

    let ipc_message: IpcMessage = schema_as_ipc.try_into()?;

    Ok(ipc_message.0.to_vec())
}

/// Return [`Schema`] if `schema_bytes` can be converted to an Apache Arrow schema, otherwise
/// return [`ModelarDbTypesError`](crate::error::ModelarDbTypesError).
pub fn try_convert_bytes_to_schema(schema_bytes: Vec<u8>) -> Result<Schema> {
    let ipc_message = IpcMessage(schema_bytes.into());
    Schema::try_from(ipc_message).map_err(|error| error.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::ArrowPrimitiveType;
    use arrow::datatypes::Field;

    use crate::types::ArrowValue;

    // Tests for normalize_name().
    #[test]
    fn test_normalize_table_name_lowercase_no_effect() {
        assert_eq!("table_name", normalize_name("table_name"));
    }

    #[test]
    fn test_normalize_table_name_uppercase() {
        assert_eq!("table_name", normalize_name("TABLE_NAME"));
    }

    #[test]
    fn test_normalize_table_name_mixed_case() {
        assert_eq!("table_name", normalize_name("Table_Name"));
    }

    // Tests for try_convert_schema_to_bytes() and try_convert_bytes_to_schema().
    #[test]
    fn test_schema_to_bytes_and_bytes_to_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field_1", ArrowValue::DATA_TYPE, false),
            Field::new("field_2", ArrowValue::DATA_TYPE, false),
        ]));

        // Serialize the schema to bytes.
        let bytes = try_convert_schema_to_bytes(&schema).unwrap();

        // Deserialize the bytes to the schema.
        let bytes_schema = try_convert_bytes_to_schema(bytes).unwrap();
        assert_eq!(*schema, bytes_schema);
    }

    #[test]
    fn test_invalid_bytes_to_schema() {
        assert!(try_convert_bytes_to_schema(vec!(1, 2, 4, 8)).is_err());
    }
}
