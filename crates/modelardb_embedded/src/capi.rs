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

//! Implementation of a C-API for `modelardb_embedded`. Apache Arrow is generally used for returning
//! data to the caller so memory allocated in Rust is automatically deallocated by the `release`
//! callback as described in the documentation for Apache Arrow's [C Data Interface]. Instead of
//! returning data, it is generally written to one or more pointers provided by the caller. This is
//! to allow an error code to be returned to the caller and allow multiple types of data to be
//! provided to the caller, e.g., an array and a schema. It is also recommended in the documentation
//! for Apache Arrow's [C Data Interface]. [`modelardb_embedded_open_memory()`],
//! [`modelardb_embedded_open_local()`], [`modelardb_embedded_open_s3()`],
//! [`modelardb_embedded_open_azure()`], and [`modelardb_embedded_connect()`] are exceptions as
//! their object's size is only known in Rust.
//!
//! Throughout the C-API, bool is used instead of c_bool as [c_bool does not exist].
//!
//! [C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html#semantics
//! [c_bool does not exist]: https://github.com/rust-lang/rust/issues/95184

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::Path as StdPath;
use std::ptr;
use std::sync::{Arc, LazyLock};

use arrow::array::{self, Array, Float32Array, Int8Array, MapArray, StringArray, StructArray};
use arrow::ffi::{self, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use modelardb_types::types::ErrorBound;
use tokio::runtime::Runtime;

use crate::error::{ModelarDbEmbeddedError, Result};
use crate::operations::Operations;
use crate::operations::client::{Client, Node};
use crate::operations::data_folder::DataFolder;
use crate::record_batch_stream_to_record_batch;
use crate::{Aggregate, TableType};

/// A shared Tokio runtime for executing all asynchronous tasks when `modelardb_embedded` is not
/// used through Rust. If it cannot be created, `modelardb_embedded` will panic as an asynchronous
/// runtime is required.
pub static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

/// Return code to use if no errors occurred.
#[unsafe(no_mangle)]
pub static RETURN_SUCCESS: c_int = 0;

/// Return code to use if an error occurred.
#[unsafe(no_mangle)]
pub static RETURN_FAILURE: c_int = 1;

thread_local! {
    /// The last error encountered by this thread while executing a function through the C-API. It
    /// is [`None`] if a function in the C-API has yet to be executed by this thread. It is not
    /// automatically set to [`None`] when a function succeeds to match the behaviour of C's
    /// [`errno`].
    static THREAD_LAST_ERROR: RefCell<Option<ModelarDbEmbeddedError>> = const { RefCell::new(None) };

    /// The last error returned by [`modelardb_embedded_error()`] as a [`CString`]. Thus,
    /// [`THREAD_LAST_ERROR`] is only converted to a [`CString`] if [`modelardb_embedded_error()`]
    /// is called. In addition, since [`modelardb_embedded_error()`] returns a [`*const c_char`] to
    /// the [`CString`] in [`THREAD_LAST_ERROR_CSTRING`], the caller does not need to deallocate
    /// memory. However, the lifetime of the [`*const c_char`] returned by
    /// [`modelardb_embedded_error()`] ends when [`modelardb_embedded_error()`] is called again.
    static THREAD_LAST_ERROR_CSTRING: RefCell<Option<CString>> = const { RefCell::new(None) } ;
}

/// Creates a [`DataFolder`] that manages data in memory and returns a pointer to the [`DataFolder`]
/// or a zero-initialized pointer if an error occurs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_open_memory() -> *const c_void {
    let maybe_data_folder = open_memory();
    set_error_and_return_value_ptr(maybe_data_folder)
}

/// See documentation for [`modelardb_embedded_open_memory`].
fn open_memory() -> Result<DataFolder> {
    TOKIO_RUNTIME.block_on(DataFolder::open_memory())
}

/// Creates a [`DataFolder`] that manages data in the local folder at `data_folder_path_path` and
/// returns a pointer to the [`DataFolder`] or a zero-initialized pointer if an error occurs.
/// Assumes `data_folder_path_ptr` points to a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_open_local(
    data_folder_path_ptr: *const c_char,
) -> *const c_void {
    let maybe_data_folder = unsafe { open_local(data_folder_path_ptr) };
    set_error_and_return_value_ptr(maybe_data_folder)
}

/// See documentation for [`modelardb_embedded_open_local`].
unsafe fn open_local(data_folder_path_ptr: *const c_char) -> Result<DataFolder> {
    let data_folder_str = unsafe { c_char_ptr_to_str(data_folder_path_ptr)? };
    let data_folder_path = StdPath::new(data_folder_str);

    TOKIO_RUNTIME.block_on(DataFolder::open_local(data_folder_path))
}

/// Creates a [`DataFolder`] that manages data in an object store with a S3-compatible API and
/// returns a pointer to the [`DataFolder`] or a zero-initialized pointer if an error occurs.
/// Assumes `endpoint_ptr`, `bucket_name_ptr`, `access_key_id_ptr`, and `secret_access_key_ptr`
/// points to valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_open_s3(
    endpoint_ptr: *const c_char,
    bucket_name_ptr: *const c_char,
    access_key_id_ptr: *const c_char,
    secret_access_key_ptr: *const c_char,
) -> *const c_void {
    let maybe_data_folder = unsafe {
        open_s3(
            endpoint_ptr,
            bucket_name_ptr,
            access_key_id_ptr,
            secret_access_key_ptr,
        )
    };

    set_error_and_return_value_ptr(maybe_data_folder)
}

/// See documentation for [`modelardb_embedded_open_s3`].
unsafe fn open_s3(
    endpoint_ptr: *const c_char,
    bucket_name_ptr: *const c_char,
    access_key_id_ptr: *const c_char,
    secret_access_key_ptr: *const c_char,
) -> Result<DataFolder> {
    let endpoint = unsafe { c_char_ptr_to_str(endpoint_ptr)? };
    let bucket_name = unsafe { c_char_ptr_to_str(bucket_name_ptr)? };
    let access_key_id = unsafe { c_char_ptr_to_str(access_key_id_ptr)? };
    let secret_access_key = unsafe { c_char_ptr_to_str(secret_access_key_ptr)? };

    TOKIO_RUNTIME.block_on(DataFolder::open_s3(
        endpoint.to_owned(),
        bucket_name.to_owned(),
        access_key_id.to_owned(),
        secret_access_key.to_owned(),
    ))
}

/// Creates a [`DataFolder`] that manages data in an object store with an Azure-compatible API and
/// returns a pointer to the [`DataFolder`] or a zero-initialized pointer if an error occurs.
/// Assumes `account_name_ptr`, `access_key_ptr`, and `container_name_ptr` point to valid C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_open_azure(
    account_name_ptr: *const c_char,
    access_key_ptr: *const c_char,
    container_name_ptr: *const c_char,
) -> *const c_void {
    let maybe_data_folder =
        unsafe { open_azure(account_name_ptr, access_key_ptr, container_name_ptr) };
    set_error_and_return_value_ptr(maybe_data_folder)
}

/// See documentation for [`modelardb_embedded_open_azure`].
unsafe fn open_azure(
    account_name_ptr: *const c_char,
    access_key_ptr: *const c_char,
    container_name_ptr: *const c_char,
) -> Result<DataFolder> {
    let account_name = unsafe { c_char_ptr_to_str(account_name_ptr)? };
    let access_key = unsafe { c_char_ptr_to_str(access_key_ptr)? };
    let container_name = unsafe { c_char_ptr_to_str(container_name_ptr)? };

    TOKIO_RUNTIME.block_on(DataFolder::open_azure(
        account_name.to_owned(),
        access_key.to_owned(),
        container_name.to_owned(),
    ))
}

/// Creates a [`Client`] that is connected to the Apache Arrow Flight server URL in `node_url_ptr`
/// and returns a pointer to the [`Client`] or a zero-initialized pointer if an error occurs.
/// Assumes `node_url_ptr` points to a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_connect(
    node_url_ptr: *const c_char,
    is_server_node: bool,
) -> *const c_void {
    let maybe_client = unsafe { connect(node_url_ptr, is_server_node) };
    set_error_and_return_value_ptr(maybe_client)
}

/// See documentation for [`modelardb_embedded_connect`].
unsafe fn connect(node_url_ptr: *const c_char, is_server_node: bool) -> Result<Client> {
    let node_url_str = unsafe { c_char_ptr_to_str(node_url_ptr)? };

    let node = if is_server_node {
        Node::Server(node_url_str.to_owned())
    } else {
        Node::Manager(node_url_str.to_owned())
    };

    TOKIO_RUNTIME.block_on(Client::connect(node))
}

/// Moves the value in `maybe_value` to a [`Box`] and returns a pointer to it if `maybe_value` is
/// [`Ok`], otherwise sets [`THREAD_LAST_ERROR`] and returns [`ptr::null`].
fn set_error_and_return_value_ptr<T>(maybe_value: Result<T>) -> *const c_void {
    match maybe_value {
        Ok(value) => {
            let boxed_value = Box::new(value);
            Box::into_raw(boxed_value).cast()
        }
        Err(error) => {
            THREAD_LAST_ERROR.set(Some(error));
            ptr::null()
        }
    }
}

/// Converts `maybe_operations_ptr` to a [`DataFolder`] or [`Client`] and deallocates it if
/// `maybe_operations_ptr` is not null or unaligned. Assumes `maybe_operations_ptr` points to a
/// [`DataFolder`] or [`Client`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_close(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
) -> c_int {
    if is_data_folder {
        let maybe_data_folder_ptr: *mut DataFolder = maybe_operations_ptr.cast();
        if !maybe_data_folder_ptr.is_null() && maybe_data_folder_ptr.is_aligned() {
            // The box is assigned to _data_folder as Box::from_raw() is #[must_use].
            let _data_folder = unsafe { Box::from_raw(maybe_data_folder_ptr) };
            RETURN_SUCCESS
        } else {
            RETURN_FAILURE
        }
    } else {
        let maybe_client_ptr: *mut Client = maybe_operations_ptr.cast();
        if !maybe_client_ptr.is_null() && maybe_client_ptr.is_aligned() {
            // The box is assigned to _client as Box::from_raw() is #[must_use].
            let _client = unsafe { Box::from_raw(maybe_client_ptr) };
            RETURN_SUCCESS
        } else {
            RETURN_FAILURE
        }
    }
}

/// Creates a table with the name in `table_name_ptr`, the schema in `schema_ptr`, and the error
/// bounds in `error_bounds_ptr` in the [`DataFolder`] or [`Client`] in `maybe_operations_ptr`.
/// Assumes `maybe_operations_ptr` points to a [`DataFolder`] or [`Client`]; `table_name_ptr` points
/// to a valid C string; `schema_ptr` points to an Apache Arrow [`Schema`]; `error_bounds_array_ptr`
/// and `error_bounds_array_schema_ptr` point to a [`MapArray`] that maps from field column names to
/// error bounds; and `generated_columns_array_ptr` and `generated_columns_array_schema_ptr` point
/// to a [`MapArray`] that maps from field column names to error bounds. If an error bound is zero
/// the column with that name is stored losslessly, if the error bound is positive it is interpreted
/// as an absolute error bound, and if the error bound is negative it is interpreted as a relative
/// error bound.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_create(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
    is_time_series_table: bool,
    schema_ptr: *const FFI_ArrowSchema,
    error_bounds_array_ptr: *const FFI_ArrowArray,
    error_bounds_array_schema_ptr: *const FFI_ArrowSchema,
    generated_columns_array_ptr: *const FFI_ArrowArray,
    generated_columns_array_schema_ptr: *const FFI_ArrowSchema,
) -> c_int {
    let maybe_unit = unsafe {
        create(
            maybe_operations_ptr,
            is_data_folder,
            table_name_ptr,
            is_time_series_table,
            schema_ptr,
            error_bounds_array_ptr,
            error_bounds_array_schema_ptr,
            generated_columns_array_ptr,
            generated_columns_array_schema_ptr,
        )
    };

    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_create`].
#[allow(clippy::too_many_arguments)]
unsafe fn create(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
    is_time_series_table: bool,
    schema_ptr: *const FFI_ArrowSchema,
    error_bounds_array_ptr: *const FFI_ArrowArray,
    error_bounds_array_schema_ptr: *const FFI_ArrowSchema,
    generated_columns_array_ptr: *const FFI_ArrowArray,
    generated_columns_array_schema_ptr: *const FFI_ArrowSchema,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };
    let table_name = unsafe { c_char_ptr_to_str(table_name_ptr)? };
    let schema = unsafe { (&*schema_ptr).try_into()? };

    let error_bounds_array_data = unsafe {
        ffi::from_ffi(
            error_bounds_array_ptr.read(),
            &error_bounds_array_schema_ptr.read(),
        )?
    };

    let generated_columns_array_data = unsafe {
        ffi::from_ffi(
            generated_columns_array_ptr.read(),
            &generated_columns_array_schema_ptr.read(),
        )?
    };

    let table_type = if is_time_series_table {
        let error_bounds_array: MapArray = error_bounds_array_data.into();
        let error_bounds = error_bounds_array_to_error_bounds(&error_bounds_array);
        let generated_columns_array: MapArray = generated_columns_array_data.into();
        let generated_columns = map_array_to_map_of_string_to_string(&generated_columns_array);
        TableType::TimeSeriesTable(schema, error_bounds, generated_columns)
    } else {
        TableType::NormalTable(schema)
    };

    TOKIO_RUNTIME.block_on(modelardb.create(table_name, table_type))
}

/// Converts the [`MapArray`] in error_bounds_array` to a [`HashMap`]. If a value is zero the column
/// with that name is stored losslessly, if the value is positive it is interpreted as an absolute
/// error bound, and if the value is negative it is interpreted as a relative error bound. Assumes
/// the error bound values are finite [`f32`]s.
fn error_bounds_array_to_error_bounds(
    error_bounds_array: &MapArray,
) -> HashMap<String, ErrorBound> {
    let error_bounds_array_entries = error_bounds_array.entries();
    let column_names = modelardb_types::array!(error_bounds_array_entries, 0, StringArray);
    let error_bounds_values = modelardb_types::array!(error_bounds_array_entries, 1, Float32Array);

    let mut error_bounds = HashMap::new();
    for index in 0..error_bounds_array_entries.len() {
        let column_name = column_names.value(index);
        let error_bound_value = error_bounds_values.value(index);

        // unwrap() is safe because the error bound values are finite [`f32`]s.
        let error_bound = if error_bound_value < 0.0 {
            ErrorBound::try_new_relative(-error_bound_value).unwrap()
        } else {
            ErrorBound::try_new_absolute(error_bound_value).unwrap()
        };
        error_bounds.insert(column_name.to_owned(), error_bound);
    }

    error_bounds
}

/// Writes the name of all tables to `tables_array_ptr` and `tables_array_schema_ptr` in the
/// [`DataFolder`] or [`Client`] in `maybe_operations_ptr`. Assumes `maybe_operations_ptr` points to
/// a [`DataFolder`] or [`Client`]; `table_array_ptr` is a valid pointer to enough memory for an
/// Apache Arrow C Data Interface Array; and `tables_array_schema_ptr` is a valid pointer to enough
/// memory for an Apache Arrow C Data Interface Schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_tables(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    tables_array_ptr: *mut FFI_ArrowArray,
    tables_array_schema_ptr: *mut FFI_ArrowSchema,
) -> c_int {
    let maybe_unit = unsafe {
        tables(
            maybe_operations_ptr,
            is_data_folder,
            tables_array_ptr,
            tables_array_schema_ptr,
        )
    };

    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_tables`].
unsafe fn tables(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    tables_array_ptr: *mut FFI_ArrowArray,
    tables_array_schema_ptr: *mut FFI_ArrowSchema,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };

    let tables = TOKIO_RUNTIME.block_on(modelardb.tables())?;

    let tables_array = StringArray::from(tables);
    let tables_array_data = tables_array.into_data();
    let (out_array, out_schema) = ffi::to_ffi(&tables_array_data)?;
    unsafe { tables_array_ptr.write(out_array) };
    unsafe { tables_array_schema_ptr.write(out_schema) };

    Ok(())
}

/// Writes the [`Schema`] of the table with the name in `table_name_ptr` in the [`DataFolder`] or
/// [`Client`] in `maybe_operations_ptr` to `schema_struct_array_ptr` and
/// `schema_struct_array_schema_ptr`. Assumes `maybe_operations_ptr` points to a [`DataFolder`] or
/// [`Client`]; table_name_ptr` points to a valid C string; schema_struct_array_ptr` is a valid
/// pointer to enough memory for an Apache Arrow C Data Interface Array; and
/// `schema_struct_array_schema_ptr` is a valid pointer to enough memory for an Apache Arrow C Data
/// Interface Schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_schema(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
    schema_struct_array_ptr: *mut FFI_ArrowArray,
    schema_struct_array_schema_ptr: *mut FFI_ArrowSchema,
) -> c_int {
    let maybe_unit = unsafe {
        schema(
            maybe_operations_ptr,
            is_data_folder,
            table_name_ptr,
            schema_struct_array_ptr,
            schema_struct_array_schema_ptr,
        )
    };
    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_schema`].
unsafe fn schema(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
    schema_struct_array_ptr: *mut FFI_ArrowArray,
    schema_struct_array_schema_ptr: *mut FFI_ArrowSchema,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };
    let table_name = unsafe { c_char_ptr_to_str(table_name_ptr)? };

    let schema = TOKIO_RUNTIME.block_on(modelardb.schema(table_name))?;
    let schema_batch = RecordBatch::new_empty(Arc::new(schema));

    // The schema is returned using an empty record batch since using a pointer to the schema
    // causes an ArrowInvalid error.
    unsafe {
        record_batch_to_pointers(
            schema_batch,
            schema_struct_array_ptr,
            schema_struct_array_schema_ptr,
        )
    }
}

/// Writes the data in `struct_array_ptr` and `struct_array_schema_ptr` to the table with the table
/// name in `table_name_ptr` in the [`DataFolder`] or [`Client`] in `maybe_operations_ptr`. Assumes
/// `maybe_operations_ptr` points to a [`DataFolder`] or [`Client`]; `table_name_ptr` points to a
/// valid C string; `struct_array_ptr` is a valid pointer to enough memory for an Apache Arrow C
/// Data Interface Array; and `struct_array_schema_ptr` is a valid pointer to enough memory for an
/// Apache Arrow C Data Interface Schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_write(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
    struct_array_ptr: *const FFI_ArrowArray,
    struct_array_schema_ptr: *const FFI_ArrowSchema,
) -> c_int {
    let maybe_unit = unsafe {
        write(
            maybe_operations_ptr,
            is_data_folder,
            table_name_ptr,
            struct_array_ptr,
            struct_array_schema_ptr,
        )
    };

    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_write`].
unsafe fn write(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
    struct_array_ptr: *const FFI_ArrowArray,
    struct_array_schema_ptr: *const FFI_ArrowSchema,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };
    let table_name = unsafe { c_char_ptr_to_str(table_name_ptr)? };

    let uncompressed_data =
        unsafe { pointers_to_record_batch(struct_array_ptr, struct_array_schema_ptr)? };

    TOKIO_RUNTIME.block_on(modelardb.write(table_name, uncompressed_data))
}

/// Executes the SQL in `sql_ptr` in the [`DataFolder`] or [`Client`] in `maybe_operations_ptr` and
/// writes the result to `decompressed_struct_array_ptr` and `decompressed_struct_array_schema_ptr`.
/// Assumes `maybe_operations_ptr` points to a [`DataFolder`] or [`Client`]; `sql_ptr` points to a
/// valid C string; `decompressed_struct_array_ptr` is a valid pointer to enough memory for an
/// Apache Arrow C Data Interface Array; and `decompressed_struct_array_schema_ptr` is a valid
/// pointer to enough memory for an Apache Arrow C Data Interface Schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_read(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    sql_ptr: *const c_char,
    decompressed_struct_array_ptr: *mut FFI_ArrowArray,
    decompressed_struct_array_schema_ptr: *mut FFI_ArrowSchema,
) -> c_int {
    let maybe_unit = unsafe {
        read(
            maybe_operations_ptr,
            is_data_folder,
            sql_ptr,
            decompressed_struct_array_ptr,
            decompressed_struct_array_schema_ptr,
        )
    };

    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_read`].
unsafe fn read(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    sql_ptr: *const c_char,
    decompressed_struct_array_ptr: *mut FFI_ArrowArray,
    decompressed_struct_array_schema_ptr: *mut FFI_ArrowSchema,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };
    let sql = unsafe { c_char_ptr_to_str(sql_ptr)? };

    let decompressed_data_stream = TOKIO_RUNTIME.block_on(modelardb.read(sql))?;
    let decompressed_data = TOKIO_RUNTIME.block_on(record_batch_stream_to_record_batch(
        decompressed_data_stream,
    ))?;

    unsafe {
        record_batch_to_pointers(
            decompressed_data,
            decompressed_struct_array_ptr,
            decompressed_struct_array_schema_ptr,
        )
    }
}

/// Executes the SQL in `sql_ptr` in the [`DataFolder`] or [`Client`] in `maybe_source_operations_ptr`
/// and copies the result to the normal table in `maybe_target_operations_ptr`. Assumes
/// `maybe_source_operations_ptr` points to a [`DataFolder`] or [`Client`]; `sql_ptr` points to a valid
/// C string; `maybe_target_operations_ptr` points to a [`DataFolder`] or [`Client`]; and
/// `target_table_name_ptr` points to a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_copy(
    maybe_source_operations_ptr: *mut c_void,
    is_data_folder: bool,
    sql_ptr: *const c_char,
    maybe_target_operations_ptr: *mut c_void,
    target_table_name_ptr: *const c_char,
) -> c_int {
    let maybe_unit = unsafe {
        copy(
            maybe_source_operations_ptr,
            is_data_folder,
            sql_ptr,
            maybe_target_operations_ptr,
            target_table_name_ptr,
        )
    };

    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_copy`].
unsafe fn copy(
    maybe_source_operations_ptr: *mut c_void,
    is_data_folder: bool,
    sql_ptr: *const c_char,
    maybe_target_operations_ptr: *mut c_void,
    target_table_name_ptr: *const c_char,
) -> Result<()> {
    let source = unsafe { c_void_to_operations(maybe_source_operations_ptr, is_data_folder)? };
    let sql = unsafe { c_char_ptr_to_str(sql_ptr)? };

    let target = unsafe { c_void_to_operations(maybe_target_operations_ptr, is_data_folder)? };
    let target_table_name = unsafe { c_char_ptr_to_str(target_table_name_ptr)? };

    TOKIO_RUNTIME.block_on(source.copy(sql, target, target_table_name))
}

/// Reads data from the time series table with the table name in `table_name_ptr` in the [`DataFolder`] or
/// [`Client`] in `maybe_operations_ptr` and writes it to `decompressed_struct_array_ptr` and
/// `decompressed_struct_array_schema_ptr`. The remaining parameters optionally specify which subset
/// of the data to read. Assumes `maybe_operations_ptr` points to a [`DataFolder`] or [`Client`];
/// `table_name_ptr` points to a valid C string; `columns_array_ptr` and `columns_array_schema_ptr`
/// points to a [`StructArray`]; `group_by_array_ptr` and `group_by_array_schema_ptr` points to a
/// [`StringArray`]; `start_time_ptr` points to a valid C string with an ISO 8601 timestamp;
/// `end_time_ptr` points to a valid C string with an ISO 8601 timestamp; `tags_array_ptr` and
/// `tags_array_schema_ptr` points to a [`MapArray`]; decompressed_struct_array_ptr` is a valid
/// pointer to enough memory for an Apache Arrow C Data Interface Array; and
/// `decompressed_struct_array_schema_ptr` is a valid pointer to enough memory for an Apache Arrow C
/// Data Interface Schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_read_time_series_table(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
    columns_array_ptr: *const FFI_ArrowArray,
    columns_array_schema_ptr: *const FFI_ArrowSchema,
    group_by_array_ptr: *const FFI_ArrowArray,
    group_by_array_schema_ptr: *const FFI_ArrowSchema,
    start_time_ptr: *const c_char,
    end_time_ptr: *const c_char,
    tags_array_ptr: *const FFI_ArrowArray,
    tags_array_schema_ptr: *const FFI_ArrowSchema,
    decompressed_struct_array_ptr: *mut FFI_ArrowArray,
    decompressed_struct_array_schema_ptr: *mut FFI_ArrowSchema,
) -> c_int {
    let maybe_unit = unsafe {
        read_time_series_table(
            maybe_operations_ptr,
            is_data_folder,
            table_name_ptr,
            columns_array_ptr,
            columns_array_schema_ptr,
            group_by_array_ptr,
            group_by_array_schema_ptr,
            start_time_ptr,
            end_time_ptr,
            tags_array_ptr,
            tags_array_schema_ptr,
            decompressed_struct_array_ptr,
            decompressed_struct_array_schema_ptr,
        )
    };

    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_read_time_series_table`].
#[allow(clippy::too_many_arguments)]
unsafe fn read_time_series_table(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
    columns_array_ptr: *const FFI_ArrowArray,
    columns_array_schema_ptr: *const FFI_ArrowSchema,
    group_by_array_ptr: *const FFI_ArrowArray,
    group_by_array_schema_ptr: *const FFI_ArrowSchema,
    start_time_ptr: *const c_char,
    end_time_ptr: *const c_char,
    tags_array_ptr: *const FFI_ArrowArray,
    tags_array_schema_ptr: *const FFI_ArrowSchema,
    decompressed_struct_array_ptr: *mut FFI_ArrowArray,
    decompressed_struct_array_schema_ptr: *mut FFI_ArrowSchema,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };

    let table_name = unsafe { c_char_ptr_to_str(table_name_ptr)? };
    let maybe_start_time = unsafe { c_char_ptr_to_maybe_str(start_time_ptr)? };
    let maybe_end_time = unsafe { c_char_ptr_to_maybe_str(end_time_ptr)? };
    let columns_array_data =
        unsafe { ffi::from_ffi(columns_array_ptr.read(), &columns_array_schema_ptr.read())? };
    let columns_array: StructArray = columns_array_data.into();
    let columns = columns_array_to_columns(&columns_array)?;
    let group_by_array_data =
        unsafe { ffi::from_ffi(group_by_array_ptr.read(), &group_by_array_schema_ptr.read())? };
    let group_by_array: StringArray = group_by_array_data.into();
    let group_by: Vec<String> = (0..group_by_array.len())
        .map(|index| group_by_array.value(index).to_owned())
        .collect();
    let tags_array_data =
        unsafe { ffi::from_ffi(tags_array_ptr.read(), &tags_array_schema_ptr.read())? };
    let tags_array: MapArray = tags_array_data.into();
    let tags = map_array_to_map_of_string_to_string(&tags_array);

    let decompressed_data_stream = TOKIO_RUNTIME.block_on(modelardb.read_time_series_table(
        table_name,
        &columns,
        &group_by,
        maybe_start_time,
        maybe_end_time,
        tags,
    ))?;

    let decompressed_data = TOKIO_RUNTIME.block_on(record_batch_stream_to_record_batch(
        decompressed_data_stream,
    ))?;

    unsafe {
        record_batch_to_pointers(
            decompressed_data,
            decompressed_struct_array_ptr,
            decompressed_struct_array_schema_ptr,
        )
    }
}

/// Converts the [`StructArray`] in `columns_array` to a list of (String, [`Aggregate`]) tuples,
/// returns [`ModelarDbEmbeddedError`] if the [`i8`] is not defined in [`Aggregate`].
fn columns_array_to_columns(columns_array: &StructArray) -> Result<Vec<(String, Aggregate)>> {
    let column_names = modelardb_types::array!(columns_array, 0, StringArray);
    let column_aggregate_values = modelardb_types::array!(columns_array, 1, Int8Array);

    let mut columns = vec![];
    for index in 0..columns_array.len() {
        let column_name = column_names.value(index);
        let column_aggregate_value = column_aggregate_values.value(index);
        let column_aggregate = match column_aggregate_value {
            0 => Ok(Aggregate::None),
            1 => Ok(Aggregate::Count),
            2 => Ok(Aggregate::Min),
            3 => Ok(Aggregate::Max),
            4 => Ok(Aggregate::Sum),
            5 => Ok(Aggregate::Avg),
            _ => Err(ModelarDbEmbeddedError::InvalidArgument(
                "Unsupported aggregate operation.".to_owned(),
            )),
        };

        columns.push((column_name.to_owned(), column_aggregate?));
    }

    Ok(columns)
}

/// Copies data from the time series table with the name in `source_table_name_ptr` in the [`DataFolder`] or
/// [`Client`] in `maybe_source_operations_ptr` to the time series table with the name in
/// `target_table_name_ptr` in the [`DataFolder`] or [`Client`] in
/// `maybe_target_operations_ptr`. `maybe_source_operations_ptr` and `maybe_target_operations_ptr`
/// cannot be different types. The remaining parameters optionally specify which subset of the data
/// to copy. Duplicate data is not dropped. Assumes `maybe_source_operations_ptr` points to a
/// [`DataFolder`] or [`Client`]; `source_table_name_ptr` points to a valid C string;
/// `maybe_target_operations_ptr` points to a [`DataFolder`] or [`Client`]; and `target_table_name_ptr`
/// points to a valid C string; `start_time_ptr` points to a valid C string with an ISO 8601
/// timestamp; `end_time_ptr` points to a valid C string with an ISO 8601 timestamp; and
/// `tags_array_ptr` and `tags_array_schema_ptr` points to a [`MapArray`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_copy_time_series_table(
    maybe_source_operations_ptr: *mut c_void,
    is_data_folder: bool,
    source_table_name_ptr: *const c_char,
    maybe_target_operations_ptr: *mut c_void,
    target_table_name_ptr: *const c_char,
    start_time_ptr: *const c_char,
    end_time_ptr: *const c_char,
    tags_array_ptr: *const FFI_ArrowArray,
    tags_array_schema_ptr: *const FFI_ArrowSchema,
) -> c_int {
    let maybe_unit = unsafe {
        copy_time_series_table(
            maybe_source_operations_ptr,
            is_data_folder,
            source_table_name_ptr,
            maybe_target_operations_ptr,
            target_table_name_ptr,
            start_time_ptr,
            end_time_ptr,
            tags_array_ptr,
            tags_array_schema_ptr,
        )
    };

    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_copy_time_series_table`].
#[allow(clippy::too_many_arguments)]
unsafe fn copy_time_series_table(
    maybe_source_operations_ptr: *mut c_void,
    is_data_folder: bool,
    source_table_name_ptr: *const c_char,
    maybe_target_operations_ptr: *mut c_void,
    target_table_name_ptr: *const c_char,
    start_time_ptr: *const c_char,
    end_time_ptr: *const c_char,
    tags_array_ptr: *const FFI_ArrowArray,
    tags_array_schema_ptr: *const FFI_ArrowSchema,
) -> Result<()> {
    let source = unsafe { c_void_to_operations(maybe_source_operations_ptr, is_data_folder)? };
    let source_table_name = unsafe { c_char_ptr_to_str(source_table_name_ptr)? };

    let target = unsafe { c_void_to_operations(maybe_target_operations_ptr, is_data_folder)? };
    let target_table_name = unsafe { c_char_ptr_to_str(target_table_name_ptr)? };

    let maybe_start_time = unsafe { c_char_ptr_to_maybe_str(start_time_ptr)? };
    let maybe_end_time = unsafe { c_char_ptr_to_maybe_str(end_time_ptr)? };
    let tags_array_data =
        unsafe { ffi::from_ffi(tags_array_ptr.read(), &tags_array_schema_ptr.read())? };
    let tags_array: MapArray = tags_array_data.into();
    let tags = map_array_to_map_of_string_to_string(&tags_array);

    TOKIO_RUNTIME.block_on(source.copy_time_series_table(
        source_table_name,
        target,
        target_table_name,
        maybe_start_time,
        maybe_end_time,
        tags,
    ))
}

/// Returns a reference to a [`&str`] if `maybe_cstr_ptr` is a valid pointer to a [`CStr`] in UTF-8.
/// Returns [`None`] if `maybe_cstr_ptr` is null and [`ModelarDbEmbeddedError`] if `maybe_cstr_ptr`
/// is unaligned or if the string is not UTF-8.
unsafe fn c_char_ptr_to_maybe_str<'a>(maybe_cstr_ptr: *const c_char) -> Result<Option<&'a str>> {
    if maybe_cstr_ptr.is_null() {
        Ok(None)
    } else {
        unsafe { c_char_ptr_to_str(maybe_cstr_ptr).map(Some) }
    }
}

/// Converts the [`MapArray`] in `map_array` to a [`HashMap`].
fn map_array_to_map_of_string_to_string(map_array: &MapArray) -> HashMap<String, String> {
    let entries = map_array.entries();
    let keys = modelardb_types::array!(entries, 0, StringArray);
    let values = modelardb_types::array!(entries, 1, StringArray);

    let mut map = HashMap::new();
    for index in 0..entries.len() {
        let key = keys.value(index);
        let value = values.value(index);
        map.insert(key.to_owned(), value.to_owned());
    }

    map
}

/// Move all data from the table with the name in `source_table_name_ptr` in the [`DataFolder`] or
/// [`Client`] in `maybe_source_operations_ptr` to the table with the name in `target_table_name_ptr`
/// in the [`DataFolder`] or [`Client`] in `maybe_target_operations_ptr`. Assumes
/// `maybe_source_operations_ptr` points to a [`DataFolder`] or [`Client`]; `source_table_name_ptr`
/// points to a valid C string; `maybe_target_operations_ptr` points to a [`DataFolder`] or [`Client`];
/// and `target_table_name_ptr` points to a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_move(
    maybe_source_operations_ptr: *mut c_void,
    is_data_folder: bool,
    source_table_name_ptr: *const c_char,
    maybe_target_operations_ptr: *mut c_void,
    target_table_name_ptr: *const c_char,
) -> c_int {
    let maybe_unit = unsafe {
        r#move(
            maybe_source_operations_ptr,
            is_data_folder,
            source_table_name_ptr,
            maybe_target_operations_ptr,
            target_table_name_ptr,
        )
    };

    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_move`].
unsafe fn r#move(
    maybe_source_operations_ptr: *mut c_void,
    is_data_folder: bool,
    source_table_name_ptr: *const c_char,
    maybe_target_operations_ptr: *mut c_void,
    target_table_name_ptr: *const c_char,
) -> Result<()> {
    let source = unsafe { c_void_to_operations(maybe_source_operations_ptr, is_data_folder)? };
    let source_table_name = unsafe { c_char_ptr_to_str(source_table_name_ptr)? };

    let target = unsafe { c_void_to_operations(maybe_target_operations_ptr, is_data_folder)? };
    let target_table_name = unsafe { c_char_ptr_to_str(target_table_name_ptr)? };

    TOKIO_RUNTIME.block_on(source.r#move(source_table_name, target, target_table_name))
}

/// Truncates the table with the name in `table_name_ptr` in the [`DataFolder`] or [`Client`] in
/// `maybe_operations_ptr`. Assumes `maybe_operations_ptr` points to a [`DataFolder`] or [`Client`];
/// and `table_name_ptr` points to a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_truncate(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
) -> c_int {
    let maybe_unit = unsafe { truncate(maybe_operations_ptr, is_data_folder, table_name_ptr) };
    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_truncate`].
unsafe fn truncate(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };
    let table_name = unsafe { c_char_ptr_to_str(table_name_ptr)? };

    TOKIO_RUNTIME.block_on(modelardb.truncate(table_name))
}

/// Drops the table with the name in `table_name_ptr` in the [`DataFolder`] or [`Client`] in
/// `maybe_operations_ptr`. Assumes `maybe_operations_ptr` points to a [`DataFolder`] or [`Client`];
/// and `table_name_ptr` points to a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_drop(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
) -> c_int {
    let maybe_unit = unsafe { drop(maybe_operations_ptr, is_data_folder, table_name_ptr) };
    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_drop`].
unsafe fn drop(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };
    let table_name = unsafe { c_char_ptr_to_str(table_name_ptr)? };

    TOKIO_RUNTIME.block_on(modelardb.drop(table_name))
}

/// Vacuums the table with the name in `table_name_ptr` in the [`DataFolder`] or [`Client`] in
/// `maybe_operations_ptr`. Assumes `maybe_operations_ptr` points to a [`DataFolder`] or [`Client`];
/// and `table_name_ptr` points to a valid C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_vacuum(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
) -> c_int {
    let maybe_unit = unsafe { vacuum(maybe_operations_ptr, is_data_folder, table_name_ptr) };
    set_error_and_return_code(maybe_unit)
}

/// See documentation for [`modelardb_embedded_vacuum`].
unsafe fn vacuum(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
    table_name_ptr: *const c_char,
) -> Result<()> {
    let modelardb = unsafe { c_void_to_operations(maybe_operations_ptr, is_data_folder)? };
    let table_name = unsafe { c_char_ptr_to_str(table_name_ptr)? };

    TOKIO_RUNTIME.block_on(modelardb.vacuum(table_name))
}

/// Return a read-only [`*const c_char`] with a human-readable representation of the last error the
/// current thread encountered. The lifetime of the returned [`*const c_char`] ends when
/// [`modelardb_embedded_error()`] is called again. If no errors have occurred, a zero-initialized
/// pointer is returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn modelardb_embedded_error() -> *const c_char {
    // with_borrow_mut() is safe as THREAD_LAST_ERROR is stored in local storage created by
    // thread_local!() and with_borrow_mut() is only called here.
    THREAD_LAST_ERROR.with_borrow_mut(|maybe_error| match maybe_error {
        Some(error) => {
            // The error is converted to a CString by best effort by using
            // CString::from_vec_unchecked() instead of CString::new() so as much as possible of the
            // error is returned to the caller if the error string contains zero bytes.
            let error_string_bytes = error.to_string().into();
            let error_cstring = unsafe { CString::from_vec_unchecked(error_string_bytes) };
            THREAD_LAST_ERROR_CSTRING.set(Some(error_cstring));

            THREAD_LAST_ERROR_CSTRING.with_borrow(|maybe_error_cstring| {
                // unwrap() is safe as THREAD_LAST_ERROR_CSTRING has just been set above.
                maybe_error_cstring.as_ref().unwrap().as_ptr()
            })
        }
        None => ptr::null(),
    })
}

/// Returns a [`&mut DataFolder`] if `maybe_operations_ptr` is a valid pointer to a [`DataFolder`]
/// or a [`&mut Client`] if `maybe_operations_ptr` is a valid pointer to a [`Client`]. Returns
/// [`ModelarDbEmbeddedError`] if `maybe_operations_ptr` is null or unaligned. Assumes that multiple
/// mutable references to the same [`DataFolder`] or [`Client`] are not created.
unsafe fn c_void_to_operations<'a>(
    maybe_operations_ptr: *mut c_void,
    is_data_folder: bool,
) -> Result<&'a mut dyn Operations> {
    if is_data_folder {
        let maybe_data_folder_ptr: *mut DataFolder = maybe_operations_ptr.cast();
        if !maybe_data_folder_ptr.is_null() && maybe_data_folder_ptr.is_aligned() {
            unsafe { Ok(&mut *maybe_data_folder_ptr) }
        } else {
            Err(ModelarDbEmbeddedError::InvalidArgument(
                "Pointer to DataFolder is null or unaligned.".to_owned(),
            ))
        }
    } else {
        let maybe_client_ptr: *mut Client = maybe_operations_ptr.cast();
        if !maybe_client_ptr.is_null() && maybe_client_ptr.is_aligned() {
            unsafe { Ok(&mut *maybe_client_ptr) }
        } else {
            Err(ModelarDbEmbeddedError::InvalidArgument(
                "Pointer to Client is null or unaligned.".to_owned(),
            ))
        }
    }
}

/// Returns a reference to a [`&str`] if `maybe_cstr_ptr` is a valid pointer to a [`CStr`] in UTF-8.
/// Returns [`ModelarDbEmbeddedError`] if `maybe_cstr_ptr` is null, unaligned, or if the string is
/// not UTF-8.
unsafe fn c_char_ptr_to_str<'a>(maybe_cstr_ptr: *const c_char) -> Result<&'a str> {
    if maybe_cstr_ptr.is_null() {
        Err(ModelarDbEmbeddedError::InvalidArgument(
            "Pointer to string is null.".to_owned(),
        ))
    } else if !maybe_cstr_ptr.is_aligned() {
        Err(ModelarDbEmbeddedError::InvalidArgument(
            "Pointer to string is not aligned.".to_owned(),
        ))
    } else {
        unsafe {
            CStr::from_ptr(maybe_cstr_ptr)
                .to_str()
                .map_err(|error| ModelarDbEmbeddedError::InvalidArgument(error.to_string()))
        }
    }
}

/// Create a [`StructArray`] from `struct_array_ptr` and `struct_array_schema_ptr` and convert it to
/// a [`RecordBatch`]. A non-zero value is returned with the following meaning if an error occurs:
/// 1. Failed to read an array from `struct_array_ptr` and `struct_array_schema_ptr`.
/// 2. Failed to convert the array to a [`StructArray`].
unsafe fn pointers_to_record_batch(
    struct_array_ptr: *const FFI_ArrowArray,
    struct_array_schema_ptr: *const FFI_ArrowSchema,
) -> Result<RecordBatch> {
    let struct_array_data = if let Ok(struct_array_data) =
        unsafe { ffi::from_ffi(struct_array_ptr.read(), &struct_array_schema_ptr.read()) }
    {
        struct_array_data
    } else {
        return Err(ModelarDbEmbeddedError::InvalidArgument(
            "C Data Interface data could not be converted to StructArray.".to_owned(),
        ));
    };

    let struct_array = array::make_array(struct_array_data);
    let record_batch: RecordBatch =
        if let Some(struct_array) = struct_array.as_any().downcast_ref::<StructArray>() {
            struct_array.into()
        } else {
            return Err(ModelarDbEmbeddedError::InvalidArgument(
                "C Data Interface data could not be converted to RecordBatch.".to_owned(),
            ));
        };

    Ok(record_batch)
}

/// Convert `record_batch` to a [`StructArray`] and write the address of its data to
/// `struct_array_ptr` and the address of its schema to `struct_array_schema_ptr`. A non-zero value
/// is returned with the following meaning if an error occurs: 4. Failed to convert the
/// [`RecordBatch`] to a [`StructArray`].
unsafe fn record_batch_to_pointers(
    record_batch: RecordBatch,
    struct_array_ptr: *mut FFI_ArrowArray,
    struct_array_schema_ptr: *mut FFI_ArrowSchema,
) -> Result<()> {
    let struct_array: StructArray = record_batch.into();
    let struct_array_data = struct_array.into_data();

    if let Ok((out_array, out_schema)) = ffi::to_ffi(&struct_array_data) {
        unsafe { struct_array_ptr.write(out_array) };
        unsafe { struct_array_schema_ptr.write(out_schema) };
        Ok(())
    } else {
        Err(ModelarDbEmbeddedError::InvalidArgument(
            "RecordBatch could not be converted to C Data Interface data.".to_owned(),
        ))
    }
}

/// Returns [`RETURN_SUCCESS`] if `maybe_unit` is [`Ok`], otherwise sets [`THREAD_LAST_ERROR`] and
/// returns [`RETURN_FAILURE`].
fn set_error_and_return_code(maybe_unit: Result<()>) -> c_int {
    match maybe_unit {
        Ok(_) => RETURN_SUCCESS,
        Err(error) => {
            THREAD_LAST_ERROR.set(Some(error));
            RETURN_FAILURE
        }
    }
}
