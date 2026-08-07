# Copyright 2025 The ModelarDB Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import platform
import warnings
from datetime import datetime
from enum import Enum
from typing import Self

import pyarrow
from pyarrow import MapArray, RecordBatch, Schema, StringArray
from pyarrow.cffi import ffi

from .node import Server, Manager
from .error_bound import AbsoluteErrorBound
from .table import NormalTable, TimeSeriesTable
from .ffi_array import FFIArray


class Aggregate(Enum):
    """Aggregate operations supported by :meth:`Operations.read`."""

    NONE = 0
    COUNT = 1
    MIN = 2
    MAX = 3
    SUM = 4
    AVG = 5


class Operations:
    """Class used to interact with a ModelarDB data folder or a ModelarDB node."""

    @staticmethod
    def __find_and_load_library():
        """Finds and loads the correct version of ModelarDB Embedded on this system.

        :raises RuntimeError: If there is no compatible version of ModelarDB Embedded.
        """

        # Inner function so __find_and_load_library() can call it despite being static.
        def __find_library(build: str) -> str:
            """Finds the correct version of ModelarDB Embedded on this system.

            :raises RuntimeError: If it is not executed on Linux, macOS, or Windows.
            """
            script_folder = os.path.dirname(os.path.abspath(__file__))

            # Select the library to use based on the operating system.
            match platform.system():
                case "Linux":
                    library_path = (
                            script_folder
                            + f"/../../../../../target/{build}/libmodelardb_embedded.so"
                    )
                case "Darwin":
                    library_path = (
                            script_folder
                            + f"/../../../../../target/{build}/libmodelardb_embedded.dylib"
                    )
                case "Windows":
                    library_path = (
                            script_folder
                            + f"\\..\\..\\..\\..\\..\\target\\{build}\\modelardb_embedded.dll"
                    )
                case _:
                    raise RuntimeError("Only Linux, macOS, and Windows are supported.")

            if not os.path.isfile(library_path):
                raise RuntimeError("The Rust library has not been compiled.")

            return library_path

        # Compute the path to the current working directory to locate the library.
        try:
            library_path = __find_library("release")
        except RuntimeError:
            library_path = __find_library("debug")
            warnings.warn("Using debug build, compile with --release.", RuntimeWarning)

        # cffi is used instead of ctypes as it is being used by pyarrow.cffi.
        ffi.cdef(
            """
            static int RETURN_SUCCESS;
            static int RETURN_FAILURE;

            void* modelardb_embedded_open_memory();
            void* modelardb_embedded_open_local(char* data_folder_path_ptr);
            void* modelardb_embedded_open_s3(char* endpoint_ptr,
                                             char* bucket_name_ptr,
                                             char* access_key_id_ptr,
                                             char* secret_access_key_ptr);
            void* modelardb_embedded_open_azure(char* account_name_ptr,
                                                char* access_key_ptr,
                                                char* container_name_ptr);

            void* modelardb_embedded_connect(char* node_url_ptr,
                                             bool is_server_node);

            int modelardb_embedded_close(void* maybe_operations_ptr, 
                                         bool is_data_folder);

            int modelardb_embedded_create(void* maybe_operations_ptr,
                                          bool is_data_folder,
                                          char* table_name_ptr,
                                          bool is_time_series_table,
                                          struct ArrowSchema* schema_ptr,
                                          struct ArrowArray* error_bound_array_ptr,
                                          struct ArrowSchema* error_bound_array_schema_ptr,
                                          struct ArrowArray* generated_columns_array_ptr,
                                          struct ArrowSchema* generated_columns_array_schema_ptr);

            int modelardb_embedded_tables(void* maybe_operations_ptr,
                                          bool is_data_folder,
                                          struct ArrowArray* tables_array_ptr,
                                          struct ArrowSchema* tables_array_schema_ptr);

            int modelardb_embedded_schema(void* maybe_operations_ptr,
                                          bool is_data_folder,
                                          char* table_name_ptr,
                                          struct ArrowArray* schema_struct_array_ptr,
                                          struct ArrowSchema* schema_struct_array_schema_ptr);

            int modelardb_embedded_write(void* maybe_operations_ptr,
                                         bool is_data_folder,
                                         char* table_name_ptr,
                                         struct ArrowArray* uncompressed_struct_ptr,
                                         struct ArrowSchema* uncompressed_struct_schema_ptr);

            int modelardb_embedded_read(void* maybe_operations_ptr,
                                        bool is_data_folder,
                                        char* sql_ptr,
                                        struct ArrowArray* decompressed_struct_ptr,
                                        struct ArrowSchema* decompressed_struct_schema_ptr);

            int modelardb_embedded_copy(void* maybe_source_operations_ptr,
                                        bool is_data_folder,
                                        char* sql_ptr,
                                        void* maybe_target_operations_ptr,
                                        char* target_table_name_ptr);

            int modelardb_embedded_read_time_series_table(void* maybe_operations_ptr,
                                                          bool is_data_folder,
                                                          char* table_name_ptr,
                                                          struct ArrowArray* columns_array_ptr,
                                                          struct ArrowSchema* columns_array_schema_ptr,
                                                          struct ArrowArray* group_by_array_ptr,
                                                          struct ArrowSchema* group_by_array_schema_ptr,
                                                          char* start_time_ptr,
                                                          char* end_time_ptr,
                                                          struct ArrowArray* tags_array_ptr,
                                                          struct ArrowSchema* tags_array_schema_ptr,
                                                          struct ArrowArray* decompressed_struct_array_ptr,
                                                          struct ArrowSchema* decompressed_struct_array_schema_ptr);

            int modelardb_embedded_copy_time_series_table(void* maybe_source_operations_ptr,
                                                          bool is_data_folder,
                                                          char* source_table_name_ptr,
                                                          void* maybe_target_operations_ptr,
                                                          char* target_table_name_ptr,
                                                          char* start_time_ptr,
                                                          char* end_time_ptr,
                                                          struct ArrowArray* tags_array_ptr,
                                                          struct ArrowSchema* tags_array_schema_ptr);

            int modelardb_embedded_move(void* maybe_source_operations_ptr,
                                        bool is_data_folder,
                                        char* source_table_name_ptr,
                                        void* maybe_target_operations_ptr,
                                        char* target_table_name_ptr);

            int modelardb_embedded_truncate(void* maybe_operations_ptr,
                                            bool is_data_folder,
                                            char* table_name_ptr);

            int modelardb_embedded_drop(void* maybe_operations_ptr,
                                        bool is_data_folder,
                                        char* table_name_ptr);
            
            int modelardb_embedded_vacuum(void* maybe_operations_ptr,
                                          bool is_data_folder,
                                          char* table_name_ptr);

            char* modelardb_embedded_error();
            """
        )

        return ffi.dlopen(library_path)

    # __library is a class variable to ensure the dynamic library's interface is
    # only defined once by ffi.cdef() and that it is loaded by ffi.dlopen() once.
    __library = __find_and_load_library()

    @classmethod
    def open_memory(cls):
        """Create an :obj:`Operations` data folder that manages data in memory.

        :return: The constructed :obj:`Operations`.
        :rtype: Operations
        """
        self: Operations = cls()

        self.__operations_ptr = self.__library.modelardb_embedded_open_memory()
        self.__is_data_folder = True

        if self.__operations_ptr == ffi.NULL:
            raise ValueError("Failed to create memory data folder.")

        return self

    @classmethod
    def open_local(cls, data_folder_path: str):
        """Create an :obj:`Operations` data folder that manages data in the local folder at `data_folder_path`.

        :param data_folder_path: The path of the data folder.
        :type data_folder_path: str
        :return: The constructed :obj:`Operations`.
        :rtype: Operations
        """
        self: Operations = cls()

        data_folder_path_ptr = ffi.new("char[]", bytes(data_folder_path, "UTF-8"))

        self.__operations_ptr = self.__library.modelardb_embedded_open_local(
            data_folder_path_ptr
        )
        self.__is_data_folder = True

        if self.__operations_ptr == ffi.NULL:
            raise ValueError("Failed to create local data folder.")

        return self

    @classmethod
    def open_s3(
            cls, endpoint: str, bucket_name: str, access_key_id: str, secret_access_key: str
    ):
        """Create an :obj:`Operations` data folder that manages data in an object store with a S3-compatible API.

        :param endpoint: The endpoint of the S3-compatible object store.
        :type endpoint: str
        :param bucket_name: The name of the bucket to read data from and write data to.
        :type bucket_name: str
        :param access_key_id: The access key id to use for authentication.
        :type access_key_id: str
        :param secret_access_key: The secret access key to use for authentication.
        :type secret_access_key: str
        :return: The constructed :obj:`Operations`.
        :rtype: Operations
        """
        self: Operations = cls()

        endpoint_ptr = ffi.new("char[]", bytes(endpoint, "UTF-8"))
        bucket_name_ptr = ffi.new("char[]", bytes(bucket_name, "UTF-8"))
        access_key_id_ptr = ffi.new("char[]", bytes(access_key_id, "UTF-8"))
        secret_access_key_ptr = ffi.new("char[]", bytes(secret_access_key, "UTF-8"))

        self.__operations_ptr = self.__library.modelardb_embedded_open_s3(
            endpoint_ptr, bucket_name_ptr, access_key_id_ptr, secret_access_key_ptr
        )
        self.__is_data_folder = True

        if self.__operations_ptr == ffi.NULL:
            raise ValueError("Failed to create S3 data folder.")

        return self

    @classmethod
    def open_azure(cls, account_name: str, access_key: str, container_name: str):
        """Create an :obj:`Operations` data folder that manages data in an object store with an Azure-compatible API.

        :param account_name: The account name to use for authentication.
        :type account_name: str
        :param access_key: The secret access key to use for authentication.
        :type access_key: str
        :param container_name: The name of the container to read data from and write data to.
        :type container_name: str
        :return: The constructed :obj:`Operations`.
        :rtype: Operations
        """
        self: Operations = cls()

        account_name_ptr = ffi.new("char[]", bytes(account_name, "UTF-8"))
        access_key_ptr = ffi.new("char[]", bytes(access_key, "UTF-8"))
        container_name_ptr = ffi.new("char[]", bytes(container_name, "UTF-8"))

        self.__operations_ptr = self.__library.modelardb_embedded_open_azure(
            account_name_ptr, access_key_ptr, container_name_ptr
        )
        self.__is_data_folder = True

        if self.__operations_ptr == ffi.NULL:
            raise ValueError("Failed to create Azure data folder.")

        return self

    @classmethod
    def connect(cls, node: Server | Manager):
        """Create a connection to an :obj:`Operations` node.

        :param node: The ModelarDB node to connect to.
        :type node: Server | Manager
        """
        self: Operations = cls()

        node_url_ptr = ffi.new("char[]", bytes(node.url, "UTF-8"))

        self.__operations_ptr = self.__library.modelardb_embedded_connect(
            node_url_ptr, isinstance(node, Server)
        )
        self.__is_data_folder = False

        if self.__operations_ptr == ffi.NULL:
            raise ValueError("Failed to connect to ModelarDB node.")

        return self

    def __del__(self):
        """Close the connection to the local folder, object store, or node and deallocate the memory."""
        return_code = self.__library.modelardb_embedded_close(
            self.__operations_ptr, self.__is_data_folder
        )
        self.__check_return_code_and_raise_error(return_code)

    def create(self, table_name: str, table_type: NormalTable | TimeSeriesTable):
        """Creates a table with `table_name`, `schema`, and `error_bounds`.

        :param table_name: The name of the table to create.
        :type table_name: str
        :param table_type: The type of the table to create.
        :type table_name: NormalTable or TimeSeriesTable
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))

        schema_ptr = ffi.new("struct ArrowSchema*")
        schema_ptr_int = int(ffi.cast("uintptr_t", schema_ptr))
        table_type.schema._export_to_c(schema_ptr_int)

        if isinstance(table_type, NormalTable):
            is_time_series_table = False
            error_bounds = {}
            generated_columns = {}
        elif isinstance(table_type, TimeSeriesTable):
            is_time_series_table = True

            # AbsoluteErrorBound is encoded as positive values while
            # RelativeErrorBound is encoded as negative values.
            error_bounds = {
                (column_name, error_bound.value)
                if type(error_bound) is AbsoluteErrorBound
                else (column_name, -error_bound.value)
                for column_name, error_bound in table_type.error_bounds.items()
            }
            generated_columns = table_type.generated_columns
        else:
            raise ValueError("table_type must be a NormalTable or a TimeSeriesTable")

        error_bounds_array: MapArray = pyarrow.array(
            [error_bounds], pyarrow.map_(pyarrow.string(), pyarrow.float32())
        )
        error_bounds_ffi = FFIArray.from_array(error_bounds_array)

        generated_columns_array: MapArray = pyarrow.array(
            [generated_columns], pyarrow.map_(pyarrow.string(), pyarrow.string())
        )
        generated_columns_ffi = FFIArray.from_array(generated_columns_array)

        return_code = self.__library.modelardb_embedded_create(
            self.__operations_ptr,
            self.__is_data_folder,
            table_name_ptr,
            is_time_series_table,
            schema_ptr,
            error_bounds_ffi.array_ptr,
            error_bounds_ffi.schema_ptr,
            generated_columns_ffi.array_ptr,
            generated_columns_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

    def tables(self) -> list[str]:
        """Returns the name of all the tables.

        :return: The name of all the tables.
        :rtype: list[str]
        :raises ValueError: If incorrect arguments are provided.
        """

        tables_ffi = FFIArray.from_type(StringArray)

        return_code = self.__library.modelardb_embedded_tables(
            self.__operations_ptr,
            self.__is_data_folder,
            tables_ffi.array_ptr,
            tables_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

        return tables_ffi.array().to_pylist()

    def schema(self, table_name: str) -> Schema:
        """Returns the schema of the table with `table_name`.

        :param table_name: The name of the table to return a schema for.
        :type table_name: str
        :return: The schema of the table with `table_name`.
        :rtype: Schema
        :raises ValueError: If a table with `table_name` does not exist.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))

        # The schema is retrieved using an empty record batch since using a pointer to the schema causes an
        # ArrowInvalid error.
        schema_batch_ffi = FFIArray.from_type(RecordBatch)

        return_code = self.__library.modelardb_embedded_schema(
            self.__operations_ptr,
            self.__is_data_folder,
            table_name_ptr,
            schema_batch_ffi.array_ptr,
            schema_batch_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

        schema_batch: RecordBatch = schema_batch_ffi.array()
        return schema_batch.schema

    def write(self, table_name: str, uncompressed_batch: RecordBatch):
        """Writes the data in `uncompressed_batch` to the table with
        `table_name`.

        :param table_name: The name of the table to write to.
        :type table_name: str
        :param uncompressed_batch: The data to write.
        :type uncompressed_batch: RecordBatch
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))

        uncompressed_batch_ffi = FFIArray.from_array(uncompressed_batch)

        return_code = self.__library.modelardb_embedded_write(
            self.__operations_ptr,
            self.__is_data_folder,
            table_name_ptr,
            uncompressed_batch_ffi.array_ptr,
            uncompressed_batch_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

    def read(self, sql: str) -> RecordBatch:
        """Executes an `sql` statement and returns the result.

        :param sql: An SQL statement.
        :type sql: str
        :return: The result of executing `sql`.
        :rtype: RecordBatch
        :raises ValueError: If incorrect arguments are provided.
        """
        sql_ptr = ffi.new("char[]", bytes(sql, "UTF-8"))

        decompressed_batch_ffi = FFIArray.from_type(RecordBatch)

        return_code = self.__library.modelardb_embedded_read(
            self.__operations_ptr,
            self.__is_data_folder,
            sql_ptr,
            decompressed_batch_ffi.array_ptr,
            decompressed_batch_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

        return decompressed_batch_ffi.array()

    def copy(self, sql: str, target: Self, target_table_name: str):
        """Executes an `sql` statement and copies the result to the normal
        table with `target_table_name` in `target`. Data can be copied from
        both normal tables and time series tables but only to normal tables. Duplicate
        data is not dropped. This is to not lossy compress data multiple times.

        :param sql: An SQL statement.
        :type sql: str
        :param target: :obj:`Operations` to write data from `self` to.
        :type target: Operations
        :param target_table_name: Name of the normal table to write data to.
        :type target_table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        sql_ptr = ffi.new("char[]", bytes(sql, "UTF-8"))

        target = target.__operations_ptr
        target_table_name = ffi.new("char[]", bytes(target_table_name, "UTF-8"))

        return_code = self.__library.modelardb_embedded_copy(
            self.__operations_ptr,
            self.__is_data_folder,
            sql_ptr,
            target,
            target_table_name,
        )
        self.__check_return_code_and_raise_error(return_code)

    def read_time_series_table(
            self,
            table_name: str,
            columns: None | list[str] | list[tuple[str, Aggregate]] = None,
            group_by: None | list[str] = None,
            start_time: None | datetime | str = None,
            end_time: None | datetime | str = None,
            tags: None | dict[str, str] = None,
    ) -> RecordBatch:
        """Reads data from the time series table with `table_name` and returns it. The
        remaining parameters optionally specify which subset of the data to
        read.

        :param table_name: The name of the time series table to read data from.
        :type table_name: str
        :param columns: A subset of columns to read or aggregate by.
        :type columns: list[str] | list[tuple[str, Aggregate]], optional
        :param group_by: A subset of columns to group by.
        :type group_by: list[str], optional
        :param start_time: A start time to filter by as a `datetime` or an ISO 8601 `str`.
        :type start_time: datetime | str, optional
        :param end_time: An end time to filter by as a `datetime` or an ISO 8601 `str`.
        :type end_time: datetime | str, optional
        :param tags: One or more tag and tag value pairs to filter by.
        :type tags: dict[str, str], optional
        :return: The data from the time series table with `table_name`.
        :rtype: RecordBatch
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))

        column_names = []
        aggregates = []
        if type(columns) is list and all(type(column) is str for column in columns):
            column_names = columns
            aggregates = [Aggregate.NONE.value] * len(columns)
        elif type(columns) is list and all(type(column) is tuple for column in columns):
            column_names = [column_name for column_name, _aggregate in columns]
            aggregates = [aggregate.value for _column_name, aggregate in columns]

        arrays = [
            pyarrow.array(column_names, pyarrow.string()),
            pyarrow.array(aggregates, pyarrow.int8()),
        ]
        columns_array = pyarrow.StructArray.from_arrays(
            arrays, names=["columns", "aggregates"]
        )
        columns_ffi = FFIArray.from_array(columns_array)

        if group_by is None:
            group_by = []
        group_by_array = pyarrow.array(group_by, pyarrow.string())
        group_by_ffi = FFIArray.from_array(group_by_array)

        start_time_ptr = self.__str_to_c_char_ptr(self.__get_timestamp(start_time))
        end_time_ptr = self.__str_to_c_char_ptr(self.__get_timestamp(end_time))

        if tags is None:
            tags = {}
        tags_array = pyarrow.array(
            [tags], pyarrow.map_(pyarrow.string(), pyarrow.string())
        )
        tags_ffi = FFIArray.from_array(tags_array)

        decompressed_batch_ffi = FFIArray.from_type(RecordBatch)

        return_code = self.__library.modelardb_embedded_read_time_series_table(
            self.__operations_ptr,
            self.__is_data_folder,
            table_name_ptr,
            columns_ffi.array_ptr,
            columns_ffi.schema_ptr,
            group_by_ffi.array_ptr,
            group_by_ffi.schema_ptr,
            start_time_ptr,
            end_time_ptr,
            tags_ffi.array_ptr,
            tags_ffi.schema_ptr,
            decompressed_batch_ffi.array_ptr,
            decompressed_batch_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

        return decompressed_batch_ffi.array()

    def copy_time_series_table(
            self,
            source_table_name: str,
            target: Self,
            target_table_name: str,
            start_time: None | datetime | str = None,
            end_time: None | datetime | str = None,
            tags: None | dict[str, str] = None,
    ):
        """Copies data from the time series table with `source_table_name` in `self` to
        the time series table with `target_table_name` in `target`. The remaining
        parameters optionally specify which subset of the data to copy.
        Duplicate data is not dropped.

        :param source_table_name: Name of the time series table to read data from.
        :type source_table_name: str
        :param target: :obj:`Operations` to write data from `self` to.
        :type target: Operations
        :param target_table_name: Name of the time series table to write data to.
        :type target_table_name: str
        :param start_time: A start time to filter by as a `datetime` or an ISO 8601 `str`.
        :type start_time: datetime | str, optional
        :param end_time: An end time to filter by as a `datetime` or an ISO 8601 `str`.
        :type end_time: datetime | str, optional
        :param tags: One or more tag and tag value pairs to filter by.
        :type tags: dict[str, str], optional
        :raises ValueError: If incorrect arguments are provided.
        """
        source = self.__operations_ptr
        source_table_name = ffi.new("char[]", bytes(source_table_name, "UTF-8"))

        target = target.__operations_ptr
        target_table_name = ffi.new("char[]", bytes(target_table_name, "UTF-8"))

        start_time_ptr = self.__str_to_c_char_ptr(self.__get_timestamp(start_time))
        end_time_ptr = self.__str_to_c_char_ptr(self.__get_timestamp(end_time))

        if tags is None:
            tags = {}
        tags_array = pyarrow.array(
            [tags], pyarrow.map_(pyarrow.string(), pyarrow.string())
        )
        tags_ffi = FFIArray.from_array(tags_array)

        return_code = self.__library.modelardb_embedded_copy_time_series_table(
            source,
            self.__is_data_folder,
            source_table_name,
            target,
            target_table_name,
            start_time_ptr,
            end_time_ptr,
            tags_ffi.array_ptr,
            tags_ffi.schema_ptr,
        )
        self.__check_return_code_and_raise_error(return_code)

    def __get_timestamp(self, timestamp: None | str | datetime) -> str:
        """Return `timestamp` if it is `None` or `str`, if `datetime` return it
        as an ISO 8601 `str`."""
        if type(timestamp) is datetime:
            return timestamp.isoformat()
        return timestamp

    def __str_to_c_char_ptr(self, string: None | str):
        """Return `string` as a char pointer if it is not `None` or the empty `str`,
        otherwise return the null pointer."""
        if string:
            return ffi.new("char[]", bytes(string, "UTF-8"))
        else:
            return ffi.NULL

    def move(
            self,
            source_table_name: str,
            target: Self,
            target_table_name: str,
    ):
        """Moves all data from the table with `source_table_name` in `self`
        to the table with `target_table_name` in `target`.

        :param source_table_name: Name of the table to read data from.
        :type source_table_name: str
        :param target: :obj:`Operations` to write data from `self` to.
        :type target: Operations
        :param target_table_name: Name of the table to write data to.
        :type target_table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        source = self.__operations_ptr
        source_table_name = ffi.new("char[]", bytes(source_table_name, "UTF-8"))

        target = target.__operations_ptr
        target_table_name = ffi.new("char[]", bytes(target_table_name, "UTF-8"))

        return_code = self.__library.modelardb_embedded_move(
            source,
            self.__is_data_folder,
            source_table_name,
            target,
            target_table_name,
        )
        self.__check_return_code_and_raise_error(return_code)

    def truncate(self, table_name: str):
        """Truncates the table with `table_name`.

        :param table_name: The name of the table to truncate.
        :type table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))
        return_code = self.__library.modelardb_embedded_truncate(
            self.__operations_ptr, self.__is_data_folder, table_name_ptr
        )
        self.__check_return_code_and_raise_error(return_code)

    def drop(self, table_name: str):
        """Drops the table with `table_name`.

        :param table_name: The name of the table to drop.
        :type table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))
        return_code = self.__library.modelardb_embedded_drop(
            self.__operations_ptr, self.__is_data_folder, table_name_ptr
        )
        self.__check_return_code_and_raise_error(return_code)

    def vacuum(self, table_name: str):
        """Vacuum the table with `table_name`.

        :param table_name: The name of the table to vacuum.
        :type table_name: str
        :raises ValueError: If incorrect arguments are provided.
        """
        table_name_ptr = ffi.new("char[]", bytes(table_name, "UTF-8"))
        return_code = self.__library.modelardb_embedded_vacuum(
            self.__operations_ptr, self.__is_data_folder, table_name_ptr
        )
        self.__check_return_code_and_raise_error(return_code)

    def __check_return_code_and_raise_error(self, return_code: int):
        """Raises an appropriate exception based on the return code.

        param return_code: The return code returned by a Rust function.
        :type return_code: int
        :raises RuntimeError or ValueError: If `return_code` is not 0.
        """
        match return_code:
            case self.__library.RETURN_SUCCESS:
                pass  # No errors.
            case self.__library.RETURN_FAILURE:
                raise RuntimeError(
                    ffi.string(self.__library.modelardb_embedded_error()).decode(
                        "UTF-8"
                    )
                )
            case _:
                raise ValueError("Unknown return code.")


def open_memory() -> Operations:
    """Create an :obj:`Operations` data folder that manages data in memory.

    :return: The constructed :obj:`Operations`.
    :rtype: Operations
    """
    return Operations.open_memory()


def open_local(data_folder_path: str) -> Operations:
    """Create an :obj:`Operations` data folder that manages data in the local folder at `data_folder_path`.

    :param data_folder_path: The path of the data folder.
    :type data_folder_path: str
    :return: The constructed :obj:`Operations`.
    :rtype: Operations
    """
    return Operations.open_local(data_folder_path)


def open_s3(
        endpoint: str, bucket_name: str, access_key_id: str, secret_access_key: str
) -> Operations:
    """Create an :obj:`Operations` data folder that manages data in an object store with a S3-compatible API.

    :param endpoint: The endpoint of the S3-compatible object store.
    :type endpoint: str
    :param bucket_name: The name of the bucket to read data from and write data to.
    :type bucket_name: str
    :param access_key_id: The access key id to use for authentication.
    :type access_key_id: str
    :param secret_access_key: The secret access key to use for authentication.
    :type secret_access_key: str
    :return: The constructed :obj:`Operations`.
    :rtype: Operations
    """
    return Operations.open_s3(endpoint, bucket_name, access_key_id, secret_access_key)


def open_azure(account_name: str, access_key: str, container_name: str) -> Operations:
    """Create an :obj:`Operations` data folder that manages data in an object store with an Azure-compatible API.

    :param account_name: The account name to use for authentication.
    :type account_name: str
    :param access_key: The secret access key to use for authentication.
    :type access_key: str
    :param container_name: The name of the container to read data from and write data to.
    :type container_name: str
    :return: The constructed :obj:`Operations`.
    :rtype: Operations
    """
    return Operations.open_azure(account_name, access_key, container_name)


def connect(node: Server | Manager) -> Operations:
    """Create a connection to an :obj:`Operations` node.

    :param node: The ModelarDB node to connect to.
    :type node: Server | Manager
    """
    return Operations.connect(node)
