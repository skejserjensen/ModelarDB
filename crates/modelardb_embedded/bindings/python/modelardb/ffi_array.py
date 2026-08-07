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

from pyarrow import Array, RecordBatch
from pyarrow.cffi import ffi


class FFIArray:
    """An Apache Arrow array that can be accessed as both a PyArrow
    :class:`Array` and as two pointers to the data and schema using
    Apache Arrow's C Data Interface.
    """

    @classmethod
    def from_array(cls, array: Array | RecordBatch):
        """Create a :class:`FFIArray` from an existing PyArrow :class:`Array`
        or :class:`RecordBatch`.

        :param array: PyArrow :class:`Array` or :class:`RecordBatch` to convert
        to pointers using Apache Arrow's C Data Interface.
        :type array: Array | RecordBatch
        """
        # PyArrow Array to C Data Interface pointers to array and schema.
        self: FFIArray = cls()

        self.__array = array
        self.array_type = type(self.__array)
        self.array_ptr = ffi.new("struct ArrowArray*")
        self.schema_ptr = ffi.new("struct ArrowSchema*")
        self.array_ptr_int = int(ffi.cast("uintptr_t", self.array_ptr))
        self.schema_ptr_int = int(ffi.cast("uintptr_t", self.schema_ptr))
        self.__array._export_to_c(self.array_ptr_int, self.schema_ptr_int)

        return self

    @classmethod
    def from_ffi(cls, array_type: type, array_ptr, schema_ptr):
        """Create a :class:`FFIArray` from a pointer to an Apache Arrow array
        and schema using Apache Arrow's C Data Interface.

        :param array_type: Type of PyArrow :class:`Array` or
        :class:`RecordBatch` in :attr:`array_ptr`.
        :type array_type: type
        :param array_ptr: Pointer to Apache Arrow C Data Interface ArrowArray to
        convert from.
        :type array_ptr: cdata 'struct ArrowArray*'
        :param schema_ptr: Pointer to Apache Arrow C Data Interface ArrowSchema to
        convert from.
        :type schema_ptr: cdata 'struct ArrowSchema*'
        """
        # C Data Interface pointers to array and schema to PyArrow Array.
        self: FFIArray = cls()

        self.__array = None
        self.array_type = array_type
        self.array_ptr = array_ptr
        self.schema_ptr = schema_ptr
        self.array_ptr_int = int(ffi.cast("uintptr_t", self.array_ptr))
        self.schema_ptr_int = int(ffi.cast("uintptr_t", self.schema_ptr))
        self.__array = self.array_type._import_from_c(
            self.array_ptr_int, self.schema_ptr_int
        )

        return self

    @classmethod
    def from_type(cls, array_type: type):
        """Create a :class:`FFIArray` for a specific PyArrow :class:`Array` or
        :class:`RecordBatch` that a matching Apache Arrow array and schema using
        Apache Arrow's C Data Interface can be written to.

        :param array_type: Type of PyArrow :class:`Array` or
        :class:`RecordBatch`.
        :type array_type: type
        """
        # Create C Data Interface pointers to array and schema to be converted
        # to a PyArrow Array. Designed to simplify returning Arrays from Rust.
        self: FFIArray = cls()

        self.__array = None
        self.array_type = array_type
        self.array_ptr = ffi.new("struct ArrowArray*")
        self.schema_ptr = ffi.new("struct ArrowSchema*")
        self.array_ptr_int = int(ffi.cast("uintptr_t", self.array_ptr))
        self.schema_ptr_int = int(ffi.cast("uintptr_t", self.schema_ptr))

        return self

    def array(self) -> Array | RecordBatch:
        """Returns the contents of :attr:`array_ptr` as a :attr:`array_type`
        with the schema in :attr:`schema_ptr`.
        """
        # If FFIArray was constructed by passing only array_type, no data was
        # available when it was created, so the conversion must be done now.
        if self.__array is None:
            self.__array = self.array_type._import_from_c(
                self.array_ptr_int, self.schema_ptr_int
            )
        return self.__array
