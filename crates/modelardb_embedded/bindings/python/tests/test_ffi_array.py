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

import unittest

import pyarrow
from modelardb import FFIArray
from pyarrow import Int64Array
from pyarrow.cffi import ffi


class TestFFIArray(unittest.TestCase):
    def test_can_create_ffi_array_from_array(self):
        array = pyarrow.array([1, 2, 3, 4, 5])
        array_ffi = FFIArray.from_array(array)
        self.assertEqual(array, array_ffi.array())

    def test_can_create_ffi_array_from_array_type_array_ptr_and_schema_ptr(self):
        array = pyarrow.array([1, 2, 3, 4, 5])
        array_ptr = ffi.new("struct ArrowArray*")
        schema_ptr = ffi.new("struct ArrowSchema*")
        array_ptr_int = int(ffi.cast("uintptr_t", array_ptr))
        schema_ptr_int = int(ffi.cast("uintptr_t", schema_ptr))
        array._export_to_c(array_ptr_int, schema_ptr_int)

        array_ffi = FFIArray.from_ffi(
            Int64Array, array_ptr=array_ptr, schema_ptr=schema_ptr
        )

        self.assertEqual(array, array_ffi.array())

    def test_can_create_ffi_array_from_array_type(self):
        _ = FFIArray.from_type(Int64Array)
