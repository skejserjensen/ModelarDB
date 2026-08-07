"""Python bindings for the C-API of ModelarDB Embedded."""

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

from .operations import Aggregate, Operations, open_memory, open_local, open_s3, open_azure, connect
from .node import Server, Manager
from .error_bound import AbsoluteErrorBound, RelativeErrorBound
from .table import NormalTable, TimeSeriesTable
from .ffi_array import FFIArray
