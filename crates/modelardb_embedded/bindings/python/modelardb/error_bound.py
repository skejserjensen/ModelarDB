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

import math
from dataclasses import dataclass


@dataclass
class AbsoluteErrorBound:
    """A per value absolute error bound.

    param value: A positive finite value that specifies the error bound.
    :type value: float
    :raises ValueError: If `value` is not a positive normal value.
    """

    def __init__(self, value: float):
        # Error checking is required as the C-API depends on the error bounds being valid.
        if not math.isfinite(value) or value < 0:
            raise ValueError("An absolute error bound must be a positive finite value.")
        self.value = value


@dataclass
class RelativeErrorBound:
    """A per value relative error bound.

    param value: A value between 0 and 100 that specifies the error bound.
    :type value: float
    :raises ValueError: If `value` is not between 0 and 100.
    """

    def __init__(self, value: float):
        # Error checking is required as the C-API depends on the error bounds being valid.
        if not 0 <= value <= 100:
            raise ValueError(
                "A relative error bound must be a value from 0.0% to 100.0%."
            )
        self.value = value
