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

from modelardb import AbsoluteErrorBound, RelativeErrorBound


class TestErrorBound(unittest.TestCase):
    # Tests for AbsoluteErrorBound.
    def test_can_create_absolute_error_bound_with_float_zero(self):
        _ = AbsoluteErrorBound(0)

    def test_can_create_absolute_error_bound_with_normal_positive_float(self):
        _ = AbsoluteErrorBound(1)

    def test_cannot_create_absolute_error_bound_with_normal_negative_float(self):
        self.assertRaises(ValueError, lambda: AbsoluteErrorBound(-1))

    def test_cannot_create_absolute_error_bound_with_negative_infinity(self):
        self.assertRaises(ValueError, lambda: AbsoluteErrorBound(float("-inf")))

    def test_cannot_create_absolute_error_bound_with_positive_infinity(self):
        self.assertRaises(ValueError, lambda: AbsoluteErrorBound(float("inf")))

    def test_cannot_create_absolute_error_bound_with_nan(self):
        self.assertRaises(ValueError, lambda: AbsoluteErrorBound(float("nan")))

    # Tests for RelativeErrorBound.
    def test_can_create_relative_error_bound_with_float_zero(self):
        _ = RelativeErrorBound(0)

    def test_can_create_relative_error_bound_with_normal_positive_float(self):
        _ = RelativeErrorBound(1)

    def test_cannot_create_relative_error_bound_with_normal_negative_float(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(-1))

    def test_cannot_create_relative_error_bound_with_float_above_one_hundred(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(101))

    def test_cannot_create_relative_error_bound_with_negative_infinity(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(float("-inf")))

    def test_cannot_create_relative_error_bound_with_positive_infinity(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(float("inf")))

    def test_cannot_create_relative_error_bound_with_nan(self):
        self.assertRaises(ValueError, lambda: RelativeErrorBound(float("nan")))
