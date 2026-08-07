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

from dataclasses import dataclass

from pyarrow import Schema

from .error_bound import AbsoluteErrorBound, RelativeErrorBound


@dataclass
class NormalTable:
    """A relational table storing any combination of columns.

    :param schema: The schema of the normal table.
    :type schema: Schema
    """

    def __init__(self, schema: Schema):
        self.schema: Schema = schema


@dataclass
class TimeSeriesTable:
    """A relational table storing time series with metadata more efficiently
    than :class:`Table` by storing the time series with metadata as models.

    :param schema: The schema of the time series table. It must contain a timestamp
    column with the type :class:`pyarrow.timestamp("us")`, one or more field columns
    with the type :class:`pyarrow.float32()`, and zero or more tag columns with the
    type :class:`pyarrow.string()`.
    :type schema: Schema
    :param error_bounds: Absolute or relative error bounds for the field columns
    with type :class:`pyarrow.float32()` in `schema`. If no error bound is
    specified for a column, it will be zero so the values will be stored
    losslessly.
    :type error_bounds: dict[str, AbsoluteErrorBound | RelativeErrorBound],
    optional
    :param generated_columns: SQL expressions for generating field columns of
    type :class:`pyarrow.float32()` in `schema`. If no generated column is
    specified for a field column, it will be stored.
    :type generated_columns: dict[str, str], optional
    """

    def __init__(
        self,
        schema: Schema,
        error_bounds: None | dict[str, AbsoluteErrorBound | RelativeErrorBound] = None,
        generated_columns: None | dict[str, str] = None,
    ):
        self.schema = schema
        self.error_bounds = {} if error_bounds is None else error_bounds
        self.__check_if_column_exists(self.error_bounds.keys(), schema)
        self.generated_columns = {} if generated_columns is None else generated_columns
        self.__check_if_column_exists(self.generated_columns.keys(), schema)

    def __check_if_column_exists(self, columns: list[str], schema: Schema):
        """Raise an error if a column in :attr:`columns` does not exist in
        :attr:`schema`


        :raises ValueError: If a column does not exist.
        """
        for column in columns:
            if schema.get_field_index(column) == -1:
                raise ValueError(f"The column {column} does not exist.")
