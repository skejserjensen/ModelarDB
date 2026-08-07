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

import datetime
import os
import unittest
from tempfile import TemporaryDirectory

import pyarrow
from modelardb import (
    Aggregate,
    AbsoluteErrorBound,
    TimeSeriesTable,
    NormalTable,
    Operations,
    RelativeErrorBound,
)
from pyarrow import RecordBatch, Schema

NORMAL_TABLE_NAME = "normal_table"
TIME_SERIES_TABLE_NAME = "time_series_table"
MISSING_TABLE_NAME = "missing_table"


class TestOperations(unittest.TestCase):
    def test_data_folder_init(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            new_data_folder = Operations.open_local(temp_dir)
            self.assertEqual(data_folder.tables(), new_data_folder.tables())

    def test_data_folder_create_normal_table(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            table_folder = os.path.join(temp_dir, "tables", NORMAL_TABLE_NAME)
            self.assertFalse(os.path.exists(table_folder))

            data_folder.create(NORMAL_TABLE_NAME, NormalTable(normal_table_schema()))

            self.assertTrue(os.path.exists(table_folder))
            self.assertEqual(len(os.listdir(table_folder)), 1)

    def test_data_folder_create_time_series_table(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            expected_schema = time_series_table_query_schema()
            time_series_table_type = TimeSeriesTable(
                expected_schema,
                {
                    "field_one": AbsoluteErrorBound(1),
                    "field_two": RelativeErrorBound(10),
                },
                {"field_three": "field_one + field_two"},
            )

            time_series_table_folder = os.path.join(temp_dir, "tables", TIME_SERIES_TABLE_NAME)
            self.assertFalse(os.path.exists(time_series_table_folder))

            data_folder.create(TIME_SERIES_TABLE_NAME, time_series_table_type)

            self.assertTrue(os.path.exists(time_series_table_folder))
            self.assertEqual(len(os.listdir(time_series_table_folder)), 1)

    def test_data_folder_create_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.create(NORMAL_TABLE_NAME, NormalTable(pyarrow.schema([])))

            error_message = (
                "ModelarDB Storage Error: Delta Lake Error: Generic error: At least one column must be "
                "defined to create a table."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_tables(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            self.assertEqual(
                data_folder.tables(), [NORMAL_TABLE_NAME, TIME_SERIES_TABLE_NAME]
            )

    def test_data_folder_schema(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            actual_normal_table_schema = data_folder.schema(NORMAL_TABLE_NAME)
            self.assertEqual(actual_normal_table_schema, normal_table_schema())

            actual_time_series_table_schema = data_folder.schema(TIME_SERIES_TABLE_NAME)
            self.assertEqual(actual_time_series_table_schema, time_series_table_query_schema())

    def test_data_folder_schema_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.schema(MISSING_TABLE_NAME)

            error_message = (
                f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a table."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_write(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            time_series_table_folder = os.path.join(temp_dir, "tables", TIME_SERIES_TABLE_NAME)
            self.assertEqual(len(os.listdir(time_series_table_folder)), 1)

            data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            self.assertEqual(len(os.listdir(time_series_table_folder)), 3)

    def test_data_folder_write_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.write(MISSING_TABLE_NAME, time_series_table_data())

            error_message = (
                f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a table."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_read(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())

            actual_result = data_folder.read(f"SELECT * FROM {TIME_SERIES_TABLE_NAME}")
            self.assertEqual(
                actual_result, sorted_time_series_table_data_with_generated_column()
            )

    def test_data_folder_read_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.read(f"SELECT * FROM {MISSING_TABLE_NAME}")

            error_message = (
                f"DataFusion Error: Error during planning: table 'datafusion.public.{MISSING_TABLE_NAME}' "
                f"not found"
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_copy(self):
        with TemporaryDirectory() as source_temp_dir:
            with TemporaryDirectory() as target_temp_dir:
                source_data_folder = Operations.open_local(source_temp_dir)
                create_tables_in_data_folder(source_data_folder)

                expected_result = normal_table_data()
                source_data_folder.write(NORMAL_TABLE_NAME, expected_result)

                target_data_folder = Operations.open_local(target_temp_dir)
                create_tables_in_data_folder(target_data_folder)

                sql = f"SELECT * FROM {NORMAL_TABLE_NAME}"
                source_data_folder.copy(
                    sql, target_data_folder, NORMAL_TABLE_NAME
                )

                # After copying the data it should also be in target_table.
                self.assertEqual(source_data_folder.read(sql), expected_result)
                self.assertEqual(target_data_folder.read(sql), expected_result)

    def test_data_folder_copy_error(self):
        with TemporaryDirectory() as source_temp_dir:
            with TemporaryDirectory() as target_temp_dir:
                source_data_folder = Operations.open_local(source_temp_dir)
                target_data_folder = Operations.open_local(target_temp_dir)

                with self.assertRaises(RuntimeError) as context:
                    sql = f"SELECT * FROM {MISSING_TABLE_NAME}"
                    source_data_folder.copy(
                        sql, target_data_folder, MISSING_TABLE_NAME
                    )

                error_message = f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a normal table."
                self.assertEqual(error_message, str(context.exception))

    def test_data_folder_read_time_series_table(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())

            actual_result = data_folder.read_time_series_table(TIME_SERIES_TABLE_NAME)
            self.assertEqual(
                actual_result, sorted_time_series_table_data_with_generated_column()
            )

    def test_data_folder_read_time_series_table_with_columns_tags_timestamps(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())

            end_time = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
            actual_result = data_folder.read_time_series_table(
                TIME_SERIES_TABLE_NAME,
                columns=["tag", "field_one"],
                tags={"tag": "tag_one"},
                start_time="1970-01-01T00:00:00.000150",
                end_time=end_time + datetime.timedelta(microseconds=250),
            )
            expected_result = sorted_time_series_table_data_with_generated_column()
            self.assertEqual(
                actual_result, expected_result.select(["tag", "field_one"]).slice(1, 1)
            )

    def test_data_folder_read_time_series_table_with_aggregates(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())

            columns = [
                ("tag", Aggregate.NONE),
                ("field_one", Aggregate.SUM),
            ]
            actual_result = data_folder.read_time_series_table(
                TIME_SERIES_TABLE_NAME, columns, ["tag"]
            )

            expected_result = [
                {"tag": "tag_one", f"sum({TIME_SERIES_TABLE_NAME}.field_one)": 111.0},
                {"tag": "tag_two", f"sum({TIME_SERIES_TABLE_NAME}.field_one)": 219.0},
            ]
            self.assertEqual(
                sorted(actual_result.to_pylist(), key=lambda x: x["tag"]),
                expected_result,
            )

    def test_data_folder_read_time_series_table_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.read_time_series_table(MISSING_TABLE_NAME)

            error_message = (
                f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a time series table."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_copy_time_series_table(self):
        with TemporaryDirectory() as source_temp_dir:
            with TemporaryDirectory() as target_temp_dir:
                source_data_folder = Operations.open_local(source_temp_dir)
                create_tables_in_data_folder(source_data_folder)

                source_data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())

                target_data_folder = Operations.open_local(target_temp_dir)
                create_tables_in_data_folder(target_data_folder)

                source_data_folder.copy_time_series_table(
                    TIME_SERIES_TABLE_NAME, target_data_folder, TIME_SERIES_TABLE_NAME
                )

                # After copying the data it should also be in target_table.
                expected_result = sorted_time_series_table_data_with_generated_column()
                self.assertEqual(
                    source_data_folder.read_time_series_table(TIME_SERIES_TABLE_NAME), expected_result
                )
                self.assertEqual(
                    target_data_folder.read_time_series_table(TIME_SERIES_TABLE_NAME), expected_result
                )

    def test_data_folder_copy_time_series_table_with_timestamps(self):
        with TemporaryDirectory() as source_temp_dir:
            with TemporaryDirectory() as target_temp_dir:
                source_data_folder = Operations.open_local(source_temp_dir)
                create_tables_in_data_folder(source_data_folder)

                # Force the physical data to have multiple segments by writing the data in three parts.
                for i in range(3):
                    source_data_folder.write(
                        TIME_SERIES_TABLE_NAME, time_series_table_data().slice(i * 2, 2)
                    )

                target_data_folder = Operations.open_local(target_temp_dir)
                create_tables_in_data_folder(target_data_folder)

                end_time = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
                source_data_folder.copy_time_series_table(
                    TIME_SERIES_TABLE_NAME,
                    target_data_folder,
                    TIME_SERIES_TABLE_NAME,
                    start_time="1970-01-01T00:00:00.000150",
                    end_time=end_time + datetime.timedelta(microseconds=250),
                )

                # After copying the data it should also be in target_table.
                source_expected_result = time_series_table_data_with_generated_column()
                self.assertEqual(
                    source_data_folder.read_time_series_table(TIME_SERIES_TABLE_NAME),
                    source_expected_result,
                )

                target_expected_result = source_expected_result.slice(2, 2)
                self.assertEqual(
                    target_data_folder.read_time_series_table(TIME_SERIES_TABLE_NAME),
                    target_expected_result,
                )

    def test_data_folder_copy_time_series_table_error(self):
        with TemporaryDirectory() as source_temp_dir:
            with TemporaryDirectory() as target_temp_dir:
                source_data_folder = Operations.open_local(source_temp_dir)
                target_data_folder = Operations.open_local(target_temp_dir)

                with self.assertRaises(RuntimeError) as context:
                    source_data_folder.copy_time_series_table(
                        MISSING_TABLE_NAME, target_data_folder, MISSING_TABLE_NAME
                    )

                error_message = f"Invalid Argument Error: {MISSING_TABLE_NAME} is not a time series table."
                self.assertEqual(error_message, str(context.exception))

    def test_data_folder_move(self):
        with TemporaryDirectory() as source_temp_dir:
            with TemporaryDirectory() as target_temp_dir:
                source_data_folder = Operations.open_local(source_temp_dir)
                create_tables_in_data_folder(source_data_folder)

                source_data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())

                target_data_folder = Operations.open_local(target_temp_dir)
                create_tables_in_data_folder(target_data_folder)

                source_data_folder.move(
                    TIME_SERIES_TABLE_NAME, target_data_folder, TIME_SERIES_TABLE_NAME
                )

                self.assertEqual(
                    source_data_folder.read_time_series_table(TIME_SERIES_TABLE_NAME).num_rows, 0
                )

                expected_result = sorted_time_series_table_data_with_generated_column()
                self.assertEqual(
                    target_data_folder.read_time_series_table(TIME_SERIES_TABLE_NAME), expected_result
                )

    def test_data_folder_move_error(self):
        with TemporaryDirectory() as source_temp_dir:
            with TemporaryDirectory() as target_temp_dir:
                source_data_folder = Operations.open_local(source_temp_dir)
                target_data_folder = Operations.open_local(target_temp_dir)

                with self.assertRaises(RuntimeError) as context:
                    source_data_folder.move(
                        MISSING_TABLE_NAME, target_data_folder, MISSING_TABLE_NAME
                    )

                error_message = (
                    f"Invalid Argument Error: {MISSING_TABLE_NAME} and {MISSING_TABLE_NAME} are not both "
                    f"normal tables or time series tables."
                )
                self.assertEqual(error_message, str(context.exception))

    def test_data_folder_truncate(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())

            data_folder.truncate(TIME_SERIES_TABLE_NAME)

            self.assertTrue(TIME_SERIES_TABLE_NAME in data_folder.tables())
            self.assertEqual(data_folder.read_time_series_table(TIME_SERIES_TABLE_NAME).num_rows, 0)

    def test_data_folder_truncate_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.truncate(MISSING_TABLE_NAME)

            error_message = f"Invalid Argument Error: Table with name '{MISSING_TABLE_NAME}' does not exist."
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_drop(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)
            self.assertTrue(TIME_SERIES_TABLE_NAME in data_folder.tables())

            data_folder.drop(TIME_SERIES_TABLE_NAME)

            self.assertFalse(
                os.path.exists(os.path.join(temp_dir, "tables", TIME_SERIES_TABLE_NAME))
            )
            self.assertFalse(TIME_SERIES_TABLE_NAME in data_folder.tables())

    def test_data_folder_drop_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.drop(MISSING_TABLE_NAME)

            error_message = (
                f"ModelarDB Storage Error: Invalid Argument Error: Table with name '{MISSING_TABLE_NAME}' "
                f"does not exist."
            )
            self.assertEqual(error_message, str(context.exception))

    def test_data_folder_vacuum(self):
        with TemporaryDirectory() as temp_dir:
            os.environ["MODELARDBD_RETENTION_PERIOD_IN_SECONDS"] = "0"

            data_folder = Operations.open_local(temp_dir)
            create_tables_in_data_folder(data_folder)

            data_folder.write(TIME_SERIES_TABLE_NAME, time_series_table_data())
            data_folder.truncate(TIME_SERIES_TABLE_NAME)

            # The files should still exist on disk even though they are no longer active.
            folder_path = os.path.join(temp_dir, "tables", TIME_SERIES_TABLE_NAME, "field_column=2")
            file_count = len(os.listdir(folder_path))
            self.assertEqual(file_count, 1)

            data_folder.vacuum(TIME_SERIES_TABLE_NAME)

            # No files should remain in the column folder.
            file_count = len(os.listdir(folder_path))
            self.assertEqual(file_count, 0)

    def test_data_folder_vacuum_error(self):
        with TemporaryDirectory() as temp_dir:
            data_folder = Operations.open_local(temp_dir)

            with self.assertRaises(RuntimeError) as context:
                data_folder.vacuum(MISSING_TABLE_NAME)

            error_message = f"Invalid Argument Error: Table with name '{MISSING_TABLE_NAME}' does not exist."
            self.assertEqual(error_message, str(context.exception))


def create_tables_in_data_folder(data_folder: Operations):
    table_type = NormalTable(normal_table_schema())
    data_folder.create(NORMAL_TABLE_NAME, table_type)

    time_series_table_type = TimeSeriesTable(
        time_series_table_query_schema(),
        {
            "field_one": AbsoluteErrorBound(1),
            "field_two": RelativeErrorBound(10),
        },
        {"field_three": "field_one + field_two"},
    )
    data_folder.create(TIME_SERIES_TABLE_NAME, time_series_table_type)


def normal_table_schema() -> Schema:
    return pyarrow.schema(
        [
            ("id", pyarrow.int32()),
            ("name", pyarrow.string()),
            ("age", pyarrow.int8()),
            ("height", pyarrow.int16()),
            ("weight", pyarrow.int16()),
        ]
    )


def normal_table_data() -> RecordBatch:
    schema = normal_table_schema()
    return pyarrow.RecordBatch.from_pylist(
        [{"id": 1, "name": "Mister Smith", "age": 25, "height": 193, "weight": 95}],
        schema,
    )


def time_series_table_schema() -> Schema:
    return pyarrow.schema(
        [
            ("timestamp", pyarrow.timestamp("us")),
            ("tag", pyarrow.string()),
            ("field_one", pyarrow.float32()),
            ("field_two", pyarrow.float32()),
        ]
    )


def time_series_table_query_schema() -> Schema:
    return pyarrow.schema(
        [
            ("timestamp", pyarrow.timestamp("us")),
            ("tag", pyarrow.string()),
            ("field_one", pyarrow.float32()),
            ("field_two", pyarrow.float32()),
            ("field_three", pyarrow.float32()),
        ]
    )


def sorted_time_series_table_data_with_generated_column() -> RecordBatch:
    unsorted_data = time_series_table_data_with_generated_column()
    return unsorted_data.sort_by([("tag", "ascending"), ("timestamp", "ascending")])


def time_series_table_data_with_generated_column() -> RecordBatch:
    data = time_series_table_data()

    generated_column_array = pyarrow.array(
        [74, 146, 74, 146, 74, 146], pyarrow.float32()
    )
    return data.append_column("field_three", generated_column_array)


def time_series_table_data() -> RecordBatch:
    return pyarrow.RecordBatch.from_pylist(
        [
            {
                "timestamp": 100,
                "field_one": 37.0,
                "field_two": 37.0,
                "tag": "tag_one",
            },
            {
                "timestamp": 100,
                "field_one": 73.0,
                "field_two": 73.0,
                "tag": "tag_two",
            },
            {
                "timestamp": 200,
                "field_one": 37.0,
                "field_two": 37.0,
                "tag": "tag_one",
            },
            {
                "timestamp": 200,
                "field_one": 73.0,
                "field_two": 73.0,
                "tag": "tag_two",
            },
            {
                "timestamp": 300,
                "field_one": 37.0,
                "field_two": 37.0,
                "tag": "tag_one",
            },
            {
                "timestamp": 300,
                "field_one": 73.0,
                "field_two": 73.0,
                "tag": "tag_two",
            },
        ],
        time_series_table_schema(),
    )
