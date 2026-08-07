/* Copyright 2023 The ModelarDB Contributors
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

//! The types used throughout the crate.

use std::sync::Arc;
use std::{debug_assert, iter};

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, Float32Builder, Int8Builder, Int16Array, StringArray,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use modelardb_types::types::{
    ErrorBound, Timestamp, TimestampArray, TimestampBuilder, Value, ValueArray, ValueBuilder,
};

use crate::models::gorilla::Gorilla;
use crate::models::pmc_mean::PMCMean;
use crate::models::swing::Swing;
use crate::models::{PMC_MEAN_ID, SWING_ID};
use crate::models::{VALUE_SIZE_IN_BYTES, timestamps};

/// A model being built from an uncompressed segment using the potentially lossy model types in
/// [`models`]. Each of the potentially lossy model types is used to fit models to the data points,
/// and then the model that uses the fewest number of bytes per value is selected.
pub(crate) struct ModelBuilder {
    /// Index of the first data point in `uncompressed_timestamps` and `uncompressed_values` the
    /// compressed model represents values for.
    start_index: usize,
    /// Constant function that currently represents the values in `uncompressed_values` from
    /// `start_index` to `start_index` + `pmc_mean.length`.
    pmc_mean: PMCMean,
    /// Indicates if `pmc_mean` could represent all values in `uncompressed_values` from
    /// `start_index` to `current_index` in `new()`.
    pmc_mean_could_fit_all: bool,
    /// Linear function that represents the values in `uncompressed_values` from `start_index` to
    /// `start_index` + `swing.length`.
    swing: Swing,
    /// Indicates if `swing` could represent all values in `uncompressed_values` from `start_index`
    /// to `current_index` in `new()`.
    swing_could_fit_all: bool,
}

impl ModelBuilder {
    /// Create a model that represents a sub-sequence of uncompressed values that starts at
    /// `start_index`, is within `error_bound`, and uses the fewest number of bytes per value.
    pub(crate) fn new(start_index: usize, error_bound: ErrorBound) -> Self {
        Self {
            start_index,
            pmc_mean: PMCMean::new(error_bound),
            pmc_mean_could_fit_all: true,
            swing: Swing::new(error_bound),
            swing_could_fit_all: true,
        }
    }

    /// Attempt to update the current models to also represent the `value` of the data point
    /// collected at `timestamp`. Returns [`true`] if any of the current models could represent
    /// `value`, otherwise [`false`].
    pub(crate) fn try_to_update_models(&mut self, timestamp: Timestamp, value: Value) -> bool {
        self.pmc_mean_could_fit_all = self.pmc_mean_could_fit_all && self.pmc_mean.fit_value(value);

        self.swing_could_fit_all =
            self.swing_could_fit_all && self.swing.fit_data_point(timestamp, value);

        self.pmc_mean_could_fit_all || self.swing_could_fit_all
    }

    /// Return the model that requires the fewest number of bytes per value.
    pub(crate) fn finish(self) -> CompressedSegmentBuilder {
        let bytes_per_value = [
            (PMC_MEAN_ID, self.pmc_mean.bytes_per_value()),
            (SWING_ID, self.swing.bytes_per_value()),
        ];

        // unwrap() cannot fail as the array is not empty and there are no NaN values.
        let model_type_id = bytes_per_value
            .iter()
            .min_by(|x, y| f32::partial_cmp(&x.1, &y.1).unwrap())
            .unwrap()
            .0;

        match model_type_id {
            PMC_MEAN_ID => Self::select_pmc_mean(self.start_index, self.pmc_mean),
            SWING_ID => Self::select_swing(self.start_index, self.swing),
            _ => panic!("Unknown model type."),
        }
    }

    /// Return a [`CompressedSegmentBuilder`] containing the model fitted by [`PMCMean`].
    fn select_pmc_mean(start_index: usize, pmc_mean: PMCMean) -> CompressedSegmentBuilder {
        let end_index = start_index + pmc_mean.len() - 1;
        let bytes_per_value = pmc_mean.bytes_per_value();
        let value = pmc_mean.model();

        CompressedSegmentBuilder::new(
            PMC_MEAN_ID,
            start_index,
            end_index,
            value,
            value,
            vec![],
            value,
            bytes_per_value,
        )
    }

    /// Return a [`CompressedSegmentBuilder`] containing the model fitted by [`Swing`].
    fn select_swing(start_index: usize, swing: Swing) -> CompressedSegmentBuilder {
        let end_index = start_index + swing.len() - 1;
        let bytes_per_value = swing.bytes_per_value();
        let (first_value, last_value) = swing.model();
        let min_value = Value::min(first_value, last_value);
        let max_value = Value::max(first_value, last_value);
        let values = if first_value < last_value {
            vec![]
        } else {
            vec![0]
        };

        CompressedSegmentBuilder::new(
            SWING_ID,
            start_index,
            end_index,
            min_value,
            max_value,
            values,
            last_value,
            bytes_per_value,
        )
    }
}

/// A compressed segment being built from metadata and a model.
pub(crate) struct CompressedSegmentBuilder {
    /// Id of the model type that created the model in this segment.
    pub model_type_id: i8,
    /// Index of the first data point in the `UncompressedDataBuffer` that this segment represents.
    pub start_index: usize,
    /// Index of the last data point in the `UncompressedDataBuffer` that this segment represents.
    pub end_index: usize,
    /// The segment's minimum value.
    pub min_value: Value,
    /// The segment's maximum value.
    pub max_value: Value,
    /// Data required in addition to `min` and `max` for the model to
    /// reconstruct the values it represents when given a specific timestamp.
    pub values: Vec<u8>,
    /// The last value the model represents.
    pub model_last_value: Value,
    /// The number of bytes per value used by the model.
    pub bytes_per_value: f32,
}

impl CompressedSegmentBuilder {
    fn new(
        model_type_id: i8,
        start_index: usize,
        end_index: usize,
        min_value: Value,
        max_value: Value,
        values: Vec<u8>,
        model_last_value: Value,
        bytes_per_value: f32,
    ) -> Self {
        Self {
            model_type_id,
            start_index,
            end_index,
            min_value,
            max_value,
            values,
            model_last_value,
            bytes_per_value,
        }
    }

    /// Create a compressed segment and add it to `compressed_segment_batch_builder`. The encoding
    /// used for the model's parameters may change if residuals are added to the segment as it
    /// changes the metadata stored in the segment. Assumes `uncompressed_timestamps` and
    /// `uncompressed_values` are of equal length and that `residuals_end_index` is the index of a
    /// value in `uncompressed_value` after the last value represented by the model in this segment.
    pub(crate) fn finish(
        mut self,
        error_bound: ErrorBound,
        residuals_end_index: usize,
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
        compressed_segment_batch_builder: &mut CompressedSegmentBatchBuilder,
    ) {
        // Assert that the methods assumptions are correct to simplify development.
        debug_assert_eq!(uncompressed_timestamps.len(), uncompressed_values.len());
        debug_assert!(self.end_index <= residuals_end_index);
        debug_assert!(residuals_end_index <= uncompressed_timestamps.len());

        let model_type_id = self.model_type_id;

        // Compress the timestamps for the values stored as the model and if any exist, residuals.
        let start_time = uncompressed_timestamps.value(self.start_index);
        let end_time = uncompressed_timestamps.value(residuals_end_index);
        let timestamps = timestamps::compress_residual_timestamps(
            &uncompressed_timestamps.values()[self.start_index..=residuals_end_index],
        );

        // Compress residual values using Gorilla if any exists.
        let residuals = if self.end_index < residuals_end_index {
            let residuals_start_index = self.end_index + 1;

            let uncompressed_residuals =
                &uncompressed_values.values()[residuals_start_index..=residuals_end_index];

            let (mut residuals, residuals_min_value, residuals_max_value) =
                self.compress_residuals(error_bound, uncompressed_residuals);

            self.values = match model_type_id {
                PMC_MEAN_ID => Self::encode_values_for_pmc_mean(
                    self.min_value,
                    self.max_value,
                    residuals_min_value,
                    residuals_max_value,
                ),
                SWING_ID => Self::encode_values_for_swing(
                    self.min_value,
                    self.max_value,
                    self.values.is_empty(),
                    residuals_min_value,
                    residuals_max_value,
                ),
                _ => panic!("Unknown model type."),
            };

            self.min_value = Value::min(self.min_value, residuals_min_value);
            self.max_value = Value::max(self.max_value, residuals_max_value);

            // The length is known to be at most RESIDUAL_VALUES_MAX_LENGTH: u8.
            residuals.push((residuals_end_index - residuals_start_index) as u8 + 1);
            residuals
        } else {
            vec![]
        };

        compressed_segment_batch_builder.append_compressed_segment(
            self.model_type_id,
            start_time,
            end_time,
            &timestamps,
            self.min_value,
            self.max_value,
            &self.values,
            &residuals,
            f32::NAN, // TODO: compute and store the actual error.
        )
    }

    /// Compress `uncompressed_residuals` within `error_bound` using [`Gorilla`].
    fn compress_residuals(
        &self,
        error_bound: ErrorBound,
        uncompressed_residuals: &[Value],
    ) -> (Vec<u8>, Value, Value) {
        let mut gorilla = Gorilla::new(error_bound);
        gorilla.compress_values_without_first(uncompressed_residuals, self.model_last_value);
        gorilla.model()
    }

    /// Encode the information required for a [`PMCMean`] model where the `residuals_min_value`
    /// and/or `residuals_max_value` overwrite the model's `min_value` and/or `max_value` in the
    /// segment.
    pub(crate) fn encode_values_for_pmc_mean(
        min_value: Value,
        max_value: Value,
        residuals_min_value: Value,
        residuals_max_value: Value,
    ) -> Vec<u8> {
        let mut values = vec![];

        if min_value > residuals_min_value {
            // The models minimum is overwritten so another value must be used.
            if max_value >= residuals_max_value {
                // Minimum and maximum is the same for PMC-Mean, so maximum can be used with a flag.
                values.push(1);
            } else {
                // Minimum and maximum have been overwritten, so the model's value has to be stored.
                values.extend_from_slice(&min_value.to_le_bytes());
            }
        }

        values
    }

    /// Decode the mean value stored for a model of type [`PMCMean`]. For information about how the
    /// parameter for [`PMCMean`] is encoded, see [`Self::encode_values_for_pmc_mean`].
    pub(crate) fn decode_values_for_pmc_mean(
        min_value: Value,
        max_value: Value,
        values: &[u8],
    ) -> Value {
        // unwrap() is safe as values are encoded by update_values_for_pmc_mean().
        match values.len() {
            0 => min_value,
            1 => max_value,
            _ => Value::from_le_bytes(values.try_into().unwrap()),
        }
    }

    /// Encode the information required for a [`Swing`] model where the `residuals_min_value` and/or
    /// `residuals_max_value` overwrite the model's `min_value` and/or `max_value` in the segment.
    pub(crate) fn encode_values_for_swing(
        min_value: Value,
        max_value: Value,
        min_value_is_first: bool,
        residuals_min_value: Value,
        residuals_max_value: Value,
    ) -> Vec<u8> {
        if residuals_min_value < min_value && max_value < residuals_max_value {
            // Minimum and maximum is overwritten so first and last value are stored.
            let mut values = Vec::with_capacity(2 * VALUE_SIZE_IN_BYTES as usize);
            if min_value_is_first {
                values.extend_from_slice(&min_value.to_le_bytes());
                values.extend_from_slice(&max_value.to_le_bytes());
            } else {
                values.extend_from_slice(&max_value.to_le_bytes());
                values.extend_from_slice(&min_value.to_le_bytes());
            }
            values
        } else if residuals_min_value < min_value {
            // Minimum is overwritten so a flag is stored for the order and then the models minimum.
            let mut values = Vec::with_capacity(1 + VALUE_SIZE_IN_BYTES as usize);
            if min_value_is_first {
                values.push(0);
                values.extend(min_value.to_le_bytes());
            } else {
                values.push(1);
                values.extend(min_value.to_le_bytes());
            }
            values
        } else if max_value < residuals_max_value {
            // Maximum is overwritten so a flag is stored for the order and then the models maximum.
            let mut values = Vec::with_capacity(1 + VALUE_SIZE_IN_BYTES as usize);
            if min_value_is_first {
                values.push(2);
                values.extend(max_value.to_le_bytes());
            } else {
                values.push(3);
                values.extend(max_value.to_le_bytes());
            }
            values
        } else if !min_value_is_first {
            vec![0]
        } else {
            vec![]
        }
    }

    /// Decode the first and last value stored for a model of type [`Swing`]. For information about
    /// how the parameters for Swing are encoded, see [`Self::update_values_for_swing`].
    pub(crate) fn decode_values_for_swing(
        min_value: Value,
        max_value: Value,
        values: &[u8],
    ) -> (Value, Value) {
        // unwrap() is safe as values are encoded by select_swing() and update_values_for_swing().
        match values.len() {
            0 => (min_value, max_value),
            1 => (max_value, min_value),
            5 => {
                let value = Value::from_le_bytes(values[1..].try_into().unwrap());
                match values[0] {
                    0 => (value, max_value),
                    1 => (max_value, value),
                    2 => (min_value, value),
                    3 => (value, min_value),
                    _ => panic!("Unknown encoding of swing."),
                }
            }
            8 => {
                let value_size = VALUE_SIZE_IN_BYTES as usize;
                (
                    Value::from_le_bytes(values[0..value_size].try_into().unwrap()),
                    Value::from_le_bytes(values[value_size..2 * value_size].try_into().unwrap()),
                )
            }
            _ => panic!("Unknown encoding of swing."),
        }
    }
}

/// A batch of compressed segments being built.
pub(crate) struct CompressedSegmentBatchBuilder {
    /// Schema of the compressed segments in the batch.
    compressed_schema: Arc<Schema>,
    /// Tag values for the time series the compressed segments in the batch belong to.
    tag_values: Vec<String>,
    /// Index of the field column the compressed segments in the batch belong to.
    field_column_index: i16,
    /// Model type id of each compressed segment in the batch.
    model_type_ids: Int8Builder,
    /// First timestamp of each compressed segment in the batch.
    start_times: TimestampBuilder,
    /// Last timestamp of each compressed segment in the batch.
    end_times: TimestampBuilder,
    /// Data required in addition to `start_times` and `end_times` to
    /// reconstruct the timestamps of each compressed segment in the batch.
    timestamps: BinaryBuilder,
    /// Minimum value of each compressed segment in the batch.
    min_values: ValueBuilder,
    /// Maximum value of each compressed segment in the batch.
    max_values: ValueBuilder,
    /// Data required in addition to `min_value` and `max_value` to reconstruct
    /// the values of each compressed segment in the batch within an error
    /// bound.
    values: BinaryBuilder,
    /// Values between this and the next segment, compressed using [`Gorilla`],
    /// that the models could not represent efficiently within the error bound
    /// and which are too few for a new segment due to the amount of metadata.
    residuals: BinaryBuilder,
    /// Actual error of each compressed segment in the batch.
    error: Float32Builder,
}

impl CompressedSegmentBatchBuilder {
    pub(crate) fn new(
        compressed_schema: Arc<Schema>,
        tag_values: Vec<String>,
        field_column_index: i16,
        capacity: usize,
    ) -> Self {
        Self {
            compressed_schema,
            tag_values,
            field_column_index,
            model_type_ids: Int8Builder::with_capacity(capacity),
            start_times: TimestampBuilder::with_capacity(capacity),
            end_times: TimestampBuilder::with_capacity(capacity),
            timestamps: BinaryBuilder::with_capacity(capacity, capacity),
            min_values: ValueBuilder::with_capacity(capacity),
            max_values: ValueBuilder::with_capacity(capacity),
            values: BinaryBuilder::with_capacity(capacity, capacity),
            residuals: BinaryBuilder::with_capacity(capacity, capacity),
            error: Float32Builder::with_capacity(capacity),
        }
    }

    /// Append a compressed segment to the builder.
    pub(crate) fn append_compressed_segment(
        &mut self,
        model_type_id: i8,
        start_time: Timestamp,
        end_time: Timestamp,
        timestamps: &[u8],
        min_value: Value,
        max_value: Value,
        values: &[u8],
        residuals: &[u8],
        error: f32,
    ) {
        self.model_type_ids.append_value(model_type_id);
        self.start_times.append_value(start_time);
        self.end_times.append_value(end_time);
        self.timestamps.append_value(timestamps);
        self.min_values.append_value(min_value);
        self.max_values.append_value(max_value);
        self.values.append_value(values);
        self.residuals.append_value(residuals);
        self.error.append_value(error);
    }

    /// Return [`RecordBatch`] of compressed segments and consume the builder.
    pub(crate) fn finish(mut self) -> RecordBatch {
        let batch_length = self.model_type_ids.len();
        let field_column_array: Int16Array =
            iter::repeat_n(self.field_column_index, batch_length).collect();

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.compressed_schema.fields.len());
        columns.push(Arc::new(self.model_type_ids.finish()));
        columns.push(Arc::new(self.start_times.finish()));
        columns.push(Arc::new(self.end_times.finish()));
        columns.push(Arc::new(self.timestamps.finish()));
        columns.push(Arc::new(self.min_values.finish()));
        columns.push(Arc::new(self.max_values.finish()));
        columns.push(Arc::new(self.values.finish()));
        columns.push(Arc::new(self.residuals.finish()));
        columns.push(Arc::new(self.error.finish()));
        columns.push(Arc::new(field_column_array));

        for tag_value in &self.tag_values {
            let tag_array: StringArray = iter::repeat_n(Some(tag_value), batch_length).collect();
            columns.push(Arc::new(tag_array));
        }

        RecordBatch::try_new(self.compressed_schema, columns).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::BinaryArray;
    use arrow::datatypes::{DataType, Field};
    use modelardb_test::data_generation::{self, ValuesStructure};
    use modelardb_test::{ERROR_BOUND_TEN, ERROR_BOUND_ZERO};
    use modelardb_types::schemas::COMPRESSED_SCHEMA;
    use modelardb_types::types::{TimestampArray, ValueArray};

    use crate::compression;

    const UNCOMPRESSED_TIMESTAMPS: &[Timestamp] = &[100, 200, 300, 400, 500];

    // Tests for CompressedSegmentBuilder.
    #[test]
    fn test_encoding_decoding_for_pmc_mean_without_residuals() {
        let uncompressed_values = ValueArray::from(vec![10.0, 10.0, 10.0, 10.0, 10.0]);
        assert_encoding_and_decoding_are_valid_for_pmc_mean(
            uncompressed_values,
            4,
            10.0,
            0,
            10.0,
            10.0,
            0,
        );
    }

    #[test]
    fn test_encoding_decoding_for_pmc_mean_with_residuals_with_new_minimum() {
        let uncompressed_values = ValueArray::from(vec![10.0, 10.0, 10.0, 10.0, Value::MIN]);
        assert_encoding_and_decoding_are_valid_for_pmc_mean(
            uncompressed_values,
            3,
            10.0,
            0,
            Value::MIN,
            10.0,
            1,
        );
    }

    #[test]
    fn test_encoding_decoding_for_pmc_mean_with_residuals_with_new_maximum() {
        let uncompressed_values = ValueArray::from(vec![10.0, 10.0, 10.0, 10.0, Value::MAX]);
        assert_encoding_and_decoding_are_valid_for_pmc_mean(
            uncompressed_values,
            3,
            10.0,
            0,
            10.0,
            Value::MAX,
            0,
        );
    }

    #[test]
    fn test_encoding_decoding_for_pmc_mean_with_residuals_with_new_minimum_and_maximum() {
        let uncompressed_values = ValueArray::from(vec![10.0, 10.0, 10.0, Value::MIN, Value::MAX]);
        assert_encoding_and_decoding_are_valid_for_pmc_mean(
            uncompressed_values,
            2,
            10.0,
            0,
            Value::MIN,
            Value::MAX,
            4,
        );
    }

    fn assert_encoding_and_decoding_are_valid_for_pmc_mean(
        uncompressed_values: ValueArray,
        expected_model_end_index: usize,
        expected_model_min_max_value: Value,
        expected_model_values_length: usize,
        expected_segment_min_value: Value,
        expected_segment_max_value: Value,
        expected_segment_values_length: usize,
    ) {
        let (
            _model_start_index,
            _model_end_index,
            segment_min_value,
            segment_max_value,
            segment_values,
        ) = create_and_assert_expected_segment(
            &uncompressed_values,
            PMC_MEAN_ID,
            expected_model_end_index,
            expected_model_min_max_value,
            expected_model_min_max_value,
            expected_model_values_length,
            expected_segment_min_value,
            expected_segment_max_value,
            expected_segment_values_length,
        );

        let segment_value = CompressedSegmentBuilder::decode_values_for_pmc_mean(
            segment_min_value,
            segment_max_value,
            &segment_values,
        );

        assert_eq!(expected_model_min_max_value, segment_value);
    }

    #[test]
    fn test_encoding_decoding_for_increasing_swing_without_residuals() {
        let uncompressed_values = ValueArray::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        assert_encoding_and_decoding_are_valid_for_swing(
            uncompressed_values,
            4,
            10.0,
            50.0,
            0,
            10.0,
            50.0,
            0,
        );
    }

    #[test]
    fn test_encoding_decoding_for_increasing_swing_with_residuals_with_new_minimum() {
        let uncompressed_values = ValueArray::from(vec![10.0, 20.0, 30.0, 40.0, Value::MIN]);
        assert_encoding_and_decoding_are_valid_for_swing(
            uncompressed_values,
            3,
            10.0,
            40.0,
            0,
            Value::MIN,
            40.0,
            5,
        );
    }

    #[test]
    fn test_encoding_decoding_for_increasing_swing_with_residuals_with_new_maximum() {
        let uncompressed_values = ValueArray::from(vec![10.0, 20.0, 30.0, 40.0, Value::MAX]);
        assert_encoding_and_decoding_are_valid_for_swing(
            uncompressed_values,
            3,
            10.0,
            40.0,
            0,
            10.0,
            Value::MAX,
            5,
        );
    }

    #[test]
    fn test_encoding_decoding_for_increasing_swing_with_residuals_with_new_minimum_and_maximum() {
        let uncompressed_values = ValueArray::from(vec![10.0, 20.0, 30.0, Value::MIN, Value::MAX]);
        assert_encoding_and_decoding_are_valid_for_swing(
            uncompressed_values,
            2,
            10.0,
            30.0,
            0,
            Value::MIN,
            Value::MAX,
            8,
        );
    }

    #[test]
    fn test_encoding_decoding_for_decreasing_swing_without_residuals() {
        let uncompressed_values = ValueArray::from(vec![50.0, 40.0, 30.0, 20.0, 10.0]);
        assert_encoding_and_decoding_are_valid_for_swing(
            uncompressed_values,
            4,
            10.0,
            50.0,
            1,
            10.0,
            50.0,
            1,
        );
    }

    #[test]
    fn test_encoding_decoding_for_decreasing_swing_with_residuals_with_new_minimum() {
        let uncompressed_values = ValueArray::from(vec![50.0, 40.0, 30.0, 20.0, Value::MIN]);
        assert_encoding_and_decoding_are_valid_for_swing(
            uncompressed_values,
            3,
            20.0,
            50.0,
            1,
            Value::MIN,
            50.0,
            5,
        );
    }

    #[test]
    fn test_encoding_decoding_for_decreasing_swing_with_residuals_with_new_maximum() {
        let uncompressed_values = ValueArray::from(vec![50.0, 40.0, 30.0, 20.0, Value::MAX]);
        assert_encoding_and_decoding_are_valid_for_swing(
            uncompressed_values,
            3,
            20.0,
            50.0,
            1,
            20.0,
            Value::MAX,
            5,
        );
    }

    #[test]
    fn test_encoding_decoding_for_decreasing_swing_with_residuals_with_new_minimum_and_maximum() {
        let uncompressed_values = ValueArray::from(vec![50.0, 40.0, 30.0, Value::MIN, Value::MAX]);
        assert_encoding_and_decoding_are_valid_for_swing(
            uncompressed_values,
            2,
            30.0,
            50.0,
            1,
            Value::MIN,
            Value::MAX,
            8,
        );
    }

    fn assert_encoding_and_decoding_are_valid_for_swing(
        uncompressed_values: ValueArray,
        expected_model_end_index: usize,
        expected_model_min_value: Value,
        expected_model_max_value: Value,
        expected_model_values_length: usize,
        expected_segment_min_value: Value,
        expected_segment_max_value: Value,
        expected_segment_values_length: usize,
    ) {
        let (
            model_start_index,
            model_end_index,
            segment_min_value,
            segment_max_value,
            segment_values,
        ) = create_and_assert_expected_segment(
            &uncompressed_values,
            SWING_ID,
            expected_model_end_index,
            expected_model_min_value,
            expected_model_max_value,
            expected_model_values_length,
            expected_segment_min_value,
            expected_segment_max_value,
            expected_segment_values_length,
        );

        let model_first_value = uncompressed_values.value(model_start_index);
        let model_last_value = uncompressed_values.value(model_end_index);

        let (segment_first_value, segment_last_value) =
            CompressedSegmentBuilder::decode_values_for_swing(
                segment_min_value,
                segment_max_value,
                &segment_values,
            );

        assert_eq!(model_first_value, segment_first_value);
        assert_eq!(model_last_value, segment_last_value);
    }

    fn create_and_assert_expected_segment(
        uncompressed_values: &ValueArray,
        expected_model_type_id: i8,
        expected_model_end_index: usize,
        expected_model_min_value: Value,
        expected_model_max_value: Value,
        expected_model_values_length: usize,
        expected_segment_min_value: Value,
        expected_segment_max_value: Value,
        expected_segment_values_length: usize,
    ) -> (usize, usize, Value, Value, Vec<u8>) {
        // Fit a model to the uncompressed timestamps and the uncompressed values, ensure a model of
        // the expected type is used, and assert that the expected encoding is used for the model.
        let uncompressed_timestamps = TimestampArray::from(UNCOMPRESSED_TIMESTAMPS.to_vec());

        let model = compression::fit_next_model(
            0,
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            &uncompressed_timestamps,
            uncompressed_values,
        );

        assert_eq!(expected_model_type_id, model.model_type_id);
        assert_eq!(0, model.start_index);
        assert_eq!(expected_model_end_index, model.end_index);
        assert_eq!(expected_model_min_value, model.min_value);
        assert_eq!(expected_model_max_value, model.max_value);
        assert_eq!(expected_model_values_length, model.values.len());

        let model_start_index = model.start_index;
        let model_end_index = model.end_index;

        // Create a segment that represents its values using a model of the expected type and its
        // residuals using Gorilla, and then assert that the expected encoding is used for it.
        let residuals_end_index = uncompressed_timestamps.len() - 1;

        let mut compressed_schema_fields = COMPRESSED_SCHEMA.0.fields.clone().to_vec();
        compressed_schema_fields.push(Arc::new(Field::new("tag", DataType::Utf8, false)));
        let compressed_schema = Arc::new(Schema::new(compressed_schema_fields));

        let mut compressed_segment_batch_builder =
            CompressedSegmentBatchBuilder::new(compressed_schema, vec!["tag".to_owned()], 0, 1);

        model.finish(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            residuals_end_index,
            &uncompressed_timestamps,
            uncompressed_values,
            &mut compressed_segment_batch_builder,
        );

        let batch = compressed_segment_batch_builder.finish();
        assert_eq!(1, batch.num_rows());

        let segment_min_value = modelardb_types::array!(batch, 4, ValueArray).value(0);
        let segment_max_value = modelardb_types::array!(batch, 5, ValueArray).value(0);
        let segment_values = modelardb_types::array!(batch, 6, BinaryArray).value(0);

        assert_eq!(expected_segment_min_value, segment_min_value);
        assert_eq!(expected_segment_max_value, segment_max_value);
        assert_eq!(expected_segment_values_length, segment_values.len());

        (
            model_start_index,
            model_end_index,
            segment_min_value,
            segment_max_value,
            segment_values.to_vec(),
        )
    }

    #[test]
    fn test_model_with_fewest_bytes_is_selected() {
        let uncompressed_timestamps = data_generation::generate_timestamps(25, false);
        let mut uncompressed_values = ValueBuilder::with_capacity(uncompressed_timestamps.len());

        uncompressed_values.append_slice(
            data_generation::generate_values(
                uncompressed_timestamps.values(),
                ValuesStructure::Constant(None),
            )
            .values(),
        );
        uncompressed_values.append_slice(
            data_generation::generate_values(
                uncompressed_timestamps.values(),
                ValuesStructure::Random(0.0..100.0),
            )
            .values(),
        );

        let model = compression::fit_next_model(
            0,
            ErrorBound::try_new_relative(ERROR_BOUND_TEN).unwrap(),
            &uncompressed_timestamps,
            &uncompressed_values.finish(),
        );

        assert_eq!(model.model_type_id, PMC_MEAN_ID);
    }
}
