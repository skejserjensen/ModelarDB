/* Copyright 2021 The ModelarDB Contributors
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

//! Implementation of the Gorilla model type which uses the lossless compression method for
//! floating-point values proposed for the time series management system Gorilla in the [Gorilla
//! paper]. The compression method described in the paper has been extended with support for lossy
//! compression by replacing values with the previous value if possible within the error bound. As
//! this compression method compresses the values of a time series segment using XOR and a variable
//! length binary encoding, aggregates are computed by iterating over all values in the segment.
//!
//! [Gorilla paper]: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf

use modelardb_types::types::{Timestamp, Value, ValueBuilder};

use crate::models;
use crate::models::ErrorBound;
use crate::models::bits::{BitReader, BitVecBuilder};

/// The state the Gorilla model type needs while compressing the values of a
/// time series segment.
pub struct Gorilla {
    /// Maximum relative error for the value of each data point.
    error_bound: ErrorBound,
    /// Min value compressed and added to `compressed_values`.
    min_value: Value,
    /// Max value compressed and added to `compressed_values`.
    max_value: Value,
    /// Last value compressed and added to `compressed_values`.
    last_value: Value,
    /// Number of leading zero bits for the last value that was compressed by
    /// adding its meaningful bits and leading zero bits to `compressed_values`.
    last_leading_zero_bits: u8,
    /// Number of trailing zero bits for the last value that was compressed by
    /// adding its meaningful bits and leading zero bits to `compressed_values`.
    last_trailing_zero_bits: u8,
    /// Values compressed using XOR and a variable length binary encoding.
    compressed_values: BitVecBuilder,
    /// The number of values stored in `compressed_values`.
    length: usize,
}

impl Gorilla {
    pub fn new(error_bound: ErrorBound) -> Self {
        Self {
            error_bound,
            min_value: Value::NAN,
            max_value: Value::NAN,
            last_value: 0.0,
            last_leading_zero_bits: u8::MAX,
            last_trailing_zero_bits: 0,
            compressed_values: BitVecBuilder::new(),
            length: 0,
        }
    }

    /// Store the first value in full if this instance of [`Gorilla`] is empty and then compress the
    /// remaining `values` using XOR and a variable length binary encoding before storing them.
    pub fn compress_values(&mut self, values: &[Value]) {
        for value in values {
            if self.compressed_values.is_empty() {
                // Store the first value uncompressed using size_of::<Value> bits.
                self.compressed_values
                    .append_bits(value.to_bits() as u64, models::VALUE_SIZE_IN_BITS);

                self.update_min_max_and_last_value(*value);
            } else {
                self.compress_value_xor_last_value(*value);
            };
        }
    }

    /// Assume `last_value` is stored fully elsewhere, set it as the current last value, and then
    /// compress each of the values in `values` using XOR and a variable length binary encoding.
    pub fn compress_values_without_first(&mut self, values: &[Value], model_last_value: Value) {
        self.last_value = model_last_value;
        for value in values {
            self.compress_value_xor_last_value(*value);
        }
    }

    /// Compress `value` using XOR and a variable length binary encoding and then store it.
    fn compress_value_xor_last_value(&mut self, value: Value) {
        // The best case for Gorilla is storing duplicate values.
        let value = if models::is_value_within_error_bound(self.error_bound, value, self.last_value)
        {
            self.last_value
        } else {
            value
        };

        let value_as_integer = value.to_bits();
        let last_value_as_integer = self.last_value.to_bits();
        let value_xor_last_value = value_as_integer ^ last_value_as_integer;

        if value_xor_last_value == 0 {
            // Store each repeated value as a single zero bit.
            self.compressed_values.append_a_zero_bit();
        } else {
            // Store each new value as its leading zero bits (if necessary) and
            // meaningful bits (all bits from the first to the last one bit).
            let leading_zero_bits = value_xor_last_value.leading_zeros() as u8;
            let trailing_zero_bits = value_xor_last_value.trailing_zeros() as u8;
            self.compressed_values.append_a_one_bit();

            if leading_zero_bits >= self.last_leading_zero_bits
                && trailing_zero_bits >= self.last_trailing_zero_bits
            {
                // Store only the meaningful bits.
                self.compressed_values.append_a_zero_bit();
                let meaningful_bits = models::VALUE_SIZE_IN_BITS
                    - self.last_leading_zero_bits
                    - self.last_trailing_zero_bits;
                self.compressed_values.append_bits(
                    (value_xor_last_value >> self.last_trailing_zero_bits) as u64,
                    meaningful_bits,
                );
            } else {
                // Store the leading zero bits before the meaningful bits using
                // 5 and 6 bits respectively as described in the Gorilla paper.
                self.compressed_values.append_a_one_bit();
                self.compressed_values
                    .append_bits(leading_zero_bits as u64, 5);

                let meaningful_bits =
                    models::VALUE_SIZE_IN_BITS - leading_zero_bits - trailing_zero_bits;
                self.compressed_values
                    .append_bits(meaningful_bits as u64, 6);
                self.compressed_values.append_bits(
                    (value_xor_last_value >> trailing_zero_bits) as u64,
                    meaningful_bits,
                );

                self.last_leading_zero_bits = leading_zero_bits;
                self.last_trailing_zero_bits = trailing_zero_bits;
            }
        }

        self.update_min_max_and_last_value(value);
    }

    /// Update the current minimum, maximum, and last value based on `value`.
    fn update_min_max_and_last_value(&mut self, value: Value) {
        self.min_value = Value::min(self.min_value, value);
        self.max_value = Value::max(self.max_value, value);
        self.last_value = value;
        self.length += 1;
    }

    /// Return the values compressed using XOR and a variable length binary
    /// encoding, the compressed minimum value, and the compressed maximum value.
    pub fn model(self) -> (Vec<u8>, Value, Value) {
        (
            self.compressed_values.finish(),
            self.min_value,
            self.max_value,
        )
    }
}

/// Compute the sum of the values for a time series segment whose values are compressed using
/// Gorilla's compression method for floating-point values. If `maybe_model_last_value` is provided,
/// it is assumed the first value in `values` is compressed against it instead of being stored in
/// full, i.e., uncompressed.
pub fn sum(length: usize, values: &[u8], maybe_model_last_value: Option<Value>) -> Value {
    // This function replicates code from gorilla::grid() as it isn't necessary to store the
    // timestamps and values in arrays for a sum. So any changes to the decompression must be
    // mirrored in gorilla::grid().
    let mut bits = BitReader::try_new(values).unwrap();
    let mut leading_zeros = u8::MAX;
    let mut trailing_zeros: u8 = 0;

    let (mut last_value, mut sum) = if let Some(model_last_value) = maybe_model_last_value {
        // The first value is stored compressed against model_last_value.
        (model_last_value.to_bits(), 0.0)
    } else {
        // The first value is stored uncompressed using size_of::<Value> bits.
        let first_value = bits.read_bits(models::VALUE_SIZE_IN_BITS) as u32;
        (first_value, Value::from_bits(first_value))
    };

    // Then values are stored using XOR and a variable length binary encoding.
    for _ in 0..length - maybe_model_last_value.is_none() as usize {
        if bits.read_bit() {
            if bits.read_bit() {
                // New leading and trailing zeros.
                leading_zeros = bits.read_bits(5) as u8;
                let meaningful_bits = bits.read_bits(6) as u8;
                trailing_zeros = models::VALUE_SIZE_IN_BITS - meaningful_bits - leading_zeros;
            }

            // Decompress the value by reading its meaningful bits, restoring
            // its trailing zeroes through shifting, and reversing the XOR.
            let meaningful_bits = models::VALUE_SIZE_IN_BITS - leading_zeros - trailing_zeros;
            let mut value = bits.read_bits(meaningful_bits) as u32;
            value <<= trailing_zeros;
            value ^= last_value;
            last_value = value;
        }
        sum += Value::from_bits(last_value);
    }
    sum
}

/// Decompress all the values in `values` for the `timestamps` without matching values in
/// `value_builder`. The values in `values` are compressed using Gorilla's compression method for
/// floating-point values. `values` are appended to `value_builder`. If `maybe_model_last_value`
/// is provided, it is assumed the first value in `values` is compressed against it instead of being
/// stored in full, i.e., uncompressed.
pub fn grid(
    values: &[u8],
    timestamps: &[Timestamp],
    value_builder: &mut ValueBuilder,
    maybe_model_last_value: Option<Value>,
) {
    // Changes to the decompression must be mirrored in gorilla::sum().
    // unwrap() is safe as values is from a segment and thus cannot be empty.
    let mut bits = BitReader::try_new(values).unwrap();
    let mut leading_zeros = u8::MAX;
    let mut trailing_zeros: u8 = 0;

    let mut last_value = if let Some(model_last_value) = maybe_model_last_value {
        // The first value is stored compressed against model_last_value.
        model_last_value.to_bits()
    } else {
        // The first value is stored uncompressed using size_of::<Value> bits.
        let first_value = bits.read_bits(models::VALUE_SIZE_IN_BITS) as u32;
        value_builder.append_value(Value::from_bits(first_value));
        first_value
    };

    // Then values are stored using XOR and a variable length binary encoding. If last_value was
    // provided by the model, the first value has not been read from values so all must be read now.
    for _ in 0..timestamps.len() - maybe_model_last_value.is_none() as usize {
        if bits.read_bit() {
            if bits.read_bit() {
                // New leading and trailing zeros.
                leading_zeros = bits.read_bits(5) as u8;
                let meaningful_bits = bits.read_bits(6) as u8;
                trailing_zeros = models::VALUE_SIZE_IN_BITS - meaningful_bits - leading_zeros;
            }

            // Decompress the value by reading its meaningful bits, restoring
            // its trailing zeroes through shifting, and reversing the XOR.
            let meaningful_bits = models::VALUE_SIZE_IN_BITS - leading_zeros - trailing_zeros;
            let mut value = bits.read_bits(meaningful_bits) as u32;
            value <<= trailing_zeros;
            value ^= last_value;
            last_value = value;
        }
        value_builder.append_value(Value::from_bits(last_value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::{ERROR_BOUND_TEN, ERROR_BOUND_ZERO};
    use proptest::num::f32 as ProptestValue;
    use proptest::{bool, collection, prop_assert, prop_assert_eq, prop_assume, proptest};

    use crate::models;

    // Tests for Gorilla.
    #[test]
    fn test_empty_sequence_with_absolute_error_bound_zero() {
        let error_bound = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        assert!(Gorilla::new(error_bound).model().0.is_empty());
    }

    #[test]
    fn test_empty_sequence_with_relative_error_bound_zero() {
        let error_bound = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        assert!(Gorilla::new(error_bound).model().0.is_empty());
    }

    proptest! {
    #[test]
    fn test_append_single_value_with_absolute_error_bound_zero(value in ProptestValue::ANY) {
        let error_bound = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[value]);

        prop_assert!(models::equal_or_nan(value as f64, model_type.last_value as f64));
        prop_assert_eq!(model_type.last_leading_zero_bits, u8::MAX);
        prop_assert_eq!(model_type.last_trailing_zero_bits, 0);
    }

    #[test]
    fn test_append_single_value_with_relative_error_bound_zero(value in ProptestValue::ANY) {
        let error_bound = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[value]);

        prop_assert!(models::equal_or_nan(value as f64, model_type.last_value as f64));
        prop_assert_eq!(model_type.last_leading_zero_bits, u8::MAX);
        prop_assert_eq!(model_type.last_trailing_zero_bits, 0);
    }

    #[test]
    fn test_append_repeated_values_with_absolute_error_bound_zero(value in ProptestValue::ANY) {
        let error_bound = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[value, value]);

        prop_assert!(models::equal_or_nan(value as f64, model_type.last_value as f64));
        prop_assert_eq!(model_type.last_leading_zero_bits, u8::MAX);
        prop_assert_eq!(model_type.last_trailing_zero_bits, 0);
    }

    #[test]
    fn test_append_repeated_values_with_relative_error_bound_zero(value in ProptestValue::ANY) {
        let error_bound = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[value, value]);

        prop_assert!(models::equal_or_nan(value as f64, model_type.last_value as f64));
        prop_assert_eq!(model_type.last_leading_zero_bits, u8::MAX);
        prop_assert_eq!(model_type.last_trailing_zero_bits, 0);
    }
    }

    #[test]
    fn test_append_different_values_with_leading_zero_bits_with_absolute_error_bound_zero() {
        let error_bound = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[37.0, 73.0]);

        assert!(models::equal_or_nan(73.0, model_type.last_value as f64));
        assert_eq!(model_type.last_leading_zero_bits, 8);
        assert_eq!(model_type.last_trailing_zero_bits, 17);
    }

    #[test]
    fn test_append_different_values_with_leading_zero_bits_with_relative_error_bound_zero() {
        let error_bound = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[37.0, 73.0]);

        assert!(models::equal_or_nan(73.0, model_type.last_value as f64));
        assert_eq!(model_type.last_leading_zero_bits, 8);
        assert_eq!(model_type.last_trailing_zero_bits, 17);
    }

    #[test]
    fn test_append_different_values_without_leading_zero_bits_with_absolute_error_bound_zero() {
        let error_bound = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[37.0, 71.0, 73.0]);

        assert!(models::equal_or_nan(73.0, model_type.last_value as f64));
        assert_eq!(model_type.last_leading_zero_bits, 8);
        assert_eq!(model_type.last_trailing_zero_bits, 17);
    }

    #[test]
    fn test_append_different_values_without_leading_zero_bits_with_relative_error_bound_zero() {
        let error_bound = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[37.0, 71.0, 73.0]);

        assert!(models::equal_or_nan(73.0, model_type.last_value as f64));
        assert_eq!(model_type.last_leading_zero_bits, 8);
        assert_eq!(model_type.last_trailing_zero_bits, 17);
    }

    #[test]
    fn test_append_values_within_error_bound_with_absolute_error_bound_ten() {
        test_append_values_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_TEN).unwrap(),
        );
    }
    #[test]
    fn test_append_values_within_error_bound_with_relative_error_bound_ten() {
        test_append_values_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_TEN).unwrap(),
        );
    }

    fn test_append_values_within_error_bound(error_bound: ErrorBound) {
        let mut model_type = Gorilla::new(error_bound);

        model_type.compress_values(&[10.0]);
        let before_last_value = model_type.last_value;
        let before_last_leading_zero_bits = model_type.last_leading_zero_bits;
        let before_last_trailing_zero_bits = model_type.last_trailing_zero_bits;

        model_type.compress_values(&[11.0]);

        // State should be unchanged when the value is within the error bound.
        assert_eq!(before_last_value, model_type.last_value);
        assert_eq!(
            before_last_leading_zero_bits,
            model_type.last_leading_zero_bits
        );
        assert_eq!(
            before_last_trailing_zero_bits,
            model_type.last_trailing_zero_bits
        );
    }

    // Tests for sum().
    proptest! {
    #[test]
    fn test_sum_with_absolute_error_bound_zero(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        let expected_sum = values.iter().sum::<f32>();
        let compressed_values = compress_values_using_gorilla(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            &values, None);
        let sum = sum(values.len(), &compressed_values, None);
        prop_assert!(models::equal_or_nan(expected_sum as f64, sum as f64));
    }

    #[test]
    fn test_sum_with_relative_error_bound_zero(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        let expected_sum = values.iter().sum::<f32>();
        let compressed_values = compress_values_using_gorilla(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            &values, None);
        let sum = sum(values.len(), &compressed_values, None);
        prop_assert!(models::equal_or_nan(expected_sum as f64, sum as f64));
    }
    }

    #[test]
    fn test_sum_model_single_value_with_absolute_error_bound_zero() {
        let compressed_values = compress_values_using_gorilla(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            &[37.0],
            None,
        );
        let sum = sum(1, &compressed_values, None);
        assert_eq!(sum, 37.0);
    }

    #[test]
    fn test_sum_model_single_value_with_relative_error_bound_zero() {
        let compressed_values = compress_values_using_gorilla(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            &[37.0],
            None,
        );
        let sum = sum(1, &compressed_values, None);
        assert_eq!(sum, 37.0);
    }

    #[test]
    fn test_sum_residuals_single_value_with_absolute_error_bound_zero() {
        let maybe_model_last_value = Some(37.0);
        let compressed_values = compress_values_using_gorilla(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            &[37.0],
            maybe_model_last_value,
        );
        let sum = sum(1, &compressed_values, maybe_model_last_value);
        assert_eq!(sum, 37.0);
    }

    #[test]
    fn test_sum_residuals_single_value_with_relative_error_bound_zero() {
        let maybe_model_last_value = Some(37.0);
        let compressed_values = compress_values_using_gorilla(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            &[37.0],
            maybe_model_last_value,
        );
        let sum = sum(1, &compressed_values, maybe_model_last_value);
        assert_eq!(sum, 37.0);
    }

    // Tests for grid().
    proptest! {
    #[test]
    fn test_grid_with_absolute_error_bound_zero(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        assert_grid_with_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            &values);
    }

    #[test]
    fn test_grid_with_relative_error_bound_zero(values in collection::vec(ProptestValue::ANY, 0..50)) {
        prop_assume!(!values.is_empty());
        assert_grid_with_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            &values);
    }
    }

    fn assert_grid_with_error_bound(error_bound: ErrorBound, values: &[Value]) {
        let compressed_values = compress_values_using_gorilla(error_bound, values, None);

        let timestamps: Vec<Timestamp> = (1..=values.len() as i64).step_by(1).collect();
        let mut value_builder = ValueBuilder::with_capacity(values.len());
        grid(&compressed_values, &timestamps, &mut value_builder, None);

        let values_array = value_builder.finish();

        assert!(values.len() == timestamps.len() && values.len() == values_array.len());
        assert!(
            timestamps
                .windows(2)
                .all(|window| window[1] - window[0] == 1)
        );
        assert!(slice_of_value_equal(values_array.values(), values));
    }

    #[test]
    fn test_grid_model_single_value_with_absolute_error_bound_zero() {
        assert_grid_single(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            None,
        );
    }

    #[test]
    fn test_grid_model_single_value_with_relative_error_bound_zero() {
        assert_grid_single(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            None,
        );
    }

    #[test]
    fn test_grid_residuals_single_value_with_absolute_error_bound_zero() {
        assert_grid_single(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            Some(37.0),
        );
    }

    #[test]
    fn test_grid_residuals_single_value_with_relative_error_bound_zero() {
        assert_grid_single(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            Some(37.0),
        );
    }

    fn assert_grid_single(error_bound: ErrorBound, maybe_model_last_value: Option<Value>) {
        let compressed_values =
            compress_values_using_gorilla(error_bound, &[37.0], maybe_model_last_value);
        let mut value_builder = ValueBuilder::new();

        grid(
            &compressed_values,
            &[100],
            &mut value_builder,
            maybe_model_last_value,
        );

        let values = value_builder.finish();

        assert_eq!(values.len(), 1);
        assert_eq!(values.value(0), 37.0);
    }

    fn compress_values_using_gorilla(
        error_bound: ErrorBound,
        values: &[Value],
        maybe_model_last_value: Option<Value>,
    ) -> Vec<u8> {
        let mut model_type = Gorilla::new(error_bound);
        if let Some(model_last_value) = maybe_model_last_value {
            model_type.compress_values_without_first(values, model_last_value);
        } else {
            model_type.compress_values(values);
        }
        model_type.compressed_values.finish()
    }

    fn slice_of_value_equal(values_one: &[Value], values_two: &[Value]) -> bool {
        let mut equal = true;
        for values in values_one.iter().zip(values_two) {
            let (value_one, value_two) = values;
            equal &= models::equal_or_nan(*value_one as f64, *value_two as f64);
        }
        equal
    }
}
