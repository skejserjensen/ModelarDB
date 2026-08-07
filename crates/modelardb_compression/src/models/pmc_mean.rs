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

//! Implementation of the PMC-Mean model type from the [Poor Man’s Compression
//! paper] and efficient computation of aggregates for models of type PMC-Mean
//! as described in the [ModelarDB paper].
//!
//! [Poor Man’s Compression paper]: https://ieeexplore.ieee.org/document/1260811
//! [ModelarDB paper]: https://www.vldb.org/pvldb/vol11/p1688-jensen.pdf

use modelardb_types::schemas::COMPRESSED_METADATA_SIZE_IN_BYTES;
use modelardb_types::types::{Timestamp, Value, ValueBuilder};

use crate::models;
use crate::models::ErrorBound;

/// The state the PMC-Mean model type needs while fitting a model to a time
/// series segment.
pub struct PMCMean {
    /// Maximum relative error for the value of each data point.
    error_bound: ErrorBound,
    /// Minimum value in the segment the current model is fitted to.
    min_value: Value,
    /// Maximum value in the segment the current model is fitted to.
    max_value: Value,
    /// The sum of the values in the segment the current model is fitted to.
    sum_of_values: f64,
    /// The number of data points the current model has been fitted to.
    length: usize,
}

impl PMCMean {
    pub fn new(error_bound: ErrorBound) -> Self {
        Self {
            error_bound,
            min_value: Value::NAN,
            max_value: Value::NAN,
            sum_of_values: 0.0,
            length: 0,
        }
    }

    /// Attempt to update the current model of type PMC-Mean to also represent
    /// `value`. Returns [`true`] if the model can also represent `value`,
    /// otherwise [`false`].
    pub fn fit_value(&mut self, value: Value) -> bool {
        let next_min_value = Value::min(self.min_value, value);
        let next_max_value = Value::max(self.max_value, value);
        let next_sum_of_values = self.sum_of_values + value as f64;
        let next_length = self.length + 1;
        let average = (next_sum_of_values / next_length as f64) as Value;
        if models::is_value_within_error_bound(self.error_bound, next_min_value, average)
            && models::is_value_within_error_bound(self.error_bound, next_max_value, average)
        {
            self.min_value = next_min_value;
            self.max_value = next_max_value;
            self.sum_of_values = next_sum_of_values;
            self.length = next_length;
            true
        } else {
            false
        }
    }

    /// Return the number of values the model currently represents.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Return the number of bytes the current model uses per data point on average.
    pub fn bytes_per_value(&self) -> f32 {
        // No additional data is needed for PMC-Mean as the value can be read from min_value or
        // max_value which is already stored as part of the metadata in the compressed segments.
        COMPRESSED_METADATA_SIZE_IN_BYTES.to_owned() as f32 / self.length as f32
    }

    /// Return the current model. For a model of type PMC-Mean, its coefficient
    /// is the average value of the time series segment the model represents.
    pub fn model(self) -> Value {
        (self.sum_of_values / self.length as f64) as Value
    }
}

/// Compute the sum of the values for a time series segment whose values are
/// represented by a model of type PMC-Mean.
pub fn sum(model_length: usize, value: Value) -> Value {
    model_length as Value * value
}

/// Reconstruct the values for the `timestamps` without matching values in `value_builder` using a
/// model of type PMC-Mean. The `values` are appended to `value_builder`.
pub fn grid(value: Value, timestamps: &[Timestamp], value_builder: &mut ValueBuilder) {
    for _timestamp in timestamps {
        value_builder.append_value(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::{
        ERROR_BOUND_ABSOLUTE_MAX, ERROR_BOUND_FIVE, ERROR_BOUND_RELATIVE_MAX, ERROR_BOUND_ZERO,
    };
    use proptest::num::f32 as ProptestValue;
    use proptest::{prop_assert, prop_assume, proptest};

    // Tests for PMCMean.
    proptest! {
    #[test]
    fn test_can_fit_sequence_of_finite_value_with_absolute_error_bound_zero(value in ProptestValue::ANY) {
        can_fit_sequence_of_value_within_error_bound(ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(), value)
    }

    #[test]
    fn test_can_fit_sequence_of_finite_value_with_relative_error_bound_zero(value in ProptestValue::ANY) {
        can_fit_sequence_of_value_within_error_bound(ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(), value)
    }
    }

    #[test]
    fn test_can_fit_sequence_of_positive_infinity_with_absolute_error_bound_zero() {
        can_fit_sequence_of_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            Value::INFINITY,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_positive_infinity_with_relative_error_bound_zero() {
        can_fit_sequence_of_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            Value::INFINITY,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_negative_infinity_with_absolute_error_bound_zero() {
        can_fit_sequence_of_value_within_error_bound(
            ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap(),
            Value::NEG_INFINITY,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_negative_infinity_with_relative_error_bound_zero() {
        can_fit_sequence_of_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            Value::NEG_INFINITY,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_nans_with_absolute_error_bound_zero() {
        can_fit_sequence_of_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            Value::NAN,
        )
    }

    #[test]
    fn test_can_fit_sequence_of_nans_with_relative_error_bound_zero() {
        can_fit_sequence_of_value_within_error_bound(
            ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap(),
            Value::NAN,
        )
    }

    fn can_fit_sequence_of_value_within_error_bound(error_bound: ErrorBound, value: Value) {
        let mut model_type = PMCMean::new(error_bound);
        for _ in 0..5 {
            assert!(model_type.fit_value(value));
        }

        if value.is_nan() {
            assert!(model_type.model().is_nan());
        } else {
            assert_eq!(model_type.model(), value);
        }
    }

    proptest! {
    #[test]
    fn test_can_fit_one_value_with_absolute_error_bound_zero(value in ProptestValue::ANY) {
        let error_bound_zero = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        prop_assert!(PMCMean::new(error_bound_zero).fit_value(value));
    }

    #[test]
    fn test_can_fit_one_value_with_relative_error_bound_zero(value in ProptestValue::ANY) {
        let error_bound_zero = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        prop_assert!(PMCMean::new(error_bound_zero).fit_value(value));
    }

    #[test]
    fn test_cannot_fit_other_value_and_positive_infinity_with_absolute_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_positive_infinity_with_relative_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_negative_infinity_with_absolute_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::NEG_INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_negative_infinity_with_relative_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::NEG_INFINITY));
    }

    #[test]
    fn test_cannot_fit_other_value_and_nan_with_absolute_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::NAN));
    }

    #[test]
    fn test_cannot_fit_other_value_and_nan_with_relative_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(value));
        prop_assert!(!model_type.fit_value(Value::NAN));
    }

    #[test]
    fn test_cannot_fit_positive_infinity_and_other_value_with_absolute_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::INFINITY));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_positive_infinity_and_other_value_with_relative_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(value != Value::INFINITY);
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::INFINITY));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_negative_infinity_and_other_value_with_absolute_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::NEG_INFINITY));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_negative_infinity_and_other_value_with_relative_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(value != Value::NEG_INFINITY);
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::NEG_INFINITY));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_nan_and_other_value_with_absolute_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new_absolute(ERROR_BOUND_ABSOLUTE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::NAN));
        prop_assert!(!model_type.fit_value(value));
    }

    #[test]
    fn test_cannot_fit_nan_and_other_value_with_relative_error_bound_max(value in ProptestValue::ANY) {
        prop_assume!(!value.is_nan());
        let error_bound_max = ErrorBound::try_new_relative(ERROR_BOUND_RELATIVE_MAX).unwrap();
        let mut model_type = PMCMean::new(error_bound_max);
        prop_assert!(model_type.fit_value(Value::NAN));
        prop_assert!(!model_type.fit_value(value));
    }
    }

    #[test]
    fn test_cannot_fit_sequence_of_different_values_with_absolute_error_bound_zero() {
        let error_bound_zero = ErrorBound::try_new_absolute(ERROR_BOUND_ZERO).unwrap();
        assert!(!fit_sequence_of_different_values_with_error_bound(
            error_bound_zero
        ))
    }

    #[test]
    fn test_cannot_fit_sequence_of_different_values_with_relative_error_bound_zero() {
        let error_bound_zero = ErrorBound::try_new_relative(ERROR_BOUND_ZERO).unwrap();
        assert!(!fit_sequence_of_different_values_with_error_bound(
            error_bound_zero
        ))
    }

    #[test]
    fn test_can_fit_sequence_of_different_values_with_absolute_error_bound_five() {
        let error_bound_five = ErrorBound::try_new_absolute(ERROR_BOUND_FIVE).unwrap();
        assert!(fit_sequence_of_different_values_with_error_bound(
            error_bound_five
        ))
    }

    #[test]
    fn test_can_fit_sequence_of_different_values_with_relative_error_bound_five() {
        let error_bound_five = ErrorBound::try_new_relative(ERROR_BOUND_FIVE).unwrap();
        assert!(fit_sequence_of_different_values_with_error_bound(
            error_bound_five
        ))
    }

    fn fit_sequence_of_different_values_with_error_bound(error_bound: ErrorBound) -> bool {
        let mut model_type = PMCMean::new(error_bound);
        let mut fit_all_values = true;
        for value in [42.0, 42.0, 42.8, 42.0, 42.0] {
            fit_all_values &= model_type.fit_value(value);
        }
        fit_all_values
    }

    // Tests for sum().
    proptest! {
    #[test]
    fn test_sum(value in ProptestValue::ANY) {
        prop_assert!(models::equal_or_nan(sum(10, value) as f64, (10.0 * value) as f64));
    }
    }

    // Tests for grid().
    proptest! {
    #[test]
    fn test_grid(value in ProptestValue::ANY) {
        let sampling_interval: i64 = 60;
        let timestamps: Vec<Timestamp> = (60..=600).step_by(60).collect();
        let mut value_builder = ValueBuilder::with_capacity(10);

        grid(
            value,
            &timestamps,
            &mut value_builder,
        );

        let values = value_builder.finish();

        prop_assert!(
            timestamps.len() == 10 && timestamps.len() == values.len()
        );
        prop_assert!(timestamps
            .windows(2)
            .all(|window| window[1] - window[0] == sampling_interval));
        prop_assert!(values
            .iter()
            .all(|value_option| models::equal_or_nan(value_option.unwrap() as f64, value as f64)));
    }
    }
}
