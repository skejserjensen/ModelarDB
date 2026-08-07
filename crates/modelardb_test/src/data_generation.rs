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

//! Implementation of functions that can generate timestamps and values with specific structure.

use std::env;
use std::iter;
use std::num::ParseIntError;
use std::ops::Range;
use std::sync::LazyLock;

use modelardb_types::types::{
    Timestamp, TimestampArray, TimestampBuilder, ValueArray, ValueBuilder,
};
use rand::distr::Uniform;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Randomly generated static seed for the random number generators used for all data generation.
/// One randomly generated seed is used to ensure new data is generated each time the tests are
/// executed while also allowing the tests to be repeated by assigning a previously generated seed
/// to the `MODELARDB_TEST_SEED` environment variable as 32 bytes, each separated by a space.
static RANDOM_NUMBER_SEED: LazyLock<[u8; 32]> = LazyLock::new(|| {
    match env::var("MODELARDB_TEST_SEED") {
        Ok(seed) => {
            let message = "MODELARDB_TEST_SEED must be 32 bytes, each separated by a space.";

            seed.split(' ')
                .map(|maybe_byte| maybe_byte.parse::<u8>())
                .collect::<Result<Vec<u8>, ParseIntError>>()
                .map_err(|_| message)
                .unwrap()
                .try_into()
                .map_err(|_| message)
                .unwrap()
        }
        Err(_) => {
            let random_number_seed = rand::random::<[u8; 32]>();

            let mut seed_as_string = String::new();
            for value in random_number_seed {
                seed_as_string.push_str(&value.to_string());
                seed_as_string.push(' ');
            }

            // unwrap() is not safe, but this function is designed to be used by tests.
            let binary_path = env::current_exe().unwrap();
            let binary_name = binary_path.file_name().unwrap().to_str().unwrap();
            println!("Seed for {binary_name} is: {seed_as_string}");

            random_number_seed
        }
    }
});

/// The different structures of values which can be generated.
#[derive(Clone)]
pub enum ValuesStructure {
    /// A sequence of constant values, optionally with noise in the provided range multiplied by the
    /// generated values if it is not [`None`].
    Constant(Option<Range<f32>>),
    /// A sequence of increasing or decreasing values, optionally with noise in the provided range
    /// multiplied by the generated values if it is not [`None`].
    Linear(Option<Range<f32>>),
    /// A sequence of random values in the provided range.
    Random(Range<f32>),
}

impl ValuesStructure {
    /// Return a [`ValuesStructure::Random`] with the largest value range that can be used in
    /// [`Uniform`] without overflowing as described in GitHub issue [rand #1380].
    ///
    /// [rand #1380]: https://github.com/rust-random/rand/issues/1380
    pub fn largest_random_without_overflow() -> Self {
        ValuesStructure::Random(f32::MIN / 2.0..f32::MAX / 2.0)
    }
}

/// Return a [`StdRng`] with [`RANDOM_NUMBER_SEED`] as the seed. [`StdRng`] is used instead of
/// [`ThreadRng`](rand::rngs::ThreadRng) as [`ThreadRng`](rand::rngs::ThreadRng) automatically
/// reseeds. A new [`StdRng`] with `RANDOM_NUMBER_SEED` as the seed is created each time the
/// function is run to ensure the order in which the tests are executed does not change the data
/// each test receives so the exact same tests are executed each time when a static seed is used.
fn create_random_number_generator() -> StdRng {
    StdRng::from_seed(*RANDOM_NUMBER_SEED)
}

/// Generate a univariate time series with sub-sequences of values with different
/// [`ValuesStructure`]. The time series will have `length` data points in sequences of
/// `segment_length_range` (except possibly for the last as it may be truncated to match `length`)
/// and the timestamps will be regular or irregular depending on the value of
/// `generate_irregular_timestamps`. If `add_noise_range` is [`Some`], random values will be
/// generated in the [`Range<f32>`] and multiplied with each value in the sequences with constant
/// and linear values. Sequences with random values are generated in the range specified as
/// `random_value_range`.
pub fn generate_univariate_time_series(
    length: usize,
    segment_length_range: Range<usize>,
    generate_irregular_timestamps: bool,
    add_noise_range: Option<Range<f32>>,
    random_value_range: Range<f32>,
) -> (TimestampArray, ValueArray) {
    let (uncompressed_timestamps, mut uncompressed_values) = generate_multivariate_time_series(
        length,
        1,
        segment_length_range,
        generate_irregular_timestamps,
        add_noise_range,
        random_value_range,
    );

    (uncompressed_timestamps, uncompressed_values.remove(0))
}

/// Generate a univariate or multivariate time series with sub-sequences of values with different
/// [`ValuesStructure`]. The time series will have `field_columns` columns containing `length` data
/// points in sequences of `segment_length_range` (except possibly for the last as it may be
/// truncated to match `length`) and the timestamps will be regular or irregular depending on the
/// value of `generate_irregular_timestamps`. If `add_noise_range` is [`Some`], random values will
/// be generated in the [`Range<f32>`] and added to each value in the sequences with constant and
/// linear values. Sequences with random values are generated in the range specified as
/// `random_value_range`.
pub fn generate_multivariate_time_series(
    length: usize,
    field_columns: usize,
    segment_length_range: Range<usize>,
    generate_irregular_timestamps: bool,
    add_noise_range: Option<Range<f32>>,
    random_value_range: Range<f32>,
) -> (TimestampArray, Vec<ValueArray>) {
    let values_structures = &[
        ValuesStructure::Constant(add_noise_range.clone()),
        ValuesStructure::Linear(add_noise_range),
        ValuesStructure::Random(random_value_range),
    ];

    // Create value builders.
    let mut uncompressed_values_builders = (0..field_columns)
        .map(|_| ValueBuilder::with_capacity(length))
        .collect::<Vec<_>>();
    let mut value_columns = Vec::with_capacity(uncompressed_values_builders.len());

    // Generate timestamps.
    let uncompressed_timestamps = generate_timestamps(length, generate_irregular_timestamps);

    // Generates values.
    let mut std_rng = create_random_number_generator();
    while !uncompressed_values_builders.is_empty() {
        let mut uncompressed_values_builders_to_delete =
            Vec::with_capacity(uncompressed_values_builders.len());

        for (index, uncompressed_values_builder) in
            uncompressed_values_builders.iter_mut().enumerate()
        {
            let segment_length = std_rng.random_range(segment_length_range.clone());
            let values_structure_index = std_rng.random_range(0..values_structures.len());
            let values_structure = &values_structures[values_structure_index];

            let uncompressed_values_builder_len = uncompressed_values_builder.values_slice().len();
            let uncompressed_timestamps_for_segment_end = usize::min(
                uncompressed_timestamps.len(),
                uncompressed_values_builder_len + segment_length,
            );
            let uncompressed_timestamps_for_segment = &uncompressed_timestamps.values()
                [uncompressed_values_builder_len..uncompressed_timestamps_for_segment_end];

            let uncompressed_values = generate_values(
                uncompressed_timestamps_for_segment,
                (*values_structure).clone(),
            );

            uncompressed_values_builder.extend(&uncompressed_values);

            // Ignore builder with enough values.
            if uncompressed_values_builder.values_slice().len() >= length {
                value_columns.push(uncompressed_values_builder.finish().slice(0, length));
                uncompressed_values_builders_to_delete.push(index);
            }
        }

        // Builders are deleted after the loop and in reverse order to not change the indices.
        uncompressed_values_builders_to_delete
            .iter()
            .rev()
            .for_each(|index| {
                uncompressed_values_builders.swap_remove(*index);
            });
    }

    (uncompressed_timestamps, value_columns)
}

/// Generate regular/irregular timestamps with [ThreadRng](rand::rngs::ThreadRng). Selects the
/// length and type of timestamps to be generated using the parameters `length` and `irregular`.
/// Returns the generated timestamps as a [`TimestampArray`].
pub fn generate_timestamps(length: usize, irregular: bool) -> TimestampArray {
    if irregular {
        let mut std_rng = create_random_number_generator();
        let mut timestamps = TimestampBuilder::with_capacity(length);
        let mut next_timestamp: i64 = 0;
        for _ in 0..length {
            timestamps.append_value(next_timestamp);
            next_timestamp += std_rng.sample(Uniform::try_from(100..200).unwrap());
        }
        timestamps.finish()
    } else {
        TimestampArray::from_iter_values((0..length as i64 * 100).step_by(100))
    }
}

/// Generate multiple test values with a specific structure using
/// [ThreadRng](rand::rngs::ThreadRng). The amount of values to be generated will match `timestamps`
/// and their structure will match `values_structure`:
/// - If `values_structure` is `Constant`, a single value is generated and repeated with a random
///   value in the associated range multiplied with each value if it is not [`None`].
/// - If `values_structure` is `Linear`, a sequence of increasing or decreasing values are generated
///   with a random value in the associated range multiplied with each value if it is not [`None`].
/// - If `values_structure` is `Random`, a sequence of random values in the associated range are
///   generated.
pub fn generate_values(
    uncompressed_timestamps: &[Timestamp],
    values_structure: ValuesStructure,
) -> ValueArray {
    let mut std_rng = create_random_number_generator();
    match values_structure {
        // Generates constant values.
        ValuesStructure::Constant(maybe_add_noise_range) => {
            let mut values = iter::repeat_n(std_rng.random(), uncompressed_timestamps.len());
            randomize_and_collect_iterator(maybe_add_noise_range, &mut values)
        }
        // Generates linear values.
        ValuesStructure::Linear(maybe_add_noise_range) => {
            // The variable slope is regenerated if it is 0, to avoid generating constant data.
            let mut slope: i64 = 0;
            while slope == 0 {
                slope = std_rng.random_range(-10..10);
            }
            let intercept: i64 = std_rng.random_range(1..50);

            let mut values = uncompressed_timestamps
                .iter()
                .map(|timestamp| (slope * timestamp + intercept) as f32);

            randomize_and_collect_iterator(maybe_add_noise_range, &mut values)
        }
        // Generates random values.
        ValuesStructure::Random(min_max) => {
            let distribution = Uniform::try_from(min_max).unwrap();
            uncompressed_timestamps
                .iter()
                .map(|_| std_rng.sample(distribution))
                .collect()
        }
    }
}

/// Add the value in `maybe_noise_range` to each value in `values` if `maybe_noise_range` is not
/// [`None`] and collect it to a [`Vec<f32>`] which is returned.
fn randomize_and_collect_iterator(
    maybe_noise_range: Option<Range<f32>>,
    values: &mut dyn Iterator<Item = f32>,
) -> ValueArray {
    let mut std_rng = create_random_number_generator();
    if let Some(noise_range) = maybe_noise_range {
        let distribution = Uniform::try_from(noise_range).unwrap();
        values
            .map(|value| value + std_rng.sample(distribution))
            .collect()
    } else {
        values.collect()
    }
}
