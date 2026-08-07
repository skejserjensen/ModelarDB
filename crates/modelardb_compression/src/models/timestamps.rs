/* Copyright 2022 The ModelarDB Contributors
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

//! Implementation of lossless compression for timestamps. Optimized compression
//! methods are used depending on the number of data points in a compressed
//! segment and if its timestamps have been sampled at a regular sampling
//! interval.
//!
//! If a segment only contains one data point its timestamp is stored as both
//! the segment's `start_time` and `end_time`, and if a segment only contains
//! two data points the timestamps are stored as the segment's `start_time` and
//! `end_time`, respectively. If a segment contains more than two data points,
//! the first and last timestamps are stored as the segment's `start_time` and
//! `end_time`, respectively, while its residual timestamps are compressed using
//! one of two methods. If the data points in the segment have been collected at
//! a regular sampling interval, the residual timestamps are compressed as the
//! segment's length with the prefix zero bits stripped. If none of the above
//! apply, the compression method proposed for timestamps for the time series
//! management system Gorilla in the [Gorilla paper] is used as a fallback.
//!
//! [Gorilla paper]: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf

use std::mem;

use modelardb_types::types::{Timestamp, TimestampBuilder};

use crate::models::bits::{BitReader, BitVecBuilder};

/// Compress the timestamps in `uncompressed_timestamps` from the second
/// timestamp to the second to last timestamp. The first and last timestamp are
/// already stored as part of the compressed segment to allow segments to be
/// pruned. If the time series is regular, the timestamps are encoded as the
/// number of data points in the segment with prefix zeros stripped, and if it
/// is irregular, the timestamp's delta-of-delta is computed and then encoded
/// using a variable length binary encoding. The first bit that is written to
/// the returned [`Vec`] is a flag that indicates if the time series was regular
/// (a zero bit is written) or if it was irregular (a one bit is written).
pub fn compress_residual_timestamps(uncompressed_timestamps: &[Timestamp]) -> Vec<u8> {
    // Nothing to do as the segments already store the first and last timestamp.
    if uncompressed_timestamps.len() <= 2 {
        return vec![];
    }

    // Compress the residual timestamps using an optimized method depending on
    // if the segment the timestamps are from is regular or irregular.
    if are_uncompressed_timestamps_regular(uncompressed_timestamps) {
        // The timestamps are regular, so only the segment's length is stored as
        // an integer with all the prefix zeros stripped from the integer.
        compress_regular_residual_timestamps(uncompressed_timestamps)
    } else {
        // The timestamps are irregular, so they are compressed as
        // delta-of-deltas stored using a variable length binary encoding.
        compress_irregular_residual_timestamps(uncompressed_timestamps)
    }
}

/// Return [`true`] if the timestamps in `uncompressed_timestamps` follow a
/// regular sampling interval, otherwise [`false`].
fn are_uncompressed_timestamps_regular(uncompressed_timestamps: &[Timestamp]) -> bool {
    if uncompressed_timestamps.len() < 2 {
        return true;
    }

    // unwrap() is safe as uncompressed_timestamps contains at least two timestamps.
    let expected_sampling_interval = uncompressed_timestamps[1] - uncompressed_timestamps[0];
    let mut uncompressed_timestamps = uncompressed_timestamps.iter();
    let mut previous_timestamp = uncompressed_timestamps.next().unwrap();

    for current_timestamp in uncompressed_timestamps {
        if current_timestamp - previous_timestamp != expected_sampling_interval {
            return false;
        }
        previous_timestamp = current_timestamp;
    }
    true
}

/// Compress `uncompressed_timestamps` for a regular time series as a zero bit
/// followed by the segment's length as an integer with prefix zeros stripped.
fn compress_regular_residual_timestamps(uncompressed_timestamps: &[Timestamp]) -> Vec<u8> {
    let length = uncompressed_timestamps.len();
    let leading_zero_bits = length.leading_zeros() as usize;
    let number_of_bits_to_write = (mem::size_of_val(&length) * 8 - leading_zero_bits) + 1;
    let number_of_bytes_to_write = (number_of_bits_to_write as f64 / 8.0).ceil() as usize;

    let sampling_interval_bytes = length.to_be_bytes();
    let bytes_index_first_byte = sampling_interval_bytes.len() - number_of_bytes_to_write;
    sampling_interval_bytes[bytes_index_first_byte..].to_vec()
}

/// Compress `uncompressed_timestamps` from an irregular time series as a one
/// bit followed by the timestamps' delta-of-deltas encode using a variable
/// length binary encoding.
fn compress_irregular_residual_timestamps(uncompressed_timestamps: &[Timestamp]) -> Vec<u8> {
    let mut compressed_timestamps = BitVecBuilder::new();
    compressed_timestamps.append_a_one_bit();

    // Encode the timestamps from the second timestamp to the second to last. A delta-of-delta is
    // computed and then encoded in buckets. Zero is used as the delta for the first delta-of-delta.
    let mut last_timestamp = uncompressed_timestamps[0];
    let mut last_delta = 0;
    for timestamp in &uncompressed_timestamps[1..uncompressed_timestamps.len() - 1] {
        let delta = timestamp - last_timestamp;
        let delta_of_delta = delta - last_delta;

        match delta_of_delta {
            0 => compressed_timestamps.append_a_zero_bit(),
            -63..=64 => {
                compressed_timestamps.append_bits(0b10, 2);
                compressed_timestamps.append_bits(delta_of_delta as u64, 7);
            }
            -255..=256 => {
                compressed_timestamps.append_bits(0b110, 3);
                compressed_timestamps.append_bits(delta_of_delta as u64, 9);
            }
            -2047..=2048 => {
                compressed_timestamps.append_bits(0b1110, 4);
                compressed_timestamps.append_bits(delta_of_delta as u64, 12);
            }
            -2147483647..=2147483648 => {
                compressed_timestamps.append_bits(0b11110, 5);
                compressed_timestamps.append_bits(delta_of_delta as u64, 32);
            }
            _ => {
                compressed_timestamps.append_bits(0b11111, 5);
                compressed_timestamps.append_bits(delta_of_delta as u64, 64);
            }
        }
        last_delta = delta;
        last_timestamp = *timestamp;
    }

    // All remaining bits in the byte the BitVecBuilder is currently packing
    // bits into is set to one to indicate that all timestamps are decompressed.
    compressed_timestamps.finish_with_one_bits()
}

/// Decompress all of a segment's timestamps which are compressed as
/// `start_time` for segments of length one, `start_time` and `end_time` for
/// segments of length two, the segment's length for regular time series, or
/// using Gorilla's compression method for timestamps for irregular time series.
/// The decompressed timestamps are appended to `timestamp_builder`.
pub fn decompress_all_timestamps(
    start_time: Timestamp,
    end_time: Timestamp,
    residual_timestamps: &[u8],
    timestamp_builder: &mut TimestampBuilder,
) {
    if residual_timestamps.is_empty() && start_time == end_time {
        // Timestamps are assumed to be unique so the segment has one timestamp.
        timestamp_builder.append_value(start_time);
    } else if residual_timestamps.is_empty() {
        // Timestamps are assumed to be unique so the segment has two timestamp.
        timestamp_builder.append_value(start_time);
        timestamp_builder.append_value(end_time);
    } else if are_compressed_timestamps_regular(residual_timestamps) {
        // The flag bit is zero, so only the segment's length is stored as an
        // integer with all the prefix zeros stripped from the integer.
        decompress_all_regular_timestamps(
            start_time,
            end_time,
            residual_timestamps,
            timestamp_builder,
        )
    } else {
        // The flag bit is one, so the timestamps are compressed as
        // delta-of-deltas stored using a variable length binary encoding.
        decompress_all_irregular_timestamps(
            start_time,
            end_time,
            residual_timestamps,
            timestamp_builder,
        )
    }
}

/// Return [`true`] if the timestamps in `residual_timestamps` follow a regular
/// sampling interval, otherwise [`false`].
pub fn are_compressed_timestamps_regular(residual_timestamps: &[u8]) -> bool {
    // The flag bit is zero so the timestamps follow a regular interval.
    residual_timestamps.is_empty() || residual_timestamps[0] & 128 == 0
}

/// Decompress all of a segment's timestamps, which for this segment are sampled
/// at a regular sampling interval, and thus compressed as the segment's length.
/// The decompressed timestamps are appended to `timestamp_builder`.
fn decompress_all_regular_timestamps(
    start_time: Timestamp,
    end_time: Timestamp,
    residual_timestamps: &[u8],
    timestamp_builder: &mut TimestampBuilder,
) {
    let mut bytes_to_decode = [0; 8];
    let bytes_to_decode_len = bytes_to_decode.len();
    bytes_to_decode[bytes_to_decode_len - residual_timestamps.len()..]
        .copy_from_slice(residual_timestamps);

    let length = usize::from_be_bytes(bytes_to_decode);
    let sampling_interval = (end_time - start_time) as usize / (length - 1);
    for timestamp in (start_time..=end_time).step_by(sampling_interval) {
        timestamp_builder.append_value(timestamp);
    }
}

/// Decompress all of a segment's timestamps, which for this segment are sampled
/// at an irregular sampling interval, and thus compressed using Gorilla's
/// compression method for timestamps. The decompressed timestamps are appended
/// to `timestamp_builder`.
fn decompress_all_irregular_timestamps(
    start_time: Timestamp,
    end_time: Timestamp,
    residual_timestamps: &[u8],
    timestamp_builder: &mut TimestampBuilder,
) {
    // Add the first timestamp stored as `start_time` in the segment.
    timestamp_builder.append_value(start_time);

    // Remove the one bit used as a flag to specify that Gorilla is used.
    let mut bits = BitReader::try_new(residual_timestamps).unwrap();
    bits.read_bit();

    // Decompress the residual timestamps.
    let mut last_delta = 0;
    let mut timestamp = start_time;
    while !bits.is_empty() {
        // Read the next flag with the value of 0, 10, 110, 1110, 11110, or 11111.
        let mut leading_one_bits = 0;
        while leading_one_bits < 5 && !bits.is_empty() && bits.read_bit() {
            leading_one_bits += 1;
        }

        // Any leftover bits in residual_timestamps are set to one. Thus, a
        // sequence of one bits followed by fewer bits than specified by the
        // flag means that all residual timestamps have been decompressed.
        if leading_one_bits != 0 && bits.remaining_bits() < 7 {
            break;
        }

        let delta = match leading_one_bits {
            0 => last_delta,                                               // Flag is 0.
            1 => read_decode_and_compute_delta(&mut bits, 7, last_delta),  // Flag is 10.
            2 => read_decode_and_compute_delta(&mut bits, 9, last_delta),  // Flag is 110.
            3 => read_decode_and_compute_delta(&mut bits, 12, last_delta), // Flag is 1110.
            4 => read_decode_and_compute_delta(&mut bits, 32, last_delta), // Flag is 11110.
            5 => read_decode_and_compute_delta(&mut bits, 64, last_delta), // Flag is 11111.
            _ => panic!("Unknown timestamp encoding with {leading_one_bits} leading one bits."),
        };

        timestamp += delta as i64;
        timestamp_builder.append_value(timestamp);
        last_delta = delta;
    }

    // Add the last timestamp stored as `end_time` in the segment.
    timestamp_builder.append_value(end_time);
}

/// Read the next delta-of-delta as `bits_to_read` from `bits`, decode the
/// delta-of-delta, and add it to `last_delta` to compute the next delta.
/// [`read_decode_and_compute_delta`] is implemented based on [code published by
/// Jerome Froelich] under MIT.
///
/// [code published by Jerome Froelich]: https://github.com/jeromefroe/tsz-rs
fn read_decode_and_compute_delta(bits: &mut BitReader, bits_to_read: u8, last_delta: u64) -> u64 {
    let encoded_delta_of_delta = bits.read_bits(bits_to_read);
    let delta_of_delta = if encoded_delta_of_delta > (1 << (bits_to_read - 1)) {
        encoded_delta_of_delta | (u128::MAX << bits_to_read) as u64
    } else {
        encoded_delta_of_delta
    };
    // wrapping_add() ensure negative values are handled correctly.
    last_delta.wrapping_add(delta_of_delta)
}

#[cfg(test)]
mod tests {
    use super::*;

    use modelardb_test::data_generation;
    use proptest::num::i64 as ProptestTimestamp;
    use proptest::{collection, proptest};

    // Tests for compress_residual_timestamps() and decompress_all_timestamps().
    #[test]
    fn test_compress_timestamps_for_time_series_with_zero_one_or_two_timestamps() {
        let mut uncompressed_timestamps_builder = TimestampBuilder::with_capacity(3);

        uncompressed_timestamps_builder.append_slice(&[]);
        let uncompressed_timestamps = &uncompressed_timestamps_builder.finish();
        assert!(compress_residual_timestamps(uncompressed_timestamps.values()).is_empty());

        uncompressed_timestamps_builder.append_slice(&[100]);
        let uncompressed_timestamps = &uncompressed_timestamps_builder.finish();
        assert!(compress_residual_timestamps(uncompressed_timestamps.values()).is_empty());

        uncompressed_timestamps_builder.append_slice(&[100, 300]);
        let uncompressed_timestamps = &uncompressed_timestamps_builder.finish();
        assert!(compress_residual_timestamps(uncompressed_timestamps.values()).is_empty());
    }

    #[test]
    fn test_compress_and_decompress_timestamps_for_a_regular_time_series() {
        compress_and_decompress_timestamps_for_a_time_series(
            &[
                1579701905500,
                1579701905600,
                1579701905700,
                1579701905800,
                1579701905900,
            ],
            Some(1),
        );
    }

    #[test]
    fn test_compress_and_decompress_timestamps_for_an_irregular_time_series() {
        compress_and_decompress_timestamps_for_a_time_series(
            &[
                1579694400057,
                1579694400197,
                1579694400353,
                1579694400493,
                1579694400650,
            ],
            Some(4),
        );
    }

    #[test]
    fn test_compress_and_decompress_bucket_sized_1_timestamps_for_an_irregular_time_series() {
        compress_and_decompress_timestamps_for_a_time_series(
            &[
                100, 100, // 0.
                200,
            ],
            Some(1),
        );
    }

    #[test]
    fn test_compress_and_decompress_bucket_sized_7_timestamps_for_an_irregular_time_series() {
        compress_and_decompress_timestamps_for_a_time_series(
            &[
                100, 37, // -63.
                38, // 64.
                200,
            ],
            Some(3),
        );
    }

    #[test]
    fn test_compress_and_decompress_bucket_sized_9_timestamps_for_an_irregular_time_series() {
        compress_and_decompress_timestamps_for_a_time_series(
            &[
                500, 245, // -255.
                246, // 256.
                500,
            ],
            Some(4),
        );
    }

    #[test]
    fn test_compress_and_decompress_bucket_sized_12_timestamps_for_an_irregular_time_series() {
        compress_and_decompress_timestamps_for_a_time_series(
            &[
                5000, 2953, // -2047.
                2954, // 2048.
                5000,
            ],
            Some(5),
        );
    }

    #[test]
    fn test_compress_and_decompress_bucket_sized_32_timestamps_for_an_irregular_time_series() {
        compress_and_decompress_timestamps_for_a_time_series(
            &[
                5000000000, 2852516353, // -2147483647.
                2852516354, // 2147483648.
                5000000000,
            ],
            Some(10),
        );
    }

    #[test]
    fn test_compress_and_decompress_timestamps_for_a_generated_regular_time_series() {
        let timestamps = data_generation::generate_timestamps(1000, false);
        compress_and_decompress_timestamps_for_a_time_series(timestamps.values(), None);
    }

    #[test]
    fn test_compress_and_decompress_timestamps_for_a_generated_irregular_time_series() {
        let timestamps = data_generation::generate_timestamps(1000, true);
        compress_and_decompress_timestamps_for_a_time_series(timestamps.values(), None);
    }

    proptest! {
    #[test]
    fn test_compress_and_decompress_timestamps_for_a_random_irregular_time_series(timestamps in collection::vec(ProptestTimestamp::ANY, 1..50)) {
        let mut timestamps = timestamps.iter().map(|ts| ts.abs()).collect::<Vec<i64>>();
        timestamps.sort();
        compress_and_decompress_timestamps_for_a_time_series(&timestamps, None);
        }
    }

    fn compress_and_decompress_timestamps_for_a_time_series(
        uncompressed_timestamps: &[Timestamp],
        maybe_known_size: Option<usize>,
    ) {
        let mut uncompressed_timestamps_builder =
            TimestampBuilder::with_capacity(uncompressed_timestamps.len());
        uncompressed_timestamps_builder.append_slice(uncompressed_timestamps);
        let uncompressed_timestamps = uncompressed_timestamps_builder.finish();

        let compressed = compress_residual_timestamps(uncompressed_timestamps.values());
        assert!(uncompressed_timestamps.len() <= 2 || !compressed.is_empty());
        assert!(uncompressed_timestamps.len() > 2 || compressed.is_empty());

        if let Some(known_size) = maybe_known_size {
            assert_eq!(compressed.len(), known_size);
        }

        let mut decompressed_timestamps = TimestampBuilder::with_capacity(10);
        decompress_all_timestamps(
            uncompressed_timestamps.value(0),
            uncompressed_timestamps.value(uncompressed_timestamps.len() - 1),
            compressed.as_slice(),
            &mut decompressed_timestamps,
        );
        assert_eq!(uncompressed_timestamps, decompressed_timestamps.finish());
    }

    // Tests for are_timestamps_regular().
    #[test]
    fn test_time_series_with_one_data_point_is_regular() {
        assert!(are_uncompressed_timestamps_regular(&[100]));
    }

    #[test]
    fn test_time_series_with_two_data_points_is_regular() {
        assert!(are_uncompressed_timestamps_regular(&[100, 200]));
    }

    #[test]
    fn test_regular_time_series_is_regular() {
        assert!(are_uncompressed_timestamps_regular(&[
            100, 200, 300, 400, 500, 600, 700
        ]))
    }

    #[test]
    fn test_irregular_time_series_is_irregular() {
        assert!(!are_uncompressed_timestamps_regular(&[
            100, 150, 300, 350, 700, 750, 1500
        ]))
    }
}
