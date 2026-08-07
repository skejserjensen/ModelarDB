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

//! Implementation of types for reading and writing bits from and to a sequence
//! of bytes.

use crate::error::{ModelarDbCompressionError, Result};

/// Read one or multiple bits from a `[u8]`. [`BitReader`] is implemented based
/// on [code published by Ilkka Rauta] dual-licensed under MIT and Apache2.
///
/// [code published by Ilkka Rauta]: https://github.com/irauta/bitreader
pub struct BitReader<'a> {
    /// Next bit to read from `bytes`.
    next_bit: usize,
    /// Bits packed into one or more [`u8s`](u8).
    bytes: &'a [u8],
}

impl<'a> BitReader<'a> {
    /// Return a [`BitReader`] if `bytes` is not empty, otherwise [`String`].
    pub fn try_new(bytes: &'a [u8]) -> Result<Self> {
        if bytes.is_empty() {
            Err(ModelarDbCompressionError::InvalidArgument(
                "The bytes slice must not be empty.".to_owned(),
            ))
        } else {
            Ok(Self { next_bit: 0, bytes })
        }
    }

    /// Return [`true`] if the reader have been exhausted, otherwise [`false`].
    pub fn is_empty(&self) -> bool {
        (self.next_bit / 8) == self.bytes.len()
    }

    /// Return the remaining bits in the [`BitReader`].
    pub fn remaining_bits(&self) -> usize {
        8 * self.bytes.len() - self.next_bit
    }

    /// Read the next bit from the [`BitReader`].
    pub fn read_bit(&mut self) -> bool {
        self.read_bits(1) == 1
    }

    /// Read the next `number_of_bits` bits from the [`BitReader`]. Assumes that
    /// `number_of_bits` is less than or equal to 64.
    pub fn read_bits(&mut self, number_of_bits: u8) -> u64 {
        debug_assert!(
            number_of_bits <= 64,
            "The number of bits to read must be less than or equal to 64."
        );

        let mut value = 0;
        let start_bit = self.next_bit;
        let end_bit = self.next_bit + number_of_bits as usize;
        for bit in start_bit..end_bit {
            // Read the byte storing the bit, compute the location of the bit in
            // the byte, shift the bit in the byte to the rightmost position,
            // clear the other bits in the byte, shift the output value one
            // position to the left, and set the rightmost bit to the read bit.
            let byte = self.bytes[bit / 8];
            let shift = 7 - (bit % 8);
            let bit = (byte >> shift) as u64 & 1;
            value = (value << 1) | bit;
        }
        self.next_bit = end_bit;
        value
    }
}

/// Append one or multiple bits to a [`Vec<u8>`].
pub struct BitVecBuilder {
    /// [`u8`] currently used for storing the bits.
    current_byte: u8,
    /// Bits remaining in `current_byte`.
    remaining_bits: u8,
    /// Bits packed into one or more [`u8s`](u8).
    bytes: Vec<u8>,
}

impl BitVecBuilder {
    pub fn new() -> Self {
        Self {
            current_byte: 0,
            remaining_bits: 8,
            bytes: vec![],
        }
    }

    /// Append a zero bit to the [`BitVecBuilder`].
    pub fn append_a_zero_bit(&mut self) {
        self.append_bits(0, 1)
    }

    /// Append a one bit to the [`BitVecBuilder`].
    pub fn append_a_one_bit(&mut self) {
        self.append_bits(1, 1)
    }

    /// Append `number_of_bits` from `bits` to the [`BitVecBuilder`].
    pub fn append_bits(&mut self, bits: u64, number_of_bits: u8) {
        let mut number_of_bits = number_of_bits;

        while number_of_bits > 0 {
            let bits_written = if number_of_bits > self.remaining_bits {
                // Write the next self.remaining bits from bits to self.current_byte.
                let shift = number_of_bits - self.remaining_bits;
                self.current_byte |= ((bits >> shift) & ((1 << self.remaining_bits) - 1)) as u8;
                self.remaining_bits
            } else {
                // Write the remaining number_of_bits bits from bits to self.current_byte.
                let shift = self.remaining_bits - number_of_bits;
                let mask = (u8::MAX >> (8 - self.remaining_bits)) as u64;
                self.current_byte |= ((bits << shift) & mask) as u8;
                number_of_bits
            };
            number_of_bits -= bits_written;
            self.remaining_bits -= bits_written;

            // Store self.current_byte if it is full and set its bits to zero.
            if self.remaining_bits == 0 {
                self.bytes.push(self.current_byte);
                self.current_byte = 0;
                self.remaining_bits = 8;
            }
        }
    }

    /// Return [`true`] if no bits have been appended to the [`BitVecBuilder`],
    /// otherwise [`false`].
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Return the number of bytes required to store the appended bits.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.bytes.len() + (self.remaining_bits != 8) as usize
    }

    /// Consume the [`BitVecBuilder`] and return the appended bits packed into a
    /// [`Vec<u8>`].
    pub fn finish(mut self) -> Vec<u8> {
        if self.remaining_bits != 8 {
            self.bytes.push(self.current_byte);
        }
        self.bytes
    }

    /// Set the remaining bits to one in the byte [`BitVecBuilder`] is currently
    /// packing bits into. Then consume the [`BitVecBuilder`] and return the
    /// appended bits packed into a [`Vec<u8>`].
    pub fn finish_with_one_bits(mut self) -> Vec<u8> {
        if self.remaining_bits != 8 {
            let remaining_bits_to_set = 2_u8.pow(self.remaining_bits as u32) - 1;
            self.append_bits(remaining_bits_to_set as u64, self.remaining_bits);
        }
        self.finish()
    }
}

impl Default for BitVecBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{bool, collection, prop_assert, prop_assume, proptest};

    // The largest byte, a random byte, and the smallest byte for testing.
    const TEST_BYTES: &[u8] = &[255, 170, 0];
    const TEST_BITS: &[bool] = &[
        true, true, true, true, true, true, true, true, true, false, true, false, true, false,
        true, false, false, false, false, false, false, false, false, false,
    ];

    // Tests for BitReader.
    #[test]
    fn test_bit_reader_cannot_be_empty() {
        assert!(BitReader::try_new(&[]).is_err());
    }

    #[test]
    fn test_reading_the_test_bits() {
        assert!(bytes_and_bits_are_equal(TEST_BYTES, TEST_BITS));
    }

    #[test]
    pub fn test_remaining_bits() {
        let mut bits = BitReader::try_new(&[0, 255]).unwrap();
        assert_eq!(bits.remaining_bits(), 16);
        bits.read_bits(4);
        assert_eq!(bits.remaining_bits(), 12);
        bits.read_bits(8);
        assert_eq!(bits.remaining_bits(), 4);
        bits.read_bits(4);
        assert_eq!(bits.remaining_bits(), 0);
        assert!(bits.is_empty());
    }

    // Tests for BitVecBuilder.
    #[test]
    fn test_empty_bit_vec_builder_is_empty() {
        assert!(BitVecBuilder::new().finish().is_empty());
    }

    #[test]
    fn test_empty_bit_vec_builder_len() {
        assert_eq!(BitVecBuilder::new().len(), 0);
    }

    #[test]
    fn test_one_one_bit_vec_builder_len() {
        let mut bit_vec_builder = BitVecBuilder::new();
        bit_vec_builder.append_a_one_bit();
        assert_eq!(bit_vec_builder.len(), 1);
    }

    #[test]
    fn test_one_zero_bit_vec_builder_len() {
        let mut bit_vec_builder = BitVecBuilder::new();
        bit_vec_builder.append_a_zero_bit();
        assert_eq!(bit_vec_builder.len(), 1);
    }

    #[test]
    fn test_eight_one_bits_vec_builder_len() {
        let mut bit_vec_builder = BitVecBuilder::new();
        (0..8).for_each(|_| bit_vec_builder.append_a_one_bit());
        assert_eq!(bit_vec_builder.len(), 1);
    }

    #[test]
    fn test_eight_zero_bits_vec_builder_len() {
        let mut bit_vec_builder = BitVecBuilder::new();
        (0..8).for_each(|_| bit_vec_builder.append_a_zero_bit());
        assert_eq!(bit_vec_builder.len(), 1);
    }

    #[test]
    fn test_nine_one_bits_vec_builder_len() {
        let mut bit_vec_builder = BitVecBuilder::new();
        (0..9).for_each(|_| bit_vec_builder.append_a_one_bit());
        assert_eq!(bit_vec_builder.len(), 2);
    }

    #[test]
    fn test_nine_zero_bits_vec_builder_len() {
        let mut bit_vec_builder = BitVecBuilder::new();
        (0..9).for_each(|_| bit_vec_builder.append_a_zero_bit());
        assert_eq!(bit_vec_builder.len(), 2);
    }

    #[test]
    fn test_finish_with_one_bits_empty_no_effect() {
        let bit_vec_builder = BitVecBuilder::new();
        let bytes = bit_vec_builder.finish_with_one_bits();
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_finish_with_one_bits_full_no_effect() {
        let mut bit_vec_builder = BitVecBuilder::new();
        bit_vec_builder.append_bits(255, 8);
        let bytes = bit_vec_builder.finish_with_one_bits();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], 255);
    }

    #[test]
    fn test_finish_with_one_bits_filling_current() {
        let mut bit_vec_builder = BitVecBuilder::new();
        bit_vec_builder.append_bits(15, 4);
        let bytes = bit_vec_builder.finish_with_one_bits();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], 255);
    }

    // Tests combining BitReader and BitVecBuilder.
    #[test]
    fn test_writing_and_reading_the_test_bits() {
        let mut bit_vector_builder = BitVecBuilder::new();
        for bit in TEST_BITS {
            write_bool_as_bit(&mut bit_vector_builder, *bit);
        }
        assert!(bytes_and_bits_are_equal(
            &bit_vector_builder.finish(),
            TEST_BITS
        ));
    }

    proptest! {
    #[test]
    fn test_writing_and_reading_random_bits(bits in collection::vec(bool::ANY, 0..50)) {
        prop_assume!(!bits.is_empty());
        let mut bit_vector_builder = BitVecBuilder::new();
        for bit in &bits {
            write_bool_as_bit(&mut bit_vector_builder, *bit);
        }
        prop_assert!(bytes_and_bits_are_equal(&bit_vector_builder.finish(), &bits));
    }
    }

    fn bytes_and_bits_are_equal(bytes: &[u8], bits: &[bool]) -> bool {
        let mut bit_reader = BitReader::try_new(bytes).unwrap();
        let mut contains = true;
        for bit in bits {
            contains &= *bit == bit_reader.read_bit();
        }
        contains
    }

    fn write_bool_as_bit(bit_vector_builder: &mut BitVecBuilder, bit: bool) {
        if bit {
            bit_vector_builder.append_a_one_bit();
        } else {
            bit_vector_builder.append_a_zero_bit();
        }
    }
}
