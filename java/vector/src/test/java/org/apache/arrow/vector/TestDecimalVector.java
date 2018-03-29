/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.DecimalUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDecimalVector {

  private static long[] intValues;

  static {
    intValues = new long[60];
    for (int i = 0; i < intValues.length / 2; i++) {
      intValues[i] = 1 << i + 1;
      intValues[2 * i] = -1 * (1 << i + 1);
    }
  }

  private int scale = 3;

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testValuesWriteRead() {
    try (NullableDecimalVector decimalVector = TestUtils.newVector(NullableDecimalVector.class, "decimal", new ArrowType.Decimal(10, scale), allocator);) {

      try (NullableDecimalVector oldConstructor = new NullableDecimalVector("decimal", allocator, 10, scale);) {
        assertEquals(decimalVector.getField().getType(), oldConstructor.getField().getType());
      }

      decimalVector.allocateNew();
      BigDecimal[] values = new BigDecimal[intValues.length];
      for (int i = 0; i < intValues.length; i++) {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(intValues[i]), scale);
        values[i] = decimal;
        decimalVector.setSafe(i, decimal);
      }

      decimalVector.setValueCount(intValues.length);

      for (int i = 0; i < intValues.length; i++) {
        BigDecimal value = decimalVector.getObject(i);
        assertEquals("unexpected data at index: " + i, values[i], value);
      }
    }
  }

  @Test
  public void testBigDecimalDifferentScaleAndPrecision() {
    try (NullableDecimalVector decimalVector = TestUtils.newVector(NullableDecimalVector.class, "decimal", new ArrowType.Decimal(4, 2), allocator);) {
      decimalVector.allocateNew();

      // test BigDecimal with different scale
      boolean hasError = false;
      try {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(0), 3);
        decimalVector.setSafe(0, decimal);
      } catch (UnsupportedOperationException ue) {
        hasError = true;
      } finally {
        assertTrue(hasError);
      }

      // test BigDecimal with larger precision than initialized
      hasError = false;
      try {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(12345), 2);
        decimalVector.setSafe(0, decimal);
      } catch (UnsupportedOperationException ue) {
        hasError = true;
      } finally {
        assertTrue(hasError);
      }
    }
  }

  /**
   * Test {@link NullableDecimalVector#setBigEndian(int, byte[])} which takes BE layout input and stores in LE layout.
   * Cases to cover: value given in byte array in different lengths in range [1-16] and negative values.
   */
  @Test
  public void decimalBE2LE() {
    try (NullableDecimalVector decimalVector = TestUtils.newVector(NullableDecimalVector.class, "decimal", new ArrowType.Decimal(21, 2), allocator);) {
      decimalVector.allocateNew();

      BigInteger[] testBigInts = new BigInteger[] {
          new BigInteger("0"),
          new BigInteger("-1"),
          new BigInteger("23"),
          new BigInteger("234234"),
          new BigInteger("-234234234"),
          new BigInteger("234234234234"),
          new BigInteger("-56345345345345"),
          new BigInteger("29823462983462893462934679234653456345"), // converts to 16 byte array
          new BigInteger("-3894572983475982374598324598234346536"), // converts to 16 byte array
          new BigInteger("-345345"),
          new BigInteger("754533")
      };

      int insertionIdx = 0;
      insertionIdx++; // insert a null
      for (BigInteger val : testBigInts) {
        decimalVector.setBigEndian(insertionIdx++, val.toByteArray());
      }
      insertionIdx++; // insert a null
      // insert a zero length buffer
      decimalVector.setBigEndian(insertionIdx++, new byte[0]);

      // Try inserting a buffer larger than 16bytes and expect a failure
      try {
        decimalVector.setBigEndian(insertionIdx, new byte[17]);
        fail("above statement should have failed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().equals("Invalid decimal value length. Valid length in [1 - 16], got 17"));
      }
      decimalVector.setValueCount(insertionIdx);

      // retrieve values and check if they are correct
      int outputIdx = 0;
      assertTrue(decimalVector.isNull(outputIdx++));
      for (BigInteger expected : testBigInts) {
        final BigDecimal actual = decimalVector.getObject(outputIdx++);
        assertEquals(expected, actual.unscaledValue());
      }
      assertTrue(decimalVector.isNull(outputIdx++));
      assertEquals(BigInteger.valueOf(0), decimalVector.getObject(outputIdx).unscaledValue());
    }
  }
}
