/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.BaseReader.MapReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

public class TestVectorUnloadLoad {

  static final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  @Test
  public void test() throws IOException {
    int count = 10000;
    Schema schema;

    try (
        BufferAllocator originalVectorsAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorsAllocator, null)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter rootWriter = writer.rootAsMap();
      IntWriter intWriter = rootWriter.integer("int");
      BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
      for (int i = 0; i < count; i++) {
        intWriter.setPosition(i);
        intWriter.writeInt(i);
        bigIntWriter.setPosition(i);
        bigIntWriter.writeBigInt(i);
      }
      writer.setValueCount(count);

      VectorUnloader vectorUnloader = new VectorUnloader(parent.getChild("root"));
      schema = vectorUnloader.getSchema();

      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
          MapVector newParent = new MapVector("parent", finalVectorsAllocator, null)) {
        FieldVector root = newParent.addOrGet("root", MinorType.MAP, NullableMapVector.class);
        VectorLoader vectorLoader = new VectorLoader(schema, root);

        vectorLoader.load(recordBatch);

        MapReader rootReader = new SingleMapReaderImpl(newParent).reader("root");
        for (int i = 0; i < count; i++) {
          rootReader.setPosition(i);
          Assert.assertEquals(i, rootReader.reader("int").readInteger().intValue());
          Assert.assertEquals(i, rootReader.reader("bigInt").readLong().longValue());
        }
      }
    }
  }

  @Test
  public void testWithoutFieldNodes() throws Exception {
    BufferAllocator original = allocator.newChildAllocator("original", 0, Integer.MAX_VALUE);
    NullableIntVector intVector = new NullableIntVector("int", original);
    NullableVarCharVector varCharVector = new NullableVarCharVector("int", original);
    ListVector listVector = new ListVector("list", original, null);
    ListVector innerListVector = (ListVector) listVector.addOrGetVector(MinorType.LIST).getVector();

    intVector.allocateNew();;
    varCharVector.allocateNew();
    listVector.allocateNew();

    UnionListWriter listWriter = listVector.getWriter();

    for (int i = 0; i < 100; i++) {
      if (i % 3 == 0) {
        continue;
      }
      intVector.getMutator().setSafe(i, i);
      byte[] s = ("val" + i).getBytes();
      varCharVector.getMutator().setSafe(i, s, 0, s.length);
      listWriter.setPosition(i);
      listWriter.startList();
      ListWriter innerListWriter = listWriter.list();
      innerListWriter.startList();
      innerListWriter.integer().writeInt(i);
      innerListWriter.integer().writeInt(i + 1);
      innerListWriter.endList();
      innerListWriter.startList();
      innerListWriter.integer().writeInt(i + 2);
      innerListWriter.integer().writeInt(i + 3);
      innerListWriter.endList();
      listWriter.endList();
    }

    intVector.getMutator().setValueCount(100);
    varCharVector.getMutator().setValueCount(100);
    listVector.getMutator().setValueCount(100);

    ByteBuf[] bufs = FluentIterable.from(
            ImmutableList.<FieldVector>of(intVector, varCharVector, listVector))
            .transformAndConcat(new Function<FieldVector, Iterable<ArrowBuf>>() {
              @Override
              public Iterable<ArrowBuf> apply(FieldVector vector) {
                return Arrays.asList(vector.getBuffers(true));
              }
            }).toList().toArray(new ByteBuf[0]);

    RecordBatch recordBatch = RecordBatch.getRootAsRecordBatch(getArrowRecordBatch(bufs, 100));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();



    for (ByteBuf buf : bufs) {
      buf.readBytes(outputStream, buf.readableBytes());
      buf.release();
    }
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();

    BufferAllocator newAllocator = allocator.newChildAllocator("new allocator", 0, Integer.MAX_VALUE);

    ArrowBuf body = newAllocator.buffer(bytes.length);

    body.writeBytes(bytes);


    NullableIntVector newIntVector = new NullableIntVector("newInt", newAllocator);
    NullableVarCharVector newVarCharVector = new NullableVarCharVector("newVarChar", newAllocator);
    ListVector newListVector = new ListVector("newListVector", newAllocator, null);
    ((ListVector) newListVector.addOrGetVector(MinorType.LIST).getVector()).addOrGetVector(MinorType.INT);

    BuffersIterator buffersIterator = new BuffersIterator(recordBatch);

    newIntVector.loadFieldBuffers(buffersIterator, body);
    newIntVector.getMutator().setValueCount(100);
    newVarCharVector.loadFieldBuffers(buffersIterator, body);
    newVarCharVector.getMutator().setValueCount(100);
    newListVector.loadFieldBuffers(buffersIterator, body);
    newListVector.getMutator().setValueCount(100);

    body.release();

    for (int i = 0; i < recordBatch.length(); i++) {
      if (i %3 == 0) {
        Assert.assertNull(newIntVector.getAccessor().getObject(i));
        Assert.assertNull(newVarCharVector.getAccessor().getObject(i));
        Assert.assertNull(newListVector.getAccessor().getObject(i));
      } else {
        Assert.assertEquals(Integer.valueOf(i), newIntVector.getAccessor().getObject(i));
        Assert.assertEquals("val" + i, newVarCharVector.getAccessor().getObject(i).toString());
        Assert.assertEquals(ImmutableList.of(ImmutableList.of(i, i + 1), ImmutableList.of(i + 2, i + 3)), newListVector.getAccessor().getObject(i));
      }
    }

    newIntVector.clear();
    newVarCharVector.clear();
    newListVector.clear();
    original.close();
    newAllocator.close();
  }

  private ByteBuffer getArrowRecordBatch(ByteBuf[] buffers, int recordCount) {
    int length = 0;
    for (ByteBuf buf : buffers) {
      length += buf.writerIndex();
    }
    FlatBufferBuilder builder = new FlatBufferBuilder();
    RecordBatch.startNodesVector(builder, 0);
    int nodesOffset = builder.endVector();
    RecordBatch.startBuffersVector(builder, buffers.length);
    int offset = length;
    for (int i = buffers.length - 1; i >= 0; i--) {
      long currentLength = buffers[i].writerIndex();
      offset -= currentLength;
      Buffer.createBuffer(builder, 0, offset, currentLength);
    }
    int buffersOffset = builder.endVector();
    RecordBatch.startRecordBatch(builder);
    RecordBatch.addLength(builder, recordCount);
    RecordBatch.addNodes(builder, nodesOffset);
    RecordBatch.addBuffers(builder, buffersOffset);
    int recordBatch = RecordBatch.endRecordBatch(builder);
    builder.finish(recordBatch);
    return builder.dataBuffer();
  }

  @AfterClass
  public static void afterClass() {
    allocator.close();
  }
}
