/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.MergedBlockMetaRequest;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.FetchShuffleBlockChunks;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.MergeStatuses;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.shuffle.protocol.UploadBlock;

public class ExternalBlockHandlerSuite {
  TransportClient client = mock(TransportClient.class);

  OneForOneStreamManager streamManager;
  ExternalShuffleBlockResolver blockResolver;
  RpcHandler handler;
  MergedShuffleFileManager mergedShuffleManager;
  ManagedBuffer[] blockMarkers = {
    new NioManagedBuffer(ByteBuffer.wrap(new byte[3])),
    new NioManagedBuffer(ByteBuffer.wrap(new byte[7]))
  };

  @Before
  public void beforeEach() {
    streamManager = mock(OneForOneStreamManager.class);
    blockResolver = mock(ExternalShuffleBlockResolver.class);
    mergedShuffleManager = mock(MergedShuffleFileManager.class);
    handler = new ExternalBlockHandler(streamManager, blockResolver, mergedShuffleManager);
  }

  @Test
  public void testRegisterExecutor() {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    String[] localDirs = new String[] {"/a", "/b"};
    ExecutorShuffleInfo config = new ExecutorShuffleInfo(localDirs, 16, "sort");
    ByteBuffer registerMessage = new RegisterExecutor("app0", "exec1", config).toByteBuffer();
    handler.receive(client, registerMessage, callback);
    verify(blockResolver, times(1)).registerExecutor("app0", "exec1", config);
    verify(mergedShuffleManager, times(1)).registerExecutor("app0", config);

    verify(callback, times(1)).onSuccess(any(ByteBuffer.class));
    verify(callback, never()).onFailure(any(Throwable.class));
    // Verify register executor request latency metrics
    Timer registerExecutorRequestLatencyMillis = (Timer) ((ExternalBlockHandler) handler)
        .getAllMetrics()
        .getMetrics()
        .get("registerExecutorRequestLatencyMillis");
    assertEquals(1, registerExecutorRequestLatencyMillis.getCount());
  }

  @Test
  public void testCompatibilityWithOldVersion() {
    when(blockResolver.getBlockData("app0", "exec1", 0, 0, 0)).thenReturn(blockMarkers[0]);
    when(blockResolver.getBlockData("app0", "exec1", 0, 0, 1)).thenReturn(blockMarkers[1]);

    OpenBlocks openBlocks = new OpenBlocks(
      "app0", "exec1", new String[] { "shuffle_0_0_0", "shuffle_0_0_1" });
    checkOpenBlocksReceive(openBlocks, blockMarkers);

    verify(blockResolver, times(1)).getBlockData("app0", "exec1", 0, 0, 0);
    verify(blockResolver, times(1)).getBlockData("app0", "exec1", 0, 0, 1);
    verifyOpenBlockLatencyMetrics();
  }

  @Test
  public void testFetchShuffleBlocks() {
    when(blockResolver.getBlockData("app0", "exec1", 0, 0, 0)).thenReturn(blockMarkers[0]);
    when(blockResolver.getBlockData("app0", "exec1", 0, 0, 1)).thenReturn(blockMarkers[1]);

    FetchShuffleBlocks fetchShuffleBlocks = new FetchShuffleBlocks(
      "app0", "exec1", 0, new long[] { 0 }, new int[][] {{ 0, 1 }}, false);
    checkOpenBlocksReceive(fetchShuffleBlocks, blockMarkers);

    verify(blockResolver, times(1)).getBlockData("app0", "exec1", 0, 0, 0);
    verify(blockResolver, times(1)).getBlockData("app0", "exec1", 0, 0, 1);
    verifyOpenBlockLatencyMetrics();
  }

  @Test
  public void testFetchShuffleBlocksInBatch() {
    ManagedBuffer[] batchBlockMarkers = {
      new NioManagedBuffer(ByteBuffer.wrap(new byte[10]))
    };
    when(blockResolver.getContinuousBlocksData(
      "app0", "exec1", 0, 0, 0, 1)).thenReturn(batchBlockMarkers[0]);

    FetchShuffleBlocks fetchShuffleBlocks = new FetchShuffleBlocks(
      "app0", "exec1", 0, new long[] { 0 }, new int[][] {{ 0, 1 }}, true);
    checkOpenBlocksReceive(fetchShuffleBlocks, batchBlockMarkers);

    verify(blockResolver, times(1)).getContinuousBlocksData("app0", "exec1", 0, 0, 0, 1);
    verifyOpenBlockLatencyMetrics();
  }

  @Test
  public void testOpenDiskPersistedRDDBlocks() {
    when(blockResolver.getRddBlockData("app0", "exec1", 0, 0)).thenReturn(blockMarkers[0]);
    when(blockResolver.getRddBlockData("app0", "exec1", 0, 1)).thenReturn(blockMarkers[1]);

    OpenBlocks openBlocks = new OpenBlocks(
      "app0", "exec1", new String[] { "rdd_0_0", "rdd_0_1" });
    checkOpenBlocksReceive(openBlocks, blockMarkers);

    verify(blockResolver, times(1)).getRddBlockData("app0", "exec1", 0, 0);
    verify(blockResolver, times(1)).getRddBlockData("app0", "exec1", 0, 1);
    verifyOpenBlockLatencyMetrics();
  }

  @Test
  public void testOpenDiskPersistedRDDBlocksWithMissingBlock() {
    ManagedBuffer[] blockMarkersWithMissingBlock = {
      new NioManagedBuffer(ByteBuffer.wrap(new byte[3])),
      null
    };
    when(blockResolver.getRddBlockData("app0", "exec1", 0, 0))
      .thenReturn(blockMarkersWithMissingBlock[0]);
    when(blockResolver.getRddBlockData("app0", "exec1", 0, 1))
      .thenReturn(null);

    OpenBlocks openBlocks = new OpenBlocks(
      "app0", "exec1", new String[] { "rdd_0_0", "rdd_0_1" });
    checkOpenBlocksReceive(openBlocks, blockMarkersWithMissingBlock);

    verify(blockResolver, times(1)).getRddBlockData("app0", "exec1", 0, 0);
    verify(blockResolver, times(1)).getRddBlockData("app0", "exec1", 0, 1);
  }

  private void checkOpenBlocksReceive(BlockTransferMessage msg, ManagedBuffer[] blockMarkers) {
    when(client.getClientId()).thenReturn("app0");

    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.receive(client, msg.toByteBuffer(), callback);

    ArgumentCaptor<ByteBuffer> response = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(callback, times(1)).onSuccess(response.capture());
    verify(callback, never()).onFailure(any());

    StreamHandle handle =
      (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response.getValue());
    assertEquals(blockMarkers.length, handle.numChunks);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterator<ManagedBuffer>> stream = (ArgumentCaptor<Iterator<ManagedBuffer>>)
        (ArgumentCaptor<?>) ArgumentCaptor.forClass(Iterator.class);
    verify(streamManager, times(1)).registerStream(anyString(), stream.capture(),
      any());
    Iterator<ManagedBuffer> buffers = stream.getValue();
    for (ManagedBuffer blockMarker : blockMarkers) {
      assertEquals(blockMarker, buffers.next());
    }
    assertFalse(buffers.hasNext());
  }

  private void verifyOpenBlockLatencyMetrics() {
    Timer openBlockRequestLatencyMillis = (Timer) ((ExternalBlockHandler) handler)
        .getAllMetrics()
        .getMetrics()
        .get("openBlockRequestLatencyMillis");
    assertEquals(1, openBlockRequestLatencyMillis.getCount());
    // Verify block transfer metrics
    Meter blockTransferRateBytes = (Meter) ((ExternalBlockHandler) handler)
        .getAllMetrics()
        .getMetrics()
        .get("blockTransferRateBytes");
    assertEquals(10, blockTransferRateBytes.getCount());
  }

  @Test
  public void testBadMessages() {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    ByteBuffer unserializableMsg = ByteBuffer.wrap(new byte[] { 0x12, 0x34, 0x56 });
    try {
      handler.receive(client, unserializableMsg, callback);
      fail("Should have thrown");
    } catch (Exception e) {
      // pass
    }

    ByteBuffer unexpectedMsg = new UploadBlock("a", "e", "b", new byte[1],
      new byte[2]).toByteBuffer();
    try {
      handler.receive(client, unexpectedMsg, callback);
      fail("Should have thrown");
    } catch (UnsupportedOperationException e) {
      // pass
    }

    verify(callback, never()).onSuccess(any(ByteBuffer.class));
    verify(callback, never()).onFailure(any(Throwable.class));
  }

  @Test
  public void testFinalizeShuffleMerge() throws IOException {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);

    FinalizeShuffleMerge req = new FinalizeShuffleMerge("app0", 0);
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(0, 1, 2);
    MergeStatuses statuses = new MergeStatuses(0, new RoaringBitmap[]{bitmap},
      new int[]{3}, new long[]{30});
    when(mergedShuffleManager.finalizeShuffleMerge(req)).thenReturn(statuses);

    ByteBuffer reqBuf = req.toByteBuffer();
    handler.receive(client, reqBuf, callback);
    verify(mergedShuffleManager, times(1)).finalizeShuffleMerge(req);
    ArgumentCaptor<ByteBuffer> response = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(callback, times(1)).onSuccess(response.capture());
    verify(callback, never()).onFailure(any());

    MergeStatuses mergeStatuses =
      (MergeStatuses) BlockTransferMessage.Decoder.fromByteBuffer(response.getValue());
    assertEquals(mergeStatuses, statuses);

    Timer finalizeShuffleMergeLatencyMillis = (Timer) ((ExternalBlockHandler) handler)
        .getAllMetrics()
        .getMetrics()
        .get("finalizeShuffleMergeLatencyMillis");
    assertEquals(1, finalizeShuffleMergeLatencyMillis.getCount());
  }

  @Test
  public void testFetchMergedBlocksMeta() {
    when(mergedShuffleManager.getMergedBlockMeta("app0", 0, 0)).thenReturn(
      new MergedBlockMeta(1, mock(ManagedBuffer.class)));
    when(mergedShuffleManager.getMergedBlockMeta("app0", 0, 1)).thenReturn(
      new MergedBlockMeta(3, mock(ManagedBuffer.class)));
    when(mergedShuffleManager.getMergedBlockMeta("app0", 0, 2)).thenReturn(
      new MergedBlockMeta(5, mock(ManagedBuffer.class)));

    int[] expectedCount = new int[]{1, 3, 5};
    String appId = "app0";
    long requestId = 0L;
    for (int reduceId = 0; reduceId < 3; reduceId++) {
      MergedBlockMetaRequest req = new MergedBlockMetaRequest(requestId++, appId, 0, reduceId);
      MergedBlockMetaResponseCallback callback = mock(MergedBlockMetaResponseCallback.class);
      handler.getMergedBlockMetaReqHandler()
        .receiveMergeBlockMetaReq(client, req, callback);
      verify(mergedShuffleManager, times(1)).getMergedBlockMeta("app0", 0, reduceId);

      ArgumentCaptor<Integer> numChunksResponse = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<ManagedBuffer> chunkBitmapResponse =
        ArgumentCaptor.forClass(ManagedBuffer.class);
      verify(callback, times(1)).onSuccess(numChunksResponse.capture(),
        chunkBitmapResponse.capture());
      assertEquals("num chunks in merged block " + reduceId, expectedCount[reduceId],
        numChunksResponse.getValue().intValue());
      assertNotNull("chunks bitmap buffer " + reduceId, chunkBitmapResponse.getValue());
    }
  }

  @Test
  public void testOpenBlocksWithShuffleChunks() {
    verifyBlockChunkFetches(true);
  }

  @Test
  public void testFetchShuffleChunks() {
    verifyBlockChunkFetches(false);
  }

  private void verifyBlockChunkFetches(boolean useOpenBlocks) {
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    ByteBuffer buffer;
    if (useOpenBlocks) {
      OpenBlocks openBlocks =
        new OpenBlocks("app0", "exec1",
          new String[] {"shuffleChunk_0_0_0", "shuffleChunk_0_0_1", "shuffleChunk_0_1_0",
            "shuffleChunk_0_1_1"});
      buffer = openBlocks.toByteBuffer();
    } else {
      FetchShuffleBlockChunks fetchChunks = new FetchShuffleBlockChunks(
        "app0", "exec1", 0, new int[] {0, 1}, new int[][] {{0, 1}, {0, 1}});
      buffer = fetchChunks.toByteBuffer();
    }
    ManagedBuffer[][] buffers = new ManagedBuffer[][] {
      {
        new NioManagedBuffer(ByteBuffer.wrap(new byte[5])),
        new NioManagedBuffer(ByteBuffer.wrap(new byte[7]))
      },
      {
        new NioManagedBuffer(ByteBuffer.wrap(new byte[5])),
        new NioManagedBuffer(ByteBuffer.wrap(new byte[7]))
      }
    };
    for (int reduceId = 0; reduceId < 2; reduceId++) {
      for (int chunkId = 0; chunkId < 2; chunkId++) {
        when(mergedShuffleManager.getMergedBlockData(
          "app0", 0, reduceId, chunkId)).thenReturn(buffers[reduceId][chunkId]);
      }
    }
    handler.receive(client, buffer, callback);
    ArgumentCaptor<ByteBuffer> response = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(callback, times(1)).onSuccess(response.capture());
    verify(callback, never()).onFailure(any());
    StreamHandle handle =
      (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response.getValue());
    assertEquals(4, handle.numChunks);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterator<ManagedBuffer>> stream = (ArgumentCaptor<Iterator<ManagedBuffer>>)
      (ArgumentCaptor<?>) ArgumentCaptor.forClass(Iterator.class);
    verify(streamManager, times(1)).registerStream(any(), stream.capture(), any());
    Iterator<ManagedBuffer> bufferIter = stream.getValue();
    for (int reduceId = 0; reduceId < 2; reduceId++) {
      for (int chunkId = 0; chunkId < 2; chunkId++) {
        assertEquals(buffers[reduceId][chunkId], bufferIter.next());
      }
    }
    assertFalse(bufferIter.hasNext());
    verify(mergedShuffleManager, never()).getMergedBlockMeta(anyString(), anyInt(), anyInt());
    verify(blockResolver, never()).getBlockData(
      anyString(), anyString(), anyInt(), anyInt(), anyInt());
    verify(mergedShuffleManager, times(1)).getMergedBlockData("app0", 0, 0, 0);
    verify(mergedShuffleManager, times(1)).getMergedBlockData("app0", 0, 0, 1);

    // Verify open block request latency metrics
    Timer openBlockRequestLatencyMillis = (Timer) ((ExternalBlockHandler) handler)
      .getAllMetrics()
      .getMetrics()
      .get("openBlockRequestLatencyMillis");
    assertEquals(1, openBlockRequestLatencyMillis.getCount());
    // Verify block transfer metrics
    Meter blockTransferRateBytes = (Meter) ((ExternalBlockHandler) handler)
      .getAllMetrics()
      .getMetrics()
      .get("blockTransferRateBytes");
    assertEquals(24, blockTransferRateBytes.getCount());
  }
}
