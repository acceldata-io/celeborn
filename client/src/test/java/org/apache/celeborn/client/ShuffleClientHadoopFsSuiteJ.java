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

package org.apache.celeborn.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.StorageInfo;

public class ShuffleClientHadoopFsSuiteJ {

  public static class CloseTrackingFileSystem extends RawLocalFileSystem {
    private volatile boolean closed = false;

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }

    boolean isClosed() {
      return closed;
    }
  }

  private CelebornConf conf;

  @Before
  public void setUp() {
    ShuffleClient.reset();
    conf = new CelebornConf();
    conf.set("celeborn.storage.availableTypes", "HDFS");
    conf.set("celeborn.storage.hdfs.dir", "hdfs://celeborn-test-nn:8020/celeborn");
    conf.set("celeborn.hadoop.fs.hdfs.impl", CloseTrackingFileSystem.class.getName());
  }

  @After
  public void tearDown() throws IOException {
    FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
    ShuffleClient.reset();
  }

  private CloseTrackingFileSystem hdfs() {
    return (CloseTrackingFileSystem) ShuffleClient.getHadoopFs(conf).get(StorageInfo.Type.HDFS);
  }

  @Test
  public void testReusesCachedFileSystemWhileOpen() {
    assertSame(hdfs(), hdfs());
  }

  @Test
  public void testRecoversAfterHostClosesCachedFileSystemsOfUgi() throws IOException {
    CloseTrackingFileSystem first = hdfs();
    assertFalse(first.isClosed());

    // Tez TezChild does this after every task when the container is reused.
    FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
    assertTrue(first.isClosed());

    CloseTrackingFileSystem second = hdfs();
    assertNotSame(first, second);
    assertFalse(second.isClosed());
  }
}
