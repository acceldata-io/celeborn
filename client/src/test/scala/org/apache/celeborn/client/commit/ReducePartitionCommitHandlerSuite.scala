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

package org.apache.celeborn.client.commit

import java.util
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder

import scala.collection.JavaConverters._

import org.roaringbitmap.RoaringBitmap
import org.scalatest.matchers.should.Matchers

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.{ShuffleAllocatedWorkers, ShuffleFileGroups}
import org.apache.celeborn.client.ShuffleCommittedInfo
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.{PartitionLocation, StorageInfo}
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils}

class ReducePartitionCommitHandlerSuite extends CelebornFunSuite with Matchers {

  private val shuffleId = 0
  private val numMappers = 2
  private val numPartitions = 2

  private class TestReducePartitionCommitHandler(
      conf: CelebornConf,
      committedPartitionInfo: CommittedPartitionInfo,
      pool: ThreadPoolExecutor)
    extends ReducePartitionCommitHandler(
      "app",
      conf,
      new ShuffleAllocatedWorkers,
      committedPartitionInfo,
      null,
      pool,
      null) {
    def fileGroups: ShuffleFileGroups = reducerFileGroupsMap
  }

  private var pool: ThreadPoolExecutor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    pool = ThreadUtils.newDaemonCachedThreadPool("reduce-partition-commit-handler-suite", 1)
  }

  override def afterAll(): Unit = {
    pool.shutdownNow()
    super.afterAll()
  }

  private def newCommittedInfo(): ShuffleCommittedInfo = {
    ShuffleCommittedInfo(
      JavaUtils.newConcurrentHashMap[Int, util.List[String]](),
      JavaUtils.newConcurrentHashMap[Int, util.List[String]](),
      JavaUtils.newConcurrentHashMap[String, WorkerInfo](),
      JavaUtils.newConcurrentHashMap[String, WorkerInfo](),
      JavaUtils.newConcurrentHashMap[String, StorageInfo](),
      JavaUtils.newConcurrentHashMap[String, StorageInfo](),
      JavaUtils.newConcurrentHashMap[String, RoaringBitmap](),
      new LongAdder,
      new util.HashSet[PartitionLocation](),
      new util.HashSet[PartitionLocation](),
      new AtomicInteger(),
      JavaUtils.newConcurrentHashMap[Int, AtomicInteger]())
  }

  private def newHandler(conf: CelebornConf = new CelebornConf())
      : (TestReducePartitionCommitHandler, ShuffleCommittedInfo) = {
    val committedPartitionInfo = new CommittedPartitionInfo
    val committedInfo = newCommittedInfo()
    committedPartitionInfo.put(shuffleId, committedInfo)
    val handler = new TestReducePartitionCommitHandler(conf, committedPartitionInfo, pool)
    handler.registerShuffle(shuffleId, numMappers, false, numPartitions)
    (handler, committedInfo)
  }

  private def mapperEnd(
      handler: ReducePartitionCommitHandler,
      mapId: Int,
      attemptId: Int): (Boolean, Boolean) = {
    handler.finishMapperAttempt(
      shuffleId,
      mapId,
      attemptId,
      numMappers,
      -1,
      null,
      _ => {},
      numPartitions,
      new Array[Int](numPartitions),
      new Array[Long](numPartitions))
  }

  private def collectOnePartition(
      handler: ReducePartitionCommitHandler,
      committedInfo: ShuffleCommittedInfo): Unit = {
    val location =
      new PartitionLocation(0, 0, "host", 1, 2, 3, 4, PartitionLocation.Mode.PRIMARY)
    val primaryPartMap = new ConcurrentHashMap[String, PartitionLocation]()
    primaryPartMap.put(location.getUniqueId, location)
    handler.collectResult(
      shuffleId,
      committedInfo,
      Iterator(location.getUniqueId).asJava,
      Iterator.empty.asJava,
      primaryPartMap,
      new ConcurrentHashMap[String, PartitionLocation]())
  }

  test("abort before stage end resets the latch and keeps the reducer file group") {
    val (handler, committedInfo) = newHandler()
    mapperEnd(handler, 0, 0) shouldBe ((true, false))
    mapperEnd(handler, 1, 0) shouldBe ((true, true))

    handler.abortMapperAttempt(shuffleId, 1, 0)

    handler.getMapperAttempts(shuffleId).toSeq shouldBe Seq(0, -1)
    handler.fileGroups.get(shuffleId) should not be null
    mapperEnd(handler, 1, 1) shouldBe ((true, true))
    handler.getMapperAttempts(shuffleId).toSeq shouldBe Seq(0, 1)

    collectOnePartition(handler, committedInfo)
    handler.fileGroups.get(shuffleId).get(0).size() shouldBe 1
  }

  test("abort after stage end does not re-open the stage") {
    val (handler, _) = newHandler()
    mapperEnd(handler, 0, 0)
    mapperEnd(handler, 1, 0)
    handler.setStageEnd(shuffleId)

    handler.abortMapperAttempt(shuffleId, 1, 0)

    handler.isStageEnd(shuffleId) shouldBe true
    handler.getMapperAttempts(shuffleId).toSeq shouldBe Seq(0, 0)
    handler.fileGroups.get(shuffleId) should not be null
  }

  test("abort of an attempt that is not the recorded one is a no-op") {
    val (handler, _) = newHandler()
    mapperEnd(handler, 0, 1)

    handler.abortMapperAttempt(shuffleId, 0, 0)
    handler.abortMapperAttempt(shuffleId, 1, 0)

    handler.getMapperAttempts(shuffleId).toSeq shouldBe Seq(1, -1)
  }

  test("newer attempt supersedes only when enabled and before stage end") {
    val (defaultHandler, _) = newHandler()
    mapperEnd(defaultHandler, 0, 0)
    mapperEnd(defaultHandler, 0, 1) shouldBe ((false, false))
    defaultHandler.getMapperAttempts(shuffleId).toSeq shouldBe Seq(0, -1)

    val conf = new CelebornConf().set(CelebornConf.SHUFFLE_ALLOW_MAP_ATTEMPT_SUPERSEDE, true)
    val (handler, _) = newHandler(conf)
    mapperEnd(handler, 0, 0)
    mapperEnd(handler, 1, 0)
    mapperEnd(handler, 0, 1) shouldBe ((true, true))
    handler.getMapperAttempts(shuffleId).toSeq shouldBe Seq(1, 0)

    handler.setStageEnd(shuffleId)
    mapperEnd(handler, 1, 1) shouldBe ((false, false))
    handler.getMapperAttempts(shuffleId).toSeq shouldBe Seq(1, 0)
  }
}
