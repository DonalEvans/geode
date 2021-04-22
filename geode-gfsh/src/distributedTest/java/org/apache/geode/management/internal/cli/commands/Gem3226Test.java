/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.commands;

import static java.time.temporal.ChronoUnit.MILLIS;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.partitioned.PutMessage;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class Gem3226Test implements Serializable {
  private static final long serialVersionUID = 8986763319491861828L;
  private static final Logger logger = LogService.getLogger();
  private static final String regionName = "regionName";
  private static final String logLevel = "warn";
  MemberVM locator;
  private final List<MemberVM> feeds = new ArrayList<>();
  private final List<MemberVM> dataHosts = new ArrayList<>();
  private static final int NUM_FEEDS = 2;
  public static final int FEED_THREADS = 3;
  private static final int NUM_DATAHOSTS = 2;
  private static final int totalNumBuckets = 20;
  private static final int keysPerFeed = 50;
  public static volatile boolean warmup = true;
  public static final AtomicBoolean shouldClose = new AtomicBoolean(false);
  private static boolean killTestBoolean = false;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1 + NUM_FEEDS + NUM_DATAHOSTS);
  private int locatorPort;

  @Before
  public void setUp() throws InterruptedException {
    locator = cluster.startLocatorVM(0, x -> x.withProperty("log-level", logLevel));
    locatorPort = locator.getPort();

    for (int i = 0; i < NUM_FEEDS; ++i) {
      int feedNumber = i;
      feeds.add(cluster.startServerVM(i + 1, x -> x
          .withConnectionToLocator(locatorPort)
          .withName("feed-" + feedNumber)
          .withProperty("log-level", logLevel)
          .withSystemProperty("gemfire.DISABLE_CREATE_BUCKET_RANDOMNESS", "true")));
    }

    for (int i = 0; i < NUM_DATAHOSTS; ++i) {
      int dataHostNumber = i;
      dataHosts.add(cluster.startServerVM(i + 1 + NUM_FEEDS, x -> x
          .withConnectionToLocator(locatorPort)
          .withName("dataHost-" + dataHostNumber)
          .withProperty("log-level", logLevel)
          .withSystemProperty("gemfire.DISABLE_CREATE_BUCKET_RANDOMNESS", "true")));
    }

    List<AsyncInvocation<?>> createDataHostRegionInvocations = new ArrayList<>();

    for (MemberVM datahost : dataHosts) {
      createDataHostRegionInvocations.add(datahost.invokeAsync(
          "DONAL: create dataHost Region for " + datahost.getName(), () -> createRegion(false)));
    }

    for (AsyncInvocation<?> invocation : createDataHostRegionInvocations) {
      invocation.await();
    }

    List<AsyncInvocation<?>> createFeedRegionInvocations = new ArrayList<>();

    for (MemberVM feed : feeds) {
      createFeedRegionInvocations.add(feed.invokeAsync(
          "DONAL: create feed Region for " + feed.getName(), () -> createRegion(true)));
    }

    for (AsyncInvocation<?> invocation : createFeedRegionInvocations) {
      invocation.await();
    }

    List<AsyncInvocation<?>> assignBucketsInvocations = new ArrayList<>();

    for (MemberVM datahost : dataHosts) {
      assignBucketsInvocations.add(datahost.invokeAsync(
          "DONAL: assigning buckets for " + datahost.getName(), Gem3226Test::assignBuckets));
    }

    for (AsyncInvocation<?> invocation : assignBucketsInvocations) {
      invocation.await();
    }

    List<AsyncInvocation<?>> createDataInvocations = new ArrayList<>();

    for (MemberVM member : feeds) {
      for (int i = 0; i < FEED_THREADS; ++i) {
        int threadNumber = i;
        createDataInvocations.add(member.invokeAsync(
            "DONAL: create region data for " + member.getName() + ", thread-" + threadNumber,
            () -> createData(member.getVM().getId(), threadNumber)));
      }
    }

    for (AsyncInvocation<?> invocation : createDataInvocations) {
      invocation.await();
    }

    logger.warn("DONAL: Created all the data");
  }

  @Test
  public void test() throws InterruptedException {
    for (MemberVM feed : feeds) {
      for (int i = 0; i < FEED_THREADS; ++i) {
        feed.invokeAsync("DONAL: starting puts on " + feed.getName() + ", thread-" + i,
            Gem3226Test::doPuts);
      }
    }

    logger.warn("DONAL: sleeping 1 second while puts warm up");
    Thread.sleep(1000);
    feeds.forEach(feed -> feed.invokeAsync(() -> warmup = false));
    logger.warn("DONAL: done sleeping 1 second while puts warm up");

    dataHosts.get(0).invoke("DONAL: setting DistributionMessageObserver", () -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof PutMessage) {
            // Only allow one put thread to initiate the cache close
            if (shouldClose.get()) {
              synchronized (shouldClose) {
                if (shouldClose.get()) {
                  Cache cache = dm.getCache();
                  if (cache != null && !cache.isClosed()) {
                    LogService.getLogger()
                        .warn("DONAL: closing the cache in DMO after receiving {}", message);
                    shouldClose.set(false);
                    cache.close();
                  }
                }
              }
            }
          }
        }
      });
    });

    dataHosts.get(0).invokeAsync("DONAL: making DMO close the cache", () -> shouldClose.set(true));

    logger.warn("DONAL: sleeping 7 seconds while waiting to see if we get stuck");
    Thread.sleep(7000);
    logger.warn("DONAL: done sleeping 7 seconds while waiting to see if we get stuck");

    feeds.forEach(memberVM -> memberVM.invoke(() -> killTestBoolean = true));
    dataHosts.forEach(memberVM -> memberVM.invoke(() -> killTestBoolean = true));
  }

  public static void createRegion(boolean isFeed) {
    PartitionAttributesFactory<Object, Object> attributesFactory =
        new PartitionAttributesFactory<>();
    if (isFeed) {
      attributesFactory.setLocalMaxMemory(0);
    }
    PartitionAttributes<Object, Object> attr = attributesFactory
        .setTotalNumBuckets(totalNumBuckets)
        .setRedundantCopies(1)
        .setRecoveryDelay(-1)
        .setStartupRecoveryDelay(0)
        .create();
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    cache.createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(attr)
        .setConcurrencyLevel(16)
        .create(regionName);
  }

  public static void assignBuckets() {
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    Region<Object, Object> region = cache.getRegion(regionName);
    PartitionRegionHelper.assignBucketsToPartitions(region);
    RebalanceFactory rf = cache.getResourceManager().createRebalanceFactory();
    RebalanceOperation ro = rf.start();
    try {
      ro.getResults();
    } catch (InterruptedException ignore) {

    }
  }

  public static void createData(int memberNumber, int threadNumber) {
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    Region<Object, Object> region = cache.getRegion(regionName);
    for (int i = 0; i < keysPerFeed / FEED_THREADS; ++i) {
      // Key identifiers start at non-zero value because feed member IDs start at 1
      int identifier = (memberNumber * keysPerFeed) + (i * FEED_THREADS) + threadNumber;
      region.create("key-" + identifier, "value-" + identifier);
    }
  }

  public static void doPuts() throws InterruptedException {
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    Region<Object, Object> region = cache.getRegion(regionName);
    Random rnd = new Random(System.nanoTime());
    final AtomicInteger putsDone = new AtomicInteger(0);
    Thread reportPutsThread = new Thread(() -> {
      Instant aSecondFromNow = Instant.now().plus(1000, MILLIS);
      while (!killTestBoolean) {
        // Monitor puts/sec and log if they drop below 500
        if (!warmup && aSecondFromNow.isBefore(Instant.now())) {
          if (putsDone.get() < 100) {
            logger.warn("DONAL: Did " + putsDone.get() + " puts in the last 1000 milliseconds");
          }
          aSecondFromNow = Instant.now().plus(1000, MILLIS);
          putsDone.getAndSet(0);
        }
      }
    });
    reportPutsThread.setDaemon(true);
    reportPutsThread.start();
    while (!killTestBoolean) {
      // Key identifiers start at non-zero value because feed member IDs start at 1
      int keyIdentifier = rnd.nextInt(keysPerFeed * NUM_FEEDS) + keysPerFeed;
      // Make value identifier random so an update actually happens most of the time
      int valueIdentifier = rnd.nextInt(keysPerFeed * NUM_FEEDS) + keysPerFeed;
      region.put("key-" + keyIdentifier, "value-" + valueIdentifier);
      putsDone.incrementAndGet();
    }
    reportPutsThread.join();
  }
}
