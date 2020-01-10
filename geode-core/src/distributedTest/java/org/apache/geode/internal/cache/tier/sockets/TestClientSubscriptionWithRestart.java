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

package org.apache.geode.internal.cache.tier.sockets;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class TestClientSubscriptionWithRestart {

  public static final String REGION = "clientRegion";
  private static final boolean IS_DURABLE = false;

  private MemberVM locator;
  private List<MemberVM> servers = new ArrayList<>();
  private List<ClientVM> clients = new ArrayList<>();

  private final int locatorsToStart = 1;
  private static final int serversToStart = 4;
  private static final int clientsToStart = 12;
  private static final int REDUNDANCY = 1;
  private static final int balancedPrimariesPerServer = clientsToStart / serversToStart;
  private static final int balancedProxiesPerServer = balancedPrimariesPerServer * (REDUNDANCY + 1);
  private static final boolean balancePrimaries = true;

  @Rule
  public final ClusterStartupRule cluster =
      new ClusterStartupRule(serversToStart + clientsToStart + locatorsToStart);

  @Before
  public void setup() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withProperty("log-level", "WARN"));

    int port = locator.getPort();
    for (int i = 0; i < serversToStart; i++) {
      servers.add(cluster.startServerVM(i + locatorsToStart,
          s -> s.withConnectionToLocator(port).withProperty("log-level", "WARN")));
    }

    if (IS_DURABLE) {
      for (int i = 0; i < clientsToStart; ++i) {
        String clientID = String.valueOf(i);
        clients.add(cluster.startClientVM(serversToStart + locatorsToStart + i,
            clientCacheRule -> clientCacheRule.withLocatorConnection(port)
                .withProperty("log-level", "WARN")
                .withProperty("durable-client-id", clientID)
                .withProperty("durable-client-timeout", "5")
                .withPoolSubscription(true)
                .withCacheSetup(cf -> cf.setPoolSubscriptionRedundancy(REDUNDANCY))));
      }
    } else {
      for (int i = 0; i < clientsToStart; ++i) {
        clients.add(cluster.startClientVM(serversToStart + locatorsToStart + i,
            clientCacheRule -> clientCacheRule.withLocatorConnection(port)
                .withProperty("log-level", "WARN")
                .withPoolSubscription(true)
                .withCacheSetup(cf -> cf.setPoolSubscriptionRedundancy(REDUNDANCY))));
      }
    }
  }

  @Test
  public void test() throws InterruptedException {
    servers.forEach(s -> s.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION);
    }));

    clients.forEach(c -> c.invoke(() -> {
      Region region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION);
      int keyValue = c.getVM().getId();
      region.put(keyValue, keyValue);
      region.registerInterest(keyValue, IS_DURABLE);
    }));
//    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    // servers.forEach(s -> {
    // if (!s.getName().contains("1")) {
    // s.stop();
    // }
    // });

    // int port = locator.getPort();
    // for (int i = 1; i < serversToStart; i++) {
    // servers.remove(i);
    // servers.add(i, cluster.startServerVM(i + locatorsToStart, s ->
    // s.withConnectionToLocator(port).withProperty("log-level", "WARN")));
    // servers.get(i).invoke(() -> {
    // ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION);
    // });
    // }
    // Thread.sleep(10000);
    // servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));
    // if (IS_DURABLE) {
    // servers.get(0).invoke(() ->
    // CacheClientNotifier.getInstance().getClientProxies().forEach(proxy -> {
    // proxy.setKeepAlive(false);
    // System.out.println("DEBR should keep proxy: " + proxy.close(true, true));
    // }));
    // } else {
    // servers.get(0).invoke(() ->
    // CacheClientNotifier.getInstance().getClientProxies().forEach(proxy -> {
    // if(proxy.isPrimary()) {
    // proxy.close();
    // }
    // }));
    // }

//    servers.get(0).invoke(() -> CacheClientNotifier.getInstance().getClientProxies().forEach(CacheClientProxy::close));
     servers.forEach(s -> {
       if (!s.getName().contains("1")) {
         s.stop();
       }
     });

    int port = locator.getPort();
    for (int i = 1; i < serversToStart; i++) {
      servers.remove(i);
      servers.add(i, cluster.startServerVM(i + locatorsToStart, s ->
          s.withConnectionToLocator(port).withProperty("log-level", "WARN")));
      servers.get(i).invoke(() -> {
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION);
      });
    }

    Thread.sleep(5000);

    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    if (balancePrimaries) {
      servers.forEach(s -> s.invoke(() -> {
        checkForCacheClientProxy();
        Collection<CacheClientProxy> primaries =
            CacheClientNotifier.getInstance().getClientProxies().stream()
                .filter(CacheClientProxy::basicIsPrimary).collect(Collectors.toSet());
        while (primaries.size() > balancedPrimariesPerServer) {
          primaries.stream().findFirst().get().close();
          Thread.sleep(1000);
          checkForCacheClientProxy();
          primaries =
              CacheClientNotifier.getInstance().getClientProxies().stream()
                  .filter(CacheClientProxy::basicIsPrimary).collect(Collectors.toSet());
        }
      }));
    }
    servers.forEach(s -> s.invoke(() -> {
      checkForCacheClientProxy();
      Collection<CacheClientProxy> secondaries = CacheClientNotifier.getInstance().getClientProxies().stream().filter(proxy -> !proxy.basicIsPrimary()).collect(
          Collectors.toSet());
      while (CacheClientNotifier.getInstance().getClientProxies().size() > balancedProxiesPerServer) {
        secondaries.stream().findFirst().get().close();
        Thread.sleep(1000);
        checkForCacheClientProxy();
        secondaries = CacheClientNotifier.getInstance().getClientProxies().stream().filter(proxy -> !proxy.basicIsPrimary()).collect(
            Collectors.toSet());
      }
    }));
    // Thread.sleep(12000);
    // long totalWaitTimeMs = 1000;
    // long currentWaitTimeMs = 0;
    // long proxyCheckInterval = 250;
    // long startTime = System.currentTimeMillis();
    // while (currentWaitTimeMs <= totalWaitTimeMs) {
    // servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));
    // Thread.sleep(proxyCheckInterval);
    // currentWaitTimeMs = System.currentTimeMillis() - startTime;
    // }
    // servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    // servers.get(1).invoke(() ->
    // CacheClientNotifier.getInstance().getClientProxies().forEach(CacheClientProxy::close));
    // servers.get(1).invoke(() ->
    // CacheClientNotifier.getInstance().getClientProxies().stream().findAny().get().close());
    Thread.sleep(2000);
    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    // clients.get(0).invoke(() -> {
    //
    // QueueManagerImpl queueManager = (QueueManagerImpl) ((PoolImpl)
    // ClusterStartupRule.getClientCache().getDefaultPool()).getQueueManager();
    // queueManager.incrementRedundancy();
    // queueManager.recoverRedundancy(new HashSet<>(), true);
    // Thread.sleep(5000);
    // queueManager.decrementRedundancy();
    // });
    // clients.get(0).invoke(() -> {
    // //TODO: Figure out how to close the CacheClientUpdater
    // });
    // Thread.sleep(10000);

    // servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));
  }

  public static void checkForCacheClientProxy() {
    System.out.println();
    CacheClientNotifier notifier = CacheClientNotifier.getInstance();
    if (notifier != null) {
      Collection<CacheClientProxy> proxies = notifier.getClientProxies();
      if (proxies != null) {
        LogService.getLogger().warn("DEBR Number of ClientCacheProxies: " + proxies.size());
        // System.out.println("DEBR Number of ClientCacheProxies: " + proxies.size());
        for (CacheClientProxy proxy : proxies) {
          LogService.getLogger().warn(
              "Proxy " + proxy.proxyID + (proxy.isPrimary() ? " is primary." : " is secondary.")
                  + " Is active: " + proxy.hasRegisteredInterested());
          // System.out.println("Proxy " + proxy.proxyID + (proxy.isPrimary() ? " is primary." : "
          // is secondary.") + " Is active: " + proxy.hasRegisteredInterested());
        }
      } else {
        System.out.println("DEBR Did not find a proxy");
      }
    } else {
      System.out.println("DEBR Did not find a notifier");
    }
  }

}
