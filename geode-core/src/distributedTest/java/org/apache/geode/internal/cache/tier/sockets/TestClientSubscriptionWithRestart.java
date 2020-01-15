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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.QueueManagerImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
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
                .withProperty("durable-client-timeout", "15")
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
  public void test() {
    servers.forEach(s -> s.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION);
    }));

    /**
     * On each client, create a proxy region, do a put and register interest in the key, then print out proxies on every server
     */
    clients.forEach(c -> c.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION);
      int keyValue = c.getVM().getId();
      region.put(keyValue, keyValue);
      region.registerInterest(keyValue, IS_DURABLE);
    }));
    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    /**
     * Close durable proxies on server 1
     */
//     if (IS_DURABLE) {
//     servers.get(0).invoke(() ->
//     CacheClientNotifier.getInstance().getClientProxies().forEach(proxy -> {
//     proxy.setKeepAlive(false);
//     System.out.println("DEBR should keep proxy: " + proxy.close(true, true));
//     }));
//     }
    /**
     * Close primaries on server 1
     */
//     servers.get(0).invoke(() ->
//     CacheClientNotifier.getInstance().getClientProxies().forEach(proxy -> {
//     if(proxy.isPrimary()) {
//     proxy.close();
//     }
//     }));

//    servers.get(0).invoke(() -> CacheClientNotifier.getInstance().getClientProxies().forEach(CacheClientProxy::close));
      /**
      *Find all queues in all pools on all clients and print the servers they're associated with.
       */
//    clients.forEach(c -> c.invoke(() -> {
//      Collection<Pool> pools = PoolManagerImpl.getPMI().getMap().values();
//      pools.forEach(pool -> {
//        QueueManagerImpl queueManager = ((PoolImpl)pool).getQueueManager();
//        QueueManagerImpl.ConnectionList connections =
//            (QueueManagerImpl.ConnectionList) queueManager.getAllConnections();
//        LogService.getLogger()
//            .warn("DEBR Primary Server = " + connections.getPrimary().getServer().toString());
//        connections.getBackups().forEach(connection -> LogService.getLogger()
//            .warn(("DEBR Secondary server = " + connection.getServer().toString())));
//      });
//    }));
    /**
     * Restart all servers except server1, wait 11 seconds, then print out proxies on all servers
     */
//     servers.forEach(s -> {
//       if (!s.getName().contains("1")) {
//         s.stop();
//       }
//     });
//
//    int port = locator.getPort();
//    for (int i = 1; i < serversToStart; i++) {
//      servers.remove(i);
//      servers.add(i, cluster.startServerVM(i + locatorsToStart, s ->
//          s.withConnectionToLocator(port).withProperty("log-level", "WARN")));
//      servers.get(i).invoke(() -> {
//        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION);
//      });
//    }
//    Thread.sleep(11000);
//    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    /**
     * Attempt to balance primary queues by closing proxies on servers
     */
//    if (balancePrimaries) {
//      servers.forEach(s -> s.invoke(() -> {
//        checkForCacheClientProxy();
//        Collection<CacheClientProxy> primaries =
//            CacheClientNotifier.getInstance().getClientProxies().stream()
//                .filter(CacheClientProxy::basicIsPrimary).collect(Collectors.toSet());
//        while (primaries.size() > balancedPrimariesPerServer) {
//          primaries.stream().findFirst().get().close();
//          Thread.sleep(1000);
//          checkForCacheClientProxy();
//          primaries =
//              CacheClientNotifier.getInstance().getClientProxies().stream()
//                  .filter(CacheClientProxy::basicIsPrimary).collect(Collectors.toSet());
//        }
//      }));
//    }

    /**
     * Attempt to balance secondary queues by closing proxies on servers
     */
//    servers.forEach(s -> s.invoke(() -> {
//      checkForCacheClientProxy();
//      Collection<CacheClientProxy> secondaries = CacheClientNotifier.getInstance().getClientProxies().stream().filter(proxy -> !proxy.basicIsPrimary()).collect(
//          Collectors.toSet());
//      while (CacheClientNotifier.getInstance().getClientProxies().size() > balancedProxiesPerServer) {
//        secondaries.stream().findFirst().get().close();
//        Thread.sleep(1000);
//        checkForCacheClientProxy();
//        secondaries = CacheClientNotifier.getInstance().getClientProxies().stream().filter(proxy -> !proxy.basicIsPrimary()).collect(
//            Collectors.toSet());
//      }
//    }));

    /**
     * Check status of proxies every 'proxyCheckInterval' for 'totalWaitTimeMs' on each server. Output can get a bit messy due to async logging
     */
//    final long totalWaitTimeMs = 1000;
//    final long proxyCheckInterval = 250;
//    Set<AsyncInvocation<?>> asyncInvocations = new HashSet<>(servers.size());
//    servers.forEach(s -> asyncInvocations.add(s.invokeAsync(() -> {
//      long currentWaitTimeMs = 0;
//      final long startTime = System.currentTimeMillis();
//      while (currentWaitTimeMs <= totalWaitTimeMs) {
//        checkForCacheClientProxy();
//        Thread.sleep(proxyCheckInterval);
//        currentWaitTimeMs = System.currentTimeMillis() - startTime;
//      }
//    })));
//    asyncInvocations.forEach(asyncInvocation -> {
//      try {
//        asyncInvocation.await();
//      } catch (Exception e) {
//        //do nothing
//      }
//    });

    /**
     * Close a random proxy on server 2 then print out proxies on every server
     */
    // servers.get(1).invoke(() ->
    // CacheClientNotifier.getInstance().getClientProxies().stream().findAny().get().close());
//    Thread.sleep(2000);
//    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    /**
     * Bump redundancy on client 1, force redundancy recovery, set redundancy back to original value, then print out proxies on every server
     */
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
    // });
    // Thread.sleep(10000);
    // servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));
  }

  /**
   * Log information about proxies on a server
   */
  public static void checkForCacheClientProxy() {
    CacheClientNotifier notifier = CacheClientNotifier.getInstance();
    if (notifier != null) {
      Collection<CacheClientProxy> proxies = notifier.getClientProxies();
      if (proxies != null) {
        String output = "DEBR Number of ClientCacheProxies: " + proxies.size();
        for (CacheClientProxy proxy : proxies) {
          output =
              output.concat(
                  "\n" + "Proxy " + proxy.proxyID + (proxy.isPrimary() ? " is primary." : " is secondary.")
                      + " Is active: " + proxy.hasRegisteredInterested());
        }
        LogService.getLogger().warn(output);
      } else {
        LogService.getLogger().warn("DEBR Did not find a proxy");
      }
    } else {
      LogService.getLogger().warn("DEBR Did not find a notifier");
    }
  }

}
