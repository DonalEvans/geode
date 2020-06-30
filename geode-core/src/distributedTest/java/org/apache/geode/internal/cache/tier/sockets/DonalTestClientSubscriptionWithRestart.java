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


import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.QueueManagerImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DonalTestClientSubscriptionWithRestart implements Serializable {

  private static final long serialVersionUID = 8957359388026935609L;
  private MemberVM locator;
  private int port;
  private Set<MemberVM> servers = new HashSet<>();
  private List<ClientVM> clients = new ArrayList<>();

  private final int locatorsToStart = 1;
  private static final int serversToStart = 5;
  private static final int clientsToStart = 10;

  private static final int REDUNDANCY = 2;
  private static final boolean IS_DURABLE = false;
  public static final long DURABLE_CLIENT_TIMEOUT = 60;
  public static final String REGION_NAME = "clientRegion";

  private static final int balancedPrimariesPerServer = clientsToStart / serversToStart;
  private static final int balancedProxiesPerServer = balancedPrimariesPerServer * (REDUNDANCY + 1);

  @Rule
  public final ClusterStartupRule cluster =
      new ClusterStartupRule(
          serversToStart + clientsToStart + locatorsToStart);

  @Before
  public void setup() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withProperty("log-level", "WARN"));

    port = locator.getPort();
    for (int i = 0; i < serversToStart; i++) {
      MemberVM server = cluster.startServerVM(i + locatorsToStart,
          s -> s.withConnectionToLocator(port).withProperty("log-level", "WARN"));
      servers.add(server);
    }

    if (IS_DURABLE) {
      for (int i = 0; i < clientsToStart; ++i) {
        String clientID = String.valueOf(i);
        clients.add(cluster.startClientVM(serversToStart + locatorsToStart + i,
            clientCacheRule -> clientCacheRule.withLocatorConnection(port)
                .withProperty("log-level", "WARN")
                .withProperty("durable-client-id", clientID)
                .withProperty("durable-client-timeout", String.valueOf(DURABLE_CLIENT_TIMEOUT))
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
    Map<String, Integer> serversAndPorts = new HashMap<>();
    servers.forEach(server -> serversAndPorts.put(server.getName(), server.getPort()));
    LogService.getLogger().warn("DONAL: servers and ports= " + serversAndPorts);
  }

  @Test
  public void test() throws InterruptedException {
    servers.forEach(s -> s.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
          .create(REGION_NAME);
    }));

    createClientRegionsAndRegisterInterest(false);

    AsyncInvocation<?> doOperations = cluster.getMember(locatorsToStart).invokeAsync(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      boolean flip = true;
//      Timer queueSizeTimer = new Timer();
//      queueSizeTimer.schedule(new PrintQueueSize(), 0, 1000);
      int nonClientVMs = locatorsToStart + serversToStart;
      while (true) {
        Thread.sleep(200);
        for (int i = nonClientVMs; i < nonClientVMs + clientsToStart; ++i) {
          if (flip) {
            region.invalidate(i);
          } else {
            region.put(i, i);
          }
        }
        flip = !flip;
      }
    });

    Thread.sleep(5000);

    rollingRestartAllButServer1(5000, true);

    Thread.sleep(5000);

    servers.forEach(s -> s.invoke(() -> checkForCacheClientProxy(false)));

    doOperations.cancel(true);
  }

  /**
   * Bump redundancy on client 1, force redundancy recovery then set redundancy back to original
   * value
   */
  private void bumpRedundancyOnClient1AndForceRecovery() throws InterruptedException {
    clients.get(0).invoke(() -> {

      QueueManagerImpl queueManager = (QueueManagerImpl) ((PoolImpl)
          ClusterStartupRule.getClientCache().getDefaultPool()).getQueueManager();
      queueManager.incrementRedundancy();
      queueManager.recoverRedundancy(new HashSet<>(), true);
      Thread.sleep(5000);
      queueManager.decrementRedundancy();
    });
  }

  /**
   * Close a random proxy on server 2
   */
  private void closeRandomProxyOnServer2() throws InterruptedException {
    cluster.getMember(locatorsToStart + 1).invoke(() ->
        CacheClientNotifier.getInstance().getClientProxies().stream().findAny().get().close());
  }

  /**
   * Check status of proxies every 'proxyCheckInterval' for 'totalWaitTimeMs' on each server
   * @param totalWaitTimeMs the duration over which to print out proxy information
   * @param proxyCheckInterval the interval to wait between each check
   */
  private void printServerProxyStatus(long totalWaitTimeMs, long proxyCheckInterval) {
    servers.forEach(s -> s.invoke(() -> checkForCacheClientProxy(false)));
    long elapsed = 0;
    long startTime = System.currentTimeMillis();
    while (elapsed < totalWaitTimeMs) {
      try {
        Thread.sleep(proxyCheckInterval);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      servers.forEach(s -> s.invoke(() -> checkForCacheClientProxy(false)));
      elapsed = System.currentTimeMillis() - startTime;
    }
  }

  /**
   * Asynchronously check status of proxies every 'proxyCheckInterval' for 'totalWaitTimeMs' on each
   * server. Output can get a bit messy due to async logging
   * @param totalWaitTimeMs the duration over which to print out proxy information
   * @param proxyCheckInterval the interval to wait between each check
   */
  private void printServerProxyStatusAsync(long totalWaitTimeMs, long proxyCheckInterval) {
    Set<AsyncInvocation<?>> asyncInvocations = new HashSet<>(servers.size());
    servers.forEach(s -> asyncInvocations.add(s.invokeAsync(() -> {
      long currentWaitTimeMs = 0;
      final long startTime = System.currentTimeMillis();
      while (currentWaitTimeMs <= totalWaitTimeMs) {
        checkForCacheClientProxy(false);
        Thread.sleep(proxyCheckInterval);
        currentWaitTimeMs = System.currentTimeMillis() - startTime;
      }
    })));
    asyncInvocations.forEach(asyncInvocation -> {
      try {
        asyncInvocation.await();
      } catch (Exception e) {
        //do nothing
      }
    });
  }

  /**
   * Attempt to balance secondary queues by closing proxies on servers
   */
  private void closeSecondaryQueuesOnOverloadedServers() {
    servers.forEach(s -> s.invoke(() -> {
      checkForCacheClientProxy(false);
      Collection<CacheClientProxy>
          secondaries =
          CacheClientNotifier.getInstance().getClientProxies().stream()
              .filter(proxy -> !proxy.basicIsPrimary()).collect(
              Collectors.toSet());
      while (CacheClientNotifier.getInstance().getClientProxies().size()
          > balancedProxiesPerServer) {
        secondaries.stream().findFirst().get().close();
        Thread.sleep(1000);
        checkForCacheClientProxy(false);
        secondaries =
            CacheClientNotifier.getInstance().getClientProxies().stream()
                .filter(proxy -> !proxy.basicIsPrimary()).collect(
                Collectors.toSet());
      }
    }));
  }

  /**
   * Attempt to balance primary queues by closing proxies on servers
   */
  private void closePrimariesOnOverloadedServers() {
    servers.forEach(s -> s.invoke(() -> {
      checkForCacheClientProxy(false);
      Collection<CacheClientProxy> primaries =
          CacheClientNotifier.getInstance().getClientProxies().stream()
              .filter(CacheClientProxy::basicIsPrimary).collect(Collectors.toSet());
      while (primaries.size() > balancedPrimariesPerServer) {
        primaries.stream().findFirst().get().close();
        Thread.sleep(1000);
        checkForCacheClientProxy(false);
        primaries =
            CacheClientNotifier.getInstance().getClientProxies().stream()
                .filter(CacheClientProxy::basicIsPrimary).collect(Collectors.toSet());
      }
    }));
  }

  /**
   * Rolling restart all servers except server 1
   * @param timeToWaitBetweenRestarts
   * @param logProxiesAfterEachRestart
   */
  private void rollingRestartAllButServer1(long timeToWaitBetweenRestarts,
                                           boolean logProxiesAfterEachRestart) throws InterruptedException {
    for (int i = 0; i < servers.size(); i++) {
      if (i != 0) {
        Thread.sleep(timeToWaitBetweenRestarts);
      }
      MemberVM server = cluster.getMember(i + locatorsToStart);
      String serverName = server.getName();
      if (!serverName.contains("1")) {
        LogService.getLogger().warn("DONAL: Stopping " + serverName);
        server.stop();
        servers.remove(server);
        LogService.getLogger().warn("DONAL: Stopped and removed " + serverName);
        MemberVM
            newServer =
            cluster.startServerVM(i + locatorsToStart,
                s -> s.withConnectionToLocator(port).withProperty("log-level", "WARN"));
        servers.add(newServer);
        LogService.getLogger()
            .warn("DONAL: Started " + newServer.getName() + "-new, port = " + newServer.getPort());
        newServer.invoke(() -> {
          ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
              .create(REGION_NAME);
        });
      }
      if (logProxiesAfterEachRestart) {
        servers.forEach(s -> s.invoke(() -> checkForCacheClientProxy(false)));
      }
    }
  }

  /**
   * Find all queues in all pools on all clients and print the servers they're associated with.
   */
  private void printAllQueueServersOnAllClients() {
    clients.forEach(c -> c.invoke(() -> {
      Collection<Pool> pools = PoolManagerImpl.getPMI().getMap().values();
      pools.forEach(pool -> {
        QueueManagerImpl queueManager = ((PoolImpl) pool).getQueueManager();
        QueueManagerImpl.ConnectionList connections =
            (QueueManagerImpl.ConnectionList) queueManager.getAllConnections();
        LogService.getLogger()
            .warn("DONAL: Primary Server = " + connections.getPrimary().getServer().toString());
        connections.getBackups().forEach(connection -> LogService.getLogger()
            .warn(("DONAL: Secondary server = " + connection.getServer().toString())));
      });
    }));
  }

  /**
   * Close all proxies on server 1
   */
  private void closeAllProxiesOnServer1() {
    cluster.getMember(locatorsToStart)
        .invoke(() -> CacheClientNotifier.getInstance().getClientProxies().forEach(
            CacheClientProxy::close));
  }

  /**
   * Close primaries on server 1
   */
  private void closePrimariesOnServer1() {
    cluster.getMember(locatorsToStart).invoke(() ->
        CacheClientNotifier.getInstance().getClientProxies().forEach(proxy -> {
          if (proxy.isPrimary()) {
            proxy.close();
          }
        }));
  }

  /**
   * Close durable proxies on server 1
   */
  private void closeDurableProxiesOnServer1() {
    cluster.getMember(locatorsToStart).invoke(() ->
        CacheClientNotifier.getInstance().getClientProxies().forEach(proxy -> {
          proxy.setKeepAlive(false);
          LogService.getLogger().warn("DONAL: should keep proxy: " + proxy.close(true, true));
        }));
  }

  /**
   * Disconnect server 1
   */
  private void forceDisconnectServer1AndPrintProxies() throws InterruptedException {
    cluster.getMember(locatorsToStart).forceDisconnect();
  }

  /**
   * Restart server 1 while periodically printing out proxies on all servers
   */
  private void restartServer1AndPrintProxies() throws InterruptedException {
    Thread restartServer = new Thread(() -> {
      MemberVM server1 = cluster.getMember(locatorsToStart);
      server1.stop();
      servers.remove(server1);
      MemberVM newServer1 = cluster.startServerVM(locatorsToStart, s ->
          s.withConnectionToLocator(port).withProperty("log-level", "WARN"));
      servers.add(newServer1);
      newServer1.invoke(() -> {
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
            .create(REGION_NAME);
      });
    });

    Thread printProxies = new Thread(() -> printServerProxyStatus(5000, 500));

    restartServer.start();
    printProxies.start();

    restartServer.join();
    printProxies.join();
  }

  /**
   * On each client, create a proxy region, do a put and register interest in the key, then print
   * out proxies on every server
   * @param logProxiesAfterRegisteringInterest
   */
  private void createClientRegionsAndRegisterInterest(boolean logProxiesAfterRegisteringInterest) {
    clients.forEach(c -> c.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      int keyValue = c.getVM().getId();
      region.put(keyValue, keyValue);
      region.registerInterest(keyValue, IS_DURABLE);
    }));
    servers.forEach(s -> s.invoke(() -> checkForCacheClientProxy(false)));
  }

  /**
   * Log information about proxies on a server
   * @param verbose
   */
  public static void checkForCacheClientProxy(boolean verbose) {
    CacheClientNotifier notifier = CacheClientNotifier.getInstance();
    if (notifier != null) {
      Collection<CacheClientProxy> proxies = notifier.getClientProxies();
      if (proxies != null) {
        StringBuilder
            output =
            new StringBuilder("DONAL: Number of ClientCacheProxies: " + proxies.size());
        if (verbose) {
          for (CacheClientProxy proxy : proxies) {
            output.append("\nProxy ").append(proxy.proxyID)
                .append(proxy.isPrimary() ? " is primary." : " is secondary.")
                .append(" Is active: ")
                .append(proxy.hasRegisteredInterested());
          }
        } else {
          int primaries = 0;
          int secondaries = 0;
          for (CacheClientProxy proxy : proxies) {
            if (proxy.isPrimary()) {
              primaries++;
            } else {
              secondaries++;
            }
          }
          output.append(": primaries = ").append(primaries)
              .append(", secondaries = ").append(secondaries);
        }
        LogService.getLogger().warn(output.toString());
      } else {
        LogService.getLogger().warn("DONAL: Did not find a proxy");
      }
    } else {
      LogService.getLogger().warn("DONAL: Did not find a notifier");
    }
  }

  public static class PrintQueueSize extends TimerTask {
    @Override
    public void run() {
      CacheClientNotifier notifier = CacheClientNotifier.getInstance();
      if (notifier == null) {
        return;
      }
      Collection<CacheClientProxy> clientProxies = notifier.getClientProxies();
      int size = 0;
      for (CacheClientProxy proxy : clientProxies) {
        size += proxy.getQueueSize();
      }
      LogService.getLogger().warn("DONAL: sum of all queue sizes = " + size);
    }
  }

}
