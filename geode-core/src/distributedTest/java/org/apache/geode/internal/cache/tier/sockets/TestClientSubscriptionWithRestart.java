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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.VMProvider;

public class TestClientSubscriptionWithRestart {

  public static final String REGION = "clientRegion";
  MemberVM locator;
  List<MemberVM> servers = new ArrayList<>();
  List<ClientVM> clients = new ArrayList<>();

  private final int locatorsToStart = 1;
  private final int serversToStart = 5;
  private final int clientsToStart = 4;

  @Rule
  public final ClusterStartupRule cluster =
      new ClusterStartupRule(serversToStart + clientsToStart + locatorsToStart);

  @Before
  public void setup() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withProperty("log-level", "WARN"));

    int port = locator.getPort();
    for (int i = 0; i < serversToStart; i++) {
      servers.add(cluster.startServerVM(i + locatorsToStart, s -> s.withConnectionToLocator(port).withProperty("log-level", "WARN")));
    }

    for (int i = 0; i < clientsToStart; ++i) {
      clients.add(cluster.startClientVM(serversToStart + locatorsToStart + i,
        clientCacheRule -> clientCacheRule.withLocatorConnection(port)
            .withProperty("log-level", "WARN")
            .withPoolSubscription(true)
            .withCacheSetup(cf -> cf.setPoolSubscriptionRedundancy(2))));
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
      region.registerInterest(keyValue, false);
    }));
    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    servers.forEach(s -> {
      if (!s.getName().contains("1")) {
        s.stop();
      }
    });

    int port = locator.getPort();
    for (int i = 1; i < serversToStart; i++) {
      servers.remove(i);
      servers.add(i, cluster.startServerVM(i + locatorsToStart, s -> s.withConnectionToLocator(port).withProperty("log-level", "WARN")));
      servers.get(i).invoke(() -> {
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION);
      });
    }
    Thread.sleep(10000);
    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

    servers.get(0).invoke(() -> CacheClientNotifier.getInstance().getClientProxies().forEach(CacheClientProxy::close));
    Thread.sleep(10000);
    servers.forEach(s -> s.invoke(TestClientSubscriptionWithRestart::checkForCacheClientProxy));

//    servers.get(0).stop();
//    servers.remove(0);
//    servers.add(0, cluster.startServerVM(1, s -> s.withConnectionToLocator(port)));
//    servers.get(0).invoke(() -> {
//      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION);
//    });


  }

  public static void checkForCacheClientProxy() {
    CacheClientNotifier notifier = CacheClientNotifier.getInstance();
    if (notifier != null) {
      Collection<CacheClientProxy> proxies = notifier.getClientProxies();
      if (proxies != null) {
        System.out.println("DEBR Number of ClientCacheProxies: " + proxies.size());
        for (CacheClientProxy proxy : proxies) {
          System.out.println("Proxy " + proxy.proxyID + (proxy.isPrimary() ? " is primary." : " is secondary.") + " Is active: " + proxy.hasRegisteredInterested());
        }
      } else {
        System.out.println("DEBR Did not find a proxy");
      }
    } else {
      System.out.println("DEBR Did not find a notifier");
    }
  }

}
