/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.test.commons;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.containers.MariaDBContainer;

public class MariaDbRelationalJdbcLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  public static final String INIT_SCRIPT = "init-script";

  private MariaDBContainer<?> mariadb;
  private String initScript;
  private DevServicesContext context;

  @Override
  public void init(Map<String, String> initArgs) {
    initScript = initArgs.get(INIT_SCRIPT);
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    mariadb = new MariaDBContainer<>("mariadb:11.6");

    if (initScript != null) {
      mariadb.withInitScript(initScript);
    }

    context.containerNetworkId().ifPresent(mariadb::withNetworkMode);
    mariadb.start();

    // MariaDB-specific configuration
    // Explicitly configure database type as mariadb for proper identification
    return Map.of(
        "polaris.persistence.type",
        "relational-jdbc",
        "polaris.persistence.relational.jdbc.database-type",
        "mariadb",
        "quarkus.datasource.db-kind",
        "mariadb",
        "quarkus.datasource.jdbc.url",
        mariadb.getJdbcUrl(),
        "quarkus.datasource.username",
        mariadb.getUsername(),
        "quarkus.datasource.password",
        mariadb.getPassword(),
        "quarkus.datasource.jdbc.initial-size",
        "10");
  }

  @Override
  public void stop() {
    if (mariadb != null) {
      try {
        mariadb.stop();
      } finally {
        mariadb = null;
      }
    }
  }

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }
}
