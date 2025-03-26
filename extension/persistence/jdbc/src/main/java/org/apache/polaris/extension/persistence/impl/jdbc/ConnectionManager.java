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
package org.apache.polaris.extension.persistence.impl.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;

public class ConnectionManager {

  private static final BasicDataSource dataSource = new BasicDataSource();

  static {
    // this should get all the strings from the conf file and load it
    // TODO: make connection
    dataSource.setUrl("jdbc:postgresql://localhost:5432/postgres");
    dataSource.setUsername("prsingh");
    dataSource.setPassword("psinghvk");
    // dataSourceConfig.setDriverClassName("org.postgresql.Driver");

    dataSource.setMinIdle(5);
    dataSource.setMaxIdle(10);
    dataSource.setMaxOpenPreparedStatements(100);
  }

  public static Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  private ConnectionManager() {} // prevents instantiation
}
