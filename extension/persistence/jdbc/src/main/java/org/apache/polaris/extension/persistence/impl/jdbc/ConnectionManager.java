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

import java.io.*;
import java.io.IOException;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;

public class ConnectionManager {

  private DataSource dataSource;

  public ConnectionManager(String propertiesFilePath) throws IOException {
    initializeDataSource(propertiesFilePath);
  }

  public static Properties readPropertiesFromResource(String resourceFileName) throws IOException {
    Properties properties = new Properties();
    ClassLoader classLoader = ConnectionManager.class.getClassLoader();
    System.out.println("Using class loader: " + classLoader);
    System.out.println("Parent class loader: " + classLoader.getParent());
    try {
      properties.load(classLoader.getResourceAsStream("db.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return properties;
  }

  private void initializeDataSource(String propertiesFilePath) throws IOException {
    Properties properties = readPropertiesFromResource("db.properties");

    BasicDataSource bds = new BasicDataSource();
    bds.setUrl(properties.getProperty("jdbc.url"));
    bds.setUsername(properties.getProperty("jdbc.username"));
    bds.setPassword(properties.getProperty("jdbc.password"));
    bds.setDriverClassName(properties.getProperty("jdbc.driverClassName"));

    // Optional connection pool configuration
    if (properties.getProperty("dbcp2.initialSize") != null) {
      bds.setInitialSize(Integer.parseInt(properties.getProperty("dbcp2.initialSize")));
    }
    if (properties.getProperty("dbcp2.maxTotal") != null) {
      bds.setMaxTotal(Integer.parseInt(properties.getProperty("dbcp2.maxTotal")));
    }
    if (properties.getProperty("dbcp2.maxIdle") != null) {
      bds.setMaxIdle(Integer.parseInt(properties.getProperty("dbcp2.maxIdle")));
    }
    if (properties.getProperty("dbcp2.minIdle") != null) {
      bds.setMinIdle(Integer.parseInt(properties.getProperty("dbcp2.minIdle")));
    }
    if (properties.getProperty("dbcp2.maxWaitMillis") != null) {
      bds.setMaxWaitMillis(Long.parseLong(properties.getProperty("dbcp2.maxWaitMillis")));
    }
    if (properties.getProperty("dbcp2.testOnBorrow") != null) {
      bds.setTestOnBorrow(Boolean.parseBoolean(properties.getProperty("dbcp2.testOnBorrow")));
    }
    if (properties.getProperty("dbcp2.testOnReturn") != null) {
      bds.setTestOnReturn(Boolean.parseBoolean(properties.getProperty("dbcp2.testOnReturn")));
    }
    if (properties.getProperty("dbcp2.testWhileIdle") != null) {
      bds.setTestWhileIdle(Boolean.parseBoolean(properties.getProperty("dbcp2.testWhileIdle")));
    }
    if (properties.getProperty("dbcp2.validationQuery") != null) {
      bds.setValidationQuery(properties.getProperty("dbcp2.validationQuery"));
    }
    if (properties.getProperty("dbcp2.timeBetweenEvictionRunsMillis") != null) {
      bds.setTimeBetweenEvictionRunsMillis(
          Long.parseLong(properties.getProperty("dbcp2.timeBetweenEvictionRunsMillis")));
    }
    if (properties.getProperty("dbcp2.minEvictableIdleTimeMillis") != null) {
      bds.setMinEvictableIdleTimeMillis(
          Long.parseLong(properties.getProperty("dbcp2.minEvictableIdleTimeMillis")));
    }

    this.dataSource = bds;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public void closeDataSource() throws Exception {
    if (dataSource instanceof BasicDataSource) {
      ((BasicDataSource) dataSource).close();
    }
  }
}
