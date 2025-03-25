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

import org.apache.polaris.core.persistence.RetryOnConcurrencyException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DatabaseOperations {

  public <T> List<T> executeSelect(String query, Class<T> targetClass) {
    System.out.println("Executing query select query: " + query);
    try (Connection connection = ConnectionManager.getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet s = statement.executeQuery(query);
      List<T> x = ResultSetToObjectConverter.convert(s, targetClass);
      return x == null || x.isEmpty() ? null : x;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
    return null;
  }

  public int executeUpdate(String query) {
    System.out.println("Executing query: " + query);
    try (Connection connection = ConnectionManager.getConnection();
        Statement statement = connection.createStatement()) {
        return statement.executeUpdate(query);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return 0;
  }

  public int executeUpdate(String query, Statement statement) throws SQLException {
    System.out.println("Executing query in transaction : " + query);
    int i = 0;
    try {
       i = statement.executeUpdate(query);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    System.out.println("Query executed: " + i);
    return i;
  }

  public boolean runWithinTransaction(TransactionCallback callback) {
    System.out.println("Executing transaction within callback: " + callback);
    Connection connection = null;
    try {
      connection = ConnectionManager.getConnection();
      connection.setAutoCommit(false); // Disable auto-commit to start a transaction

      boolean result = false;
      try (Statement statement = connection.createStatement()) {
        result = callback.execute(statement);
      }

      if (result) {
        connection.commit(); // Commit the transaction if successful
        return true;
      } else {
        connection.rollback(); // Rollback the transaction if not successful
        return false;
      }

    } catch (SQLException e) {
      if (connection != null) {
        try {
          connection.rollback(); // Rollback on exception
        } catch (SQLException ex) {
          ex.printStackTrace(); // Log rollback exception
        }
      }
      e.printStackTrace();
      return false;
    } finally {
      if (connection != null) {
        try {
          connection.setAutoCommit(true); // Restore auto-commit
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  // Interface for transaction callback
  public interface TransactionCallback {
    boolean execute(Statement statement) throws SQLException;
  }
}
