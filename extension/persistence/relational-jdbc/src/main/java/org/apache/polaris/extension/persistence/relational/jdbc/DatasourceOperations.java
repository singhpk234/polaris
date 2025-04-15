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
package org.apache.polaris.extension.persistence.relational.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasourceOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasourceOperations.class);

  private static final String ALREADY_EXISTS_STATE_POSTGRES = "42P07";
  private static final String CONSTRAINT_VIOLATION_SQL_CODE = "23505";

  private final DataSource datasource;
  private final String realm;

  public DatasourceOperations(DataSource datasource, String realm) {
    this.datasource = datasource;
    this.realm = realm.toUpperCase(Locale.ROOT);
  }

  public void executeScript(String scriptFilePath) throws SQLException {
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    Statement realmSchemaStatement = null;
    try (Connection connection = datasource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", realm));
      statement.close();
      realmSchemaStatement = borrowConnection().createStatement();
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  Objects.requireNonNull(classLoader.getResourceAsStream(scriptFilePath)), UTF_8));
      StringBuilder sqlBuffer = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty() && !line.startsWith("--")) { // Ignore empty lines and comments
          sqlBuffer.append(line).append("\n");
          if (line.endsWith(";")) { // Execute statement when semicolon is found
            String sql = sqlBuffer.toString().trim();
            try {
              int rowsUpdated = realmSchemaStatement.executeUpdate(sql);
              LOGGER.debug("Query {} executed {} rows affected", sql, rowsUpdated);
            } catch (SQLException e) {
              LOGGER.error("Error executing query {}", sql, e);
              // re:throw this as unhandled exception
              throw new RuntimeException(e);
            }
            sqlBuffer.setLength(0); // Clear the buffer for the next statement
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error("Error reading the script file", e);
      throw new RuntimeException(e);
    } catch (SQLException e) {
      LOGGER.error("Error executing the script file", e);
      throw e;
    } finally {
      if (realmSchemaStatement != null) {
        realmSchemaStatement.close();
      }
    }
  }

  public <T> List<T> executeSelect(String query, Class<T> targetClass) throws SQLException {
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement();
        ResultSet s = statement.executeQuery(query)) {
      List<T> results = ResultSetToObjectConverter.convert(s, targetClass);
      return results.isEmpty() ? null : results;
    } catch (SQLException e) {
      LOGGER.error("Error executing query {}", query, e);
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int executeUpdate(String query) throws SQLException {
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement()) {
      return statement.executeUpdate(query);
    } catch (SQLException e) {
      LOGGER.error("Error executing query {}", query, e);
      throw e;
    }
  }

  public int executeUpdate(String query, Statement statement) throws SQLException {
    LOGGER.debug("Executing query {} within transaction", query);
    try {
      return statement.executeUpdate(query);
    } catch (SQLException e) {
      LOGGER.error("Error executing query {}", query, e);
      throw e;
    }
  }

  public void runWithinTransaction(TransactionCallback callback) throws SQLException {
    Connection connection = null;
    try {
      connection = borrowConnection();
      connection.setAutoCommit(false); // Disable auto-commit to start a transaction

      boolean result;
      try (Statement statement = connection.createStatement()) {
        result = callback.execute(statement);
      }

      if (result) {
        connection.commit(); // Commit the transaction if successful
      } else {
        connection.rollback(); // Rollback the transaction if not successful
      }

    } catch (SQLException e) {
      if (connection != null) {
        try {
          connection.rollback(); // Rollback on exception
        } catch (SQLException ex) {
          LOGGER.error("Error rolling back transaction", ex);
          throw e;
        }
      }
      LOGGER.error("Caught Error while executing transaction", e);
      throw e;
    } finally {
      if (connection != null) {
        try {
          connection.setAutoCommit(true); // Restore auto-commit
          connection.close();
        } catch (SQLException e) {
          LOGGER.error("Error closing connection", e);
        }
      }
    }
  }

  // Interface for transaction callback
  public interface TransactionCallback {
    boolean execute(Statement statement) throws SQLException;
  }

  public boolean isConstraintViolation(SQLException e) {
    return CONSTRAINT_VIOLATION_SQL_CODE.equals(e.getSQLState());
  }

  public boolean isAlreadyExistsException(SQLException e) {
    return ALREADY_EXISTS_STATE_POSTGRES.equals(e.getSQLState());
  }

  private Connection borrowConnection() throws SQLException {
    Connection connection = datasource.getConnection();
    if (connection
        .getMetaData()
        .getDatabaseProductName()
        .toLowerCase(Locale.ROOT)
        .equals("postgresql")) {
      // connection.setSchema() doesn't work hence work this around.
      Statement s = connection.createStatement();
      s.execute("SET search_path TO " + realm);
      s.close();
    } else {
      connection.setSchema(realm);
    }

    return connection;
  }
}
