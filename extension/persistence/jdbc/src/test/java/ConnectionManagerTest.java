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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.polaris.extension.persistence.impl.jdbc.ConnectionManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ConnectionManagerTest {

    @Test
    public void testGetConnection_successfulConnection() throws SQLException {
        // Mocking the BasicDataSource and its getConnection method
        BasicDataSource mockDataSource = mock(BasicDataSource.class);
        Connection mockConnection = mock(Connection.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        // Replace the static dataSource with the mockDataSource using Mockito's MockedStatic
        try (MockedStatic<ConnectionManager> mockedConnectionManager = mockStatic(ConnectionManager.class)) {
            mockedConnectionManager.when(() -> ConnectionManager.getConnection()).thenReturn(mockConnection);

            Connection connection = ConnectionManager.getConnection();

            assertNotNull(connection);
            assertEquals(mockConnection, connection);
        }
    }

    @Test
    public void testGetConnection_successfulConnectionX() throws SQLException {
      Connection c = ConnectionManager.getConnection();
      Statement statement = c.createStatement();
      ResultSet s = statement.executeQuery("SELECT 1");
      System.out.println("Hello");
      while (s.next()) {
          System.out.println(s.getString(1));
      }
    }

    @Test
    public void testGetConnection_sqlException() throws SQLException {
        // Mocking the BasicDataSource to throw an SQLException
        BasicDataSource mockDataSource = mock(BasicDataSource.class);
        when(mockDataSource.getConnection()).thenThrow(new SQLException("Database connection failed"));

        // Replace the static dataSource with the mockDataSource using Mockito's MockedStatic
        try (MockedStatic<ConnectionManager> mockedConnectionManager = mockStatic(ConnectionManager.class)) {
            mockedConnectionManager.when(() -> ConnectionManager.getConnection()).thenThrow(new SQLException("Database connection failed"));

            SQLException thrown = assertThrows(SQLException.class, ConnectionManager::getConnection);
            assertEquals("Database connection failed", thrown.getMessage());
        }
    }

    // You may add more test cases for edge cases, such as testing the configuration of the BasicDataSource.
    @Test
    public void testDataSourceConfiguration() {
        // Create an instance of BasicDataSource to test its configuration.
        BasicDataSource dataSource = new BasicDataSource();

        // set the configuration as in the ConnectionManager class.
        dataSource.setUrl("jdbc:postgresql://localhost:3306/postgres");
        dataSource.setUsername("prsingh");
        dataSource.setPassword("");
        dataSource.setMinIdle(5);
        dataSource.setMaxIdle(10);
        dataSource.setMaxOpenPreparedStatements(100);

        // Verify the configurations.
        assertEquals("jdbc:postgresql://localhost:3306/postgres", dataSource.getUrl());
        assertEquals("prsingh", dataSource.getUsername());
        assertEquals("", dataSource.getPassword());
        assertEquals(5, dataSource.getMinIdle());
        assertEquals(10, dataSource.getMaxIdle());
        assertEquals(100, dataSource.getMaxOpenPreparedStatements());
    }
}
