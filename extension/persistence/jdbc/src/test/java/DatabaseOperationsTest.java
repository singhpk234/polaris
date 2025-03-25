///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//import static org.junit.jupiter.api.Assertions.*;
//import static org.mockito.Mockito.*;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import org.apache.polaris.extension.persistence.impl.jdbc.ConnectionManager;
//import org.apache.polaris.extension.persistence.impl.jdbc.DatabaseOperations;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.ArgumentCaptor;
//import org.mockito.Mock;
//import org.mockito.MockedStatic;
//import org.mockito.Mockito;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//@ExtendWith(MockitoExtension.class)
//public class DatabaseOperationsTest {
//
//  @Mock private Connection mockConnection;
//  @Mock private Statement mockStatement;
//  @Mock private ResultSet mockResultSet;
//
//  private DatabaseOperations dbOps;
//  private MockedStatic<ConnectionManager> mockedConnectionManager;
//
//  @BeforeEach
//  void setUp() throws SQLException {
//    dbOps = new DatabaseOperations();
//    mockedConnectionManager = Mockito.mockStatic(ConnectionManager.class);
//    mockedConnectionManager.when(ConnectionManager::getConnection).thenReturn(mockConnection);
//    when(mockConnection.createStatement()).thenReturn(mockStatement);
//  }
//
//  @Test
//  void testExecuteSelect() throws SQLException {
//    when(mockStatement.executeQuery("SELECT 1")).thenReturn(mockResultSet);
//    when(mockResultSet.next()).thenReturn(true, false);
//    when(mockResultSet.getInt(1)).thenReturn(1);
//
//    ResultSet resultSet = dbOps.executeSelect("SELECT 1");
//
//    assertNotNull(resultSet);
//    assertTrue(resultSet.next());
//    assertEquals(1, resultSet.getInt(1));
//    verify(mockStatement).executeQuery("SELECT 1");
//  }
//
//  @Test
//  void testExecuteUpdateQuery() throws SQLException {
//    when(mockStatement.executeUpdate("UPDATE test_table SET value = 10")).thenReturn(1);
//
//    int result = dbOps.executeUpdate("UPDATE test_table SET value = 10");
//
//    assertEquals(1, result);
//    verify(mockStatement).executeUpdate("UPDATE test_table SET value = 10");
//  }
//
//  @Test
//  void testExecuteUpdateStatement() throws SQLException {
//    Statement s = ConnectionManager.getConnection().createStatement();
//    dbOps.executeUpdate("UPDATE test_table SET value = 10", s);
//    verify(mockStatement).executeUpdate("UPDATE test_table SET value = 10");
//  }
//
//  @Test
//  void testRunWithinTransactionSuccess() throws SQLException {
//    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
//
//    boolean success =
//        dbOps.runWithinTransaction(
//            statement -> {
//              try {
//                dbOps.executeUpdate(
//                    "CREATE TABLE IF NOT EXISTS test_transaction (id INT PRIMARY KEY, value INT)",
//                    statement);
//                dbOps.executeUpdate("DELETE FROM test_transaction", statement);
//                dbOps.executeUpdate(
//                    "INSERT INTO test_transaction (id, value) VALUES (1, 10)", statement);
//                dbOps.executeUpdate(
//                    "UPDATE test_transaction SET value = 20 WHERE id = 1", statement);
//                return true;
//              } catch (SQLException e) {
//                e.printStackTrace();
//                return false;
//              }
//            });
//
//    assertTrue(success);
//    verify(mockConnection).commit();
//    verify(mockConnection, never()).rollback();
//
//    verify(mockStatement, times(4)).executeUpdate(sqlCaptor.capture());
//    assertEquals(
//        "CREATE TABLE IF NOT EXISTS test_transaction (id INT PRIMARY KEY, value INT)",
//        sqlCaptor.getAllValues().get(0));
//    assertEquals("DELETE FROM test_transaction", sqlCaptor.getAllValues().get(1));
//    assertEquals(
//        "INSERT INTO test_transaction (id, value) VALUES (1, 10)", sqlCaptor.getAllValues().get(2));
//    assertEquals(
//        "UPDATE test_transaction SET value = 20 WHERE id = 1", sqlCaptor.getAllValues().get(3));
//    verify(mockConnection).setAutoCommit(false);
//    verify(mockConnection).setAutoCommit(true);
//    verify(mockConnection).close();
//  }
//
//  @Test
//  void testRunWithinTransactionFailure() throws SQLException {
//    Statement s = ConnectionManager.getConnection().createStatement();
//    doReturn(1)
//        .when(mockStatement)
//        .executeUpdate(
//            "CREATE TABLE IF NOT EXISTS test_transaction (id INT PRIMARY KEY, value INT)");
//    doReturn(1).when(mockStatement).executeUpdate("DELETE FROM test_transaction");
//    doReturn(1)
//        .when(mockStatement)
//        .executeUpdate("INSERT INTO test_transaction (id, value) VALUES (1, 10)");
//    doThrow(new SQLException("Simulated Exception"))
//        .when(s)
//        .executeUpdate("INSERT INTO test_transaction (id, value) VALUES (1, 20)");
//
//    boolean success =
//        dbOps.runWithinTransaction(
//            statement -> {
//              try {
//                dbOps.executeUpdate(
//                    "CREATE TABLE IF NOT EXISTS test_transaction (id INT PRIMARY KEY, value INT)",
//                    statement);
//                dbOps.executeUpdate("DELETE FROM test_transaction", statement);
//                dbOps.executeUpdate(
//                    "INSERT INTO test_transaction (id, value) VALUES (1, 10)", statement);
//                dbOps.executeUpdate(
//                    "INSERT INTO test_transaction (id, value) VALUES (1, 20)", statement);
//                return true;
//              } catch (SQLException e) {
//                return false;
//              }
//            });
//
//    assertFalse(success);
//    verify(mockConnection).rollback();
//    verify(mockConnection, never()).commit();
//    verify(mockStatement, times(4)).executeUpdate(anyString());
//    verify(mockConnection).setAutoCommit(false);
//    verify(mockConnection).setAutoCommit(true);
//    verify(mockConnection).close();
//  }
//
//  @AfterEach
//  public void closeMocks() {
//    if (mockedConnectionManager != null) {
//      mockedConnectionManager.close();
//    }
//  }
//}
