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
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.extension.persistence.impl.jdbc.JdbcCrudQueryGenerator;
import org.junit.jupiter.api.Test;

public class JdbcCrudQueryGeneratorTest {

  public static class TestEntity {
    private int id;
    private String name;
    private double price;

    public TestEntity() {}

    public TestEntity(int id, String name, double price) {
      this.id = id;
      this.name = name;
      this.price = price;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public double getPrice() {
      return price;
    }
  }

  @Test
  public void testGenerateSelectQuery_FilterString() {
    String filter = "id = 1 AND name = 'Test'";
    String expected = "SELECT id, name, price FROM TestEntity WHERE id = 1 AND name = 'Test'";
    String actual =
        JdbcCrudQueryGenerator.generateSelectQuery(TestEntity.class, filter, null, null, null);
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateSelectQuery_WhereClauseMap() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("id", 1);
    whereClause.put("name", "Test");
    String expected = "SELECT id, name, price FROM TestEntity WHERE name = 'Test' AND id = 1";
    String actual =
        JdbcCrudQueryGenerator.generateSelectQuery(TestEntity.class, whereClause, null, null, null);
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateSelectQuery_WhereClauseMapWithLimitOffsetOrder() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("id", 1);
    String expected =
        "SELECT id, name, price FROM TestEntity WHERE id = 1 ORDER BY name LIMIT 10 OFFSET 5";
    String actual =
        JdbcCrudQueryGenerator.generateSelectQuery(TestEntity.class, whereClause, 10, 5, "name");
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateInsertQuery() {
    TestEntity entity = new TestEntity(1, "Test", 19.99);
    String expected = "INSERT INTO TestEntity (id, name, price) VALUES (1, Test, 19.99)";
    String actual = JdbcCrudQueryGenerator.generateInsertQuery(entity, "TestEntity");
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateInsertQuery_NullObject() {
    assertNull(JdbcCrudQueryGenerator.generateInsertQuery(null, "TestEntity"));
  }

  @Test
  public void testGenerateInsertQuery_NullTableName() {
    TestEntity entity = new TestEntity(1, "Test", 19.99);
    assertNull(JdbcCrudQueryGenerator.generateInsertQuery(entity, null));
  }

  @Test
  public void testGenerateUpdateQuery_WhereClauseMap() {
    TestEntity entity = new TestEntity(1, "Updated", 29.99);
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("id", 1);
    String expected = "UPDATE TestEntity SET id = 1, name = Updated, price = 29.99 WHERE id = 1";
    String actual = JdbcCrudQueryGenerator.generateUpdateQuery(entity, whereClause, "TestEntity");
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateDeleteQuery_WhereClauseMap() {
    Map<String, Object> whereClause = new HashMap<>();
    whereClause.put("id", 1);
    String expected = "DELETE FROM TestEntity WHERE id = 1";
    String actual = JdbcCrudQueryGenerator.generateDeleteQuery(whereClause, "TestEntity");
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateDeleteQuery_Object() {
    TestEntity entity = new TestEntity(1, "Test", 19.99);
    String expected = "DELETE FROM TestEntity WHERE id = 1 AND name = 'Test' AND price = 19.99";
    String actual = JdbcCrudQueryGenerator.generateDeleteQuery(entity, "TestEntity");
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateDeleteQuery_WhereClauseString() {
    String whereClause = "id = 1";
    String expected = "DELETE FROM TestEntity WHERE id = 1";
    String actual = JdbcCrudQueryGenerator.generateDeleteQuery(whereClause, "TestEntity");
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateSelectQuery_EmptyWhereClause() {
    Map<String, Object> whereClause = new HashMap<>();
    String expected = "SELECT id, name, price FROM TestEntity";
    String actual =
        JdbcCrudQueryGenerator.generateSelectQuery(TestEntity.class, whereClause, null, null, null);
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateSelectQuery_NullWhereClause() {
    String expected = "SELECT id, name, price FROM TestEntity";
    String actual =
        JdbcCrudQueryGenerator.generateSelectQuery(TestEntity.class, "", null, null, null);
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateUpdateQuery_NullWhereClause() {
    TestEntity entity = new TestEntity(1, "Updated", 29.99);
    String expected = "UPDATE TestEntity SET id = 1, name = Updated, price = 29.99";
    String actual = JdbcCrudQueryGenerator.generateUpdateQuery(entity, null, "TestEntity");
    assertEquals(expected, actual);
  }

  @Test
  public void testGenerateDeleteQuery_NullWhereClauseMap() {
    String expected = "DELETE FROM TestEntity";
    String actual = JdbcCrudQueryGenerator.generateDeleteQuery("", "TestEntity");
    assertEquals(expected, actual);
  }
}
