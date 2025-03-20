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

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class ResultSetToObjectConverter {

  public static <T> List<T> convert(ResultSet resultSet, Class<T> targetClass)
      throws SQLException, ReflectiveOperationException {
    List<T> resultList = new ArrayList<>();
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    String[] columnNames = new String[columnCount + 1]; // 1-based indexing

    for (int i = 1; i <= columnCount; i++) {
      columnNames[i] =
          metaData
              .getColumnLabel(i)
              .toLowerCase(Locale.ROOT); // or getColumnName(), lowercase to match field names
    }

    while (resultSet.next()) {
      T object = targetClass.getDeclaredConstructor().newInstance(); // Create a new instance
      for (int i = 1; i <= columnCount; i++) {
        String columnName = columnNames[i];
        Object value = resultSet.getObject(i);

        try {
          Field field = targetClass.getDeclaredField(columnName);
          field.setAccessible(true); // Allow access to private fields
          field.set(object, value);
        } catch (NoSuchFieldException e) {
          // Handle case where column name doesn't match field name (e.g., snake_case vs. camelCase)
          // You could implement more sophisticated matching logic here, or use annotations.
          try {
            Field field = targetClass.getDeclaredField(convertSnakeCaseToCamelCase(columnName));
            field.setAccessible(true);
            field.set(object, value);
          } catch (NoSuchFieldException e2) {
            // if still not found, just skip it.
          }
        }
      }
      resultList.add(object);
    }
    return resultList;
  }

  public static String convertSnakeCaseToCamelCase(String snakeCase) {
    StringBuilder camelCase = new StringBuilder();
    boolean nextUpperCase = false;
    for (char c : snakeCase.toCharArray()) {
      if (c == '_') {
        nextUpperCase = true;
      } else {
        if (nextUpperCase) {
          camelCase.append(Character.toUpperCase(c));
          nextUpperCase = false;
        } else {
          camelCase.append(c);
        }
      }
    }
    return camelCase.toString();
  }

  // Example Usage (with a simple POJO):
  public static class Example {
    private int id;
    private String name;
    private String address; // example of camel case field
    private String snake_case_example; // example of snake case column

    public Example() {}

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getAddress() {
      return address;
    }

    public void setAddress(String address) {
      this.address = address;
    }

    public String getSnake_case_example() {
      return snake_case_example;
    }

    public void setSnake_case_example(String snake_case_example) {
      this.snake_case_example = snake_case_example;
    }

    @Override
    public String toString() {
      return "Example{"
          + "id="
          + id
          + ", name='"
          + name
          + '\''
          + ", address='"
          + address
          + '\''
          + ", snake_case_example='"
          + snake_case_example
          + '\''
          + '}';
    }
  }

  // Example of how to use this code:
  /*
  public static void main(String[] args) {
      try (Connection connection = DriverManager.getConnection("jdbc:your_db_url", "username", "password");
           Statement statement = connection.createStatement();
           ResultSet resultSet = statement.executeQuery("SELECT id, name, address, snake_case_example FROM your_table")) {

          List<Example> examples = ResultSetToObjectConverter.convert(resultSet, Example.class);
          for (Example example : examples) {
              System.out.println(example);
          }

      } catch (SQLException | ReflectiveOperationException e) {
          e.printStackTrace();
      }
  }
   */
}
