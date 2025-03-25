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

import org.apache.polaris.extension.persistence.impl.jdbc.models.ModelGrantRecord;
import org.apache.polaris.extension.persistence.impl.jdbc.models.ModelPrincipalSecrets;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcCrudQueryGenerator {

  private static final Pattern CAMEL_CASE_PATTERN =
      Pattern.compile("(?<=[a-z0-9])[A-Z]|(?<=[A-Z])[A-Z](?=[a-z])");

  public static String generateSelectQuery(
      Class<?> entityClass, String filter, Integer limit, Integer offset, String orderBy) {
    String tableName = "entities";
    if (entityClass.equals(ModelGrantRecord.class)) {
      tableName = "grant_records";
    } else if (entityClass.equals(ModelPrincipalSecrets.class)) {
      tableName = "principal_secrets";
    }
    tableName = "polaris." + tableName;
    List<String> fields = new ArrayList<>();

    for (Field field : entityClass.getDeclaredFields()) {
      fields.add(camelToSnake(field.getName()));
    }

    String columns = String.join(", ", fields);
    StringBuilder query =
        new StringBuilder("SELECT ").append(columns).append(" FROM ").append(tableName);
    if (filter != null && !filter.isEmpty()) {
      query.append(" WHERE ").append(String.join(" AND ", filter));
    }
    return query.toString();
  }

  public static String generateSelectQuery(
      Class<?> entityClass,
      Map<String, Object> whereClause,
      Integer limit,
      Integer offset,
      String orderBy) {
    String tableName = "entities";
    if (entityClass.equals(ModelGrantRecord.class)) {
      tableName = "grant_records";
    } else if (entityClass.equals(ModelPrincipalSecrets.class)) {
      tableName = "principal_secrets";
    }
    tableName = "polaris." + tableName;
    List<String> fields = new ArrayList<>();

    for (Field field : entityClass.getDeclaredFields()) {
      fields.add(camelToSnake(field.getName()));
    }

    String columns = String.join(", ", fields);
    StringBuilder query =
        new StringBuilder("SELECT ").append(columns).append(" FROM ").append(tableName);

    if (whereClause != null && !whereClause.isEmpty()) {
      List<String> whereConditions = new ArrayList<>();
      for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
        String fieldName = entry.getKey();
        Object value = entry.getValue();
        if (value instanceof String) {
          whereConditions.add(fieldName + " = '" + value + "'");
        } else {
          whereConditions.add(fieldName + " = " + value);
        }
      }
      query.append(" WHERE ").append(String.join(" AND ", whereConditions));
    }

    if (orderBy != null && !orderBy.isEmpty()) {
      query.append(" ORDER BY ").append(orderBy);
    }

    if (limit != null) {
      query.append(" LIMIT ").append(limit);
    }

    if (offset != null && limit != null) { // Offset only makes sense with limit.
      query.append(" OFFSET ").append(offset);
    }

    return query.toString();
  }

  public static String generateInsertQuery(Object object, String tableName) {
    if (object == null || tableName == null || tableName.isEmpty()) {
      return null; // Or throw an exception
    }

    Class<?> objectClass = object.getClass();
    Field[] fields = objectClass.getDeclaredFields();
    List<String> columnNames = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (Field field : fields) {
      field.setAccessible(true); // Allow access to private fields
      try {
        Object value = field.get(object);
        if (value != null) { // Only include non-null fields
          columnNames.add(camelToSnake(field.getName()));
          values.add("'" + value.toString() + "'");
        }
      } catch (IllegalAccessException e) {
        e.printStackTrace(); // Handle the exception appropriately
        return null; // Or throw an exception
      }
    }

    if (columnNames.isEmpty()) {
      return null; // Or throw an exception if no non-null fields are found
    }

    String columns = String.join(", ", columnNames);
    String valuesString = String.join(", ", values);

    return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + valuesString + ")";
  }

  public static String generateUpdateQuery(
      Object object, Map<String, Object> whereClause, String tableName) {
    List<String> setClauses = new ArrayList<>();
    List<String> whereConditions = new ArrayList<>();
    Class<?> objectClass = object.getClass();
    Field[] fields = objectClass.getDeclaredFields();
    List<String> columnNames = new ArrayList<>();
    List<Object> values = new ArrayList<>();

    for (Field field : fields) {
      field.setAccessible(true); // Allow access to private fields
      try {
        Object value = field.get(object);
        if (value != null) { // Only include non-null fields
          columnNames.add(camelToSnake(field.getName()));
          values.add("'" + value.toString() + "'");
        }
      } catch (IllegalAccessException e) {
        e.printStackTrace(); // Handle the exception appropriately
        return null; // Or throw an exception
      }
    }

    for (int i = 0; i < columnNames.size(); i++) {
      setClauses.add(columnNames.get(i) + " = " + values.get(i)); // Placeholders
    }

    if (whereClause != null && !whereClause.isEmpty()) {
      for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
        String fieldName = entry.getKey();
        Object value = entry.getValue();
        if (value instanceof String) {
          whereConditions.add(fieldName + " = '" + value + "'");
        } else {
          whereConditions.add(fieldName + " = " + value);
        }
      }
    }

    String setClausesString = String.join(", ", setClauses);
    String whereConditionsString = String.join(" AND ", whereConditions);

    return "UPDATE "
        + tableName
        + " SET "
        + setClausesString
        + (whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereConditionsString : "");
  }

  public static String generateDeleteQuery(Map<String, Object> whereClause, String tableName) {
    List<String> whereConditions = new ArrayList<>();

    if (whereClause != null && !whereClause.isEmpty()) {
      for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
        String fieldName = entry.getKey();
        Object value = entry.getValue();
        if (value instanceof String) {
          whereConditions.add(fieldName + " = '" + value + "'");
        } else {
          whereConditions.add(fieldName + " = " + value);
        }
      }
    }

    String whereConditionsString = String.join(" AND ", whereConditions);

    return "DELETE FROM "
        + tableName
        + (whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereConditionsString : "");
  }

  public static String generateDeleteQuery(Object obj, String tableName) {
    List<String> whereConditions = new ArrayList<>();

    Class<?> objectClass = obj.getClass();
    Field[] fields = objectClass.getDeclaredFields();

    for (Field field : fields) {
      field.setAccessible(true); // Allow access to private fields
      try {
        Object value = field.get(obj);
        if (value != null) { // Only include non-null fields
          if (value instanceof String) {
            whereConditions.add(camelToSnake(field.getName()) + " = '" + value + "'");
          } else {
            whereConditions.add(camelToSnake(field.getName()) + " = " + value);
          }
        }
      } catch (IllegalAccessException e) {
        e.printStackTrace(); // Handle the exception appropriately
        return null; // Or throw an exception
      }
    }

    String whereConditionsString = "";
    if (!whereConditions.isEmpty()) {
      whereConditionsString = " WHERE " + String.join(" AND ", whereConditions);
    }

    return "DELETE FROM " + tableName + whereConditionsString;
  }

  public static String generateDeleteQuery(String whereClause, String tableName) {
    String whereConditionsString = "";
    if (whereClause != null && !whereClause.isEmpty()) {
      whereConditionsString = " WHERE " + whereClause;
    }
    return "DELETE FROM " + tableName + whereConditionsString;
  }

  public static String camelToSnake(String camelCase) {
    Matcher matcher = CAMEL_CASE_PATTERN.matcher(camelCase);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      matcher.appendReplacement(sb, "_" + matcher.group(0).toLowerCase(Locale.ROOT));
    }
    matcher.appendTail(sb);
    return sb.toString().toLowerCase(Locale.ROOT);
  }
}
