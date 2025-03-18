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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcCrudQueryGenerator {

    public static String generateSelectQuery(Class<?> entityClass, Map<String, Object> whereClause, Integer limit, Integer offset, String orderBy) {
        String tableName = entityClass.getSimpleName();
        List<String> fields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
            fields.add(field.getName());
        }

        String columns = String.join(", ", fields);
        StringBuilder query = new StringBuilder("SELECT ").append(columns).append(" FROM ").append(tableName);

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

    public static String generateInsertQuery(Class<?> entityClass, Map<String, Object> values) {
        String tableName = entityClass.getSimpleName();
        List<String> columns = new ArrayList<>();
        List<String> valuePlaceholders = new ArrayList<>();
        List<Object> actualValues = new ArrayList<>();

        for (Map.Entry<String, Object> entry : values.entrySet()) {
            columns.add(entry.getKey());
            valuePlaceholders.add("?"); // Use placeholders for prepared statements
            actualValues.add(entry.getValue());
        }

        String columnsString = String.join(", ", columns);
        String placeholdersString = String.join(", ", valuePlaceholders);

        return "INSERT INTO " + tableName + " (" + columnsString + ") VALUES (" + placeholdersString + ")";
    }

    public static String generateUpdateQuery(Class<?> entityClass, Map<String, Object> values, Map<String, Object> whereClause) {
        String tableName = entityClass.getSimpleName();
        List<String> setClauses = new ArrayList<>();
        List<String> whereConditions = new ArrayList<>();

        for (Map.Entry<String, Object> entry : values.entrySet()) {
            setClauses.add(entry.getKey() + " = ?"); // Placeholders
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

        return "UPDATE " + tableName + " SET " + setClausesString + (whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereConditionsString : "");
    }

    public static String generateDeleteQuery(Class<?> entityClass, Map<String, Object> whereClause) {
        String tableName = entityClass.getSimpleName();
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

        return "DELETE FROM " + tableName + (whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereConditionsString : "");
    }

    // List command is already implemented in the Select query.
    // Example Entity Class (similar to JPA entity)
    public static class User {
        public int id;
        public String name;
        public int age;
        public String city;

        public User(int id, String name, int age, String city) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.city = city;
        }

        public User(){} //default constructor for reflection use.
    }

    public static void main(String[] args) {
        // Example Usage
        Map<String, Object> userValues = new HashMap<>();
        userValues.put("name", "Alice");
        userValues.put("age", 25);
        userValues.put("city", "London");

        String insertQuery = generateInsertQuery(User.class, userValues);
        System.out.println("Insert Query: " + insertQuery);

        Map<String, Object> updateValues = new HashMap<>();
        updateValues.put("age", 26);

        Map<String, Object> whereClause = new HashMap<>();
        whereClause.put("name", "Alice");

        String updateQuery = generateUpdateQuery(User.class, updateValues, whereClause);
        System.out.println("Update Query: " + updateQuery);

        String deleteQuery = generateDeleteQuery(User.class, whereClause);
        System.out.println("Delete Query: " + deleteQuery);

        String selectQuery = generateSelectQuery(User.class, whereClause, null, null, null);
        System.out.println("Select Query: " + selectQuery);

        String listAll = generateSelectQuery(User.class, null, null, null, null);
        System.out.println("List All Users: " + listAll);

        String listFiltered = generateSelectQuery(User.class, whereClause, 10, 0, "age");
        System.out.println("List Filtered Users: " + listFiltered);

    }
}