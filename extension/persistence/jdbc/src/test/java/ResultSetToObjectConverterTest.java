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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.polaris.extension.persistence.impl.jdbc.ResultSetToObjectConverter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ResultSetToObjectConverterTest {

  @Test
  public void testConvert_successfulConversion() throws SQLException, ReflectiveOperationException {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getColumnCount()).thenReturn(4);
    Mockito.when(metaData.getColumnLabel(1)).thenReturn("id");
    Mockito.when(metaData.getColumnLabel(2)).thenReturn("name");
    Mockito.when(metaData.getColumnLabel(3)).thenReturn("address");
    Mockito.when(metaData.getColumnLabel(4)).thenReturn("snake_case_example");

    Mockito.when(resultSet.next()).thenReturn(true, true, false); // Two rows
    Mockito.when(resultSet.getObject(1)).thenReturn(1, 2);
    Mockito.when(resultSet.getObject(2)).thenReturn("test1", "test2");
    Mockito.when(resultSet.getObject(3)).thenReturn("addr1", "addr2");
    Mockito.when(resultSet.getObject(4)).thenReturn("snake1", "snake2");

    List<ResultSetToObjectConverter.Example> result =
        ResultSetToObjectConverter.convert(resultSet, ResultSetToObjectConverter.Example.class);

    assertEquals(2, result.size());
    assertEquals(1, result.get(0).getId());
    assertEquals("test1", result.get(0).getName());
    assertEquals("addr1", result.get(0).getAddress());
    assertEquals("snake1", result.get(0).getSnake_case_example());

    assertEquals(2, result.get(1).getId());
    assertEquals("test2", result.get(1).getName());
    assertEquals("addr2", result.get(1).getAddress());
    assertEquals("snake2", result.get(1).getSnake_case_example());
  }

  @Test
  public void testConvert_emptyResultSet() throws SQLException, ReflectiveOperationException {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getColumnCount()).thenReturn(0);
    Mockito.when(resultSet.next()).thenReturn(false);

    List<ResultSetToObjectConverter.Example> result =
        ResultSetToObjectConverter.convert(resultSet, ResultSetToObjectConverter.Example.class);

    assertEquals(0, result.size());
  }

  @Test
  public void testConvert_columnNameMismatch() throws SQLException, ReflectiveOperationException {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getColumnCount()).thenReturn(1);
    Mockito.when(metaData.getColumnLabel(1))
        .thenReturn("invalid_column_name"); // Column name doesn't match field

    Mockito.when(resultSet.next()).thenReturn(true, false);
    Mockito.when(resultSet.getObject(1)).thenReturn("value");

    List<ResultSetToObjectConverter.Example> result =
        ResultSetToObjectConverter.convert(resultSet, ResultSetToObjectConverter.Example.class);

    assertEquals(1, result.size());
    // Fields that don't match are left at the default value.
    assertEquals(0, result.get(0).getId());
    assertEquals(null, result.get(0).getName());
    assertEquals(null, result.get(0).getAddress());
    assertEquals(null, result.get(0).getSnake_case_example());
  }

  @Test
  public void testConvert_sqlException() throws SQLException {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(resultSet.getMetaData()).thenThrow(new SQLException("SQL Error"));
    assertThrows(
        SQLException.class,
        () ->
            ResultSetToObjectConverter.convert(
                resultSet, ResultSetToObjectConverter.Example.class));
  }

  @Test
  public void testConvert_reflectiveOperationException() throws SQLException {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
    Mockito.when(metaData.getColumnCount()).thenReturn(1);
    Mockito.when(metaData.getColumnLabel(1)).thenReturn("id");

    Mockito.when(resultSet.next()).thenReturn(true, false);
    Mockito.when(resultSet.getObject(1))
        .thenReturn("invalidType"); // wrong type, will cause reflection exception

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ResultSetToObjectConverter.convert(
                resultSet, ResultSetToObjectConverter.Example.class));
  }

  @Test
  public void testConvertSnakeCaseToCamelCase_basic() {
    assertEquals(
        "snakeCaseExample",
        ResultSetToObjectConverter.convertSnakeCaseToCamelCase("snake_case_example"));
  }

  @Test
  public void testConvertSnakeCaseToCamelCase_multipleUnderscores() {
    assertEquals(
        "multiWordExample",
        ResultSetToObjectConverter.convertSnakeCaseToCamelCase("multi_word_example"));
  }

  @Test
  public void testConvertSnakeCaseToCamelCase_noUnderscores() {
    assertEquals(
        "noUnderscores", ResultSetToObjectConverter.convertSnakeCaseToCamelCase("noUnderscores"));
  }

  @Test
  public void testConvertSnakeCaseToCamelCase_emptyString() {
    assertEquals("", ResultSetToObjectConverter.convertSnakeCaseToCamelCase(""));
  }

  @Test
  public void testConvertSnakeCaseToCamelCase_singleUnderscore() {
    assertEquals("aB", ResultSetToObjectConverter.convertSnakeCaseToCamelCase("a_b"));
  }
}
