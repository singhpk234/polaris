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

package org.apache.polaris.extension.persistence.impl.jdbc.mappers;

import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Entity model representing all attributes of a Polaris Entity. This is used to exchange full
 * entity information with ENTITIES table
 */
public class PolarisBaseEntityMapper {

    // Generate the atomic SQL to be run
    public static String fromEntity(PolarisBaseEntity entity, PolarisBaseEntity previousEntity) {

        // INSERT Query
        if (previousEntity == null) {

        }

        // UPDATE command
        if (previousEntity.equals(entity))  {


        }

        return null;
    }

    public static PolarisBaseEntity toEntity(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return null;
        }

        var entity =
                new PolarisBaseEntity(
                        resultSet.getLong("catalog_id"),
                        resultSet.getLong("id"),
                        PolarisEntityType.fromCode(resultSet.getInt("type_code")),
                        PolarisEntitySubType.fromCode(resultSet.getInt("sub_type_code")),
                        resultSet.getLong("parent_id"),
                        resultSet.getString("name"));

        entity.setEntityVersion(resultSet.getInt("entity_version"));
        entity.setCreateTimestamp(resultSet.getLong("create_timestamp"));
        entity.setDropTimestamp(resultSet.getLong("drop_timestamp"));
        entity.setPurgeTimestamp(resultSet.getLong("purge_timestamp"));
        entity.setToPurgeTimestamp(resultSet.getLong("to_purge_timestamp"));
        entity.setLastUpdateTimestamp(resultSet.getLong("last_update_timestamp"));
        entity.setProperties(resultSet.getString("properties"));
        entity.setGrantRecordsVersion(resultSet.getInt("grant_record_version"));

        return entity;
    }


    public static String drop(PolarisBaseEntity entity) {

        // INSERT Query
        if (entity == null) {

        }

        // generate delete and drop
        return null;
    }
}

