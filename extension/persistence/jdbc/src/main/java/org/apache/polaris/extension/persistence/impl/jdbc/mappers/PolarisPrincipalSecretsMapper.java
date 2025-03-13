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

import org.apache.polaris.core.entity.*;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PolarisPrincipalSecretsMapper {
    // Generate the atomic SQL to be run
    public static String fromPolarisPrincipalsSecret(PolarisGrantRecord grantRecord, PolarisGrantRecord prevGrantRecord) {
        // INSERT Query
        if (grantRecord == null) {

        }

        // UPDATE command
        if (prevGrantRecord.equals(grantRecord))  {


        }

        return null;
    }

    // converter from sql result set to GrantRecord
    public static PolarisPrincipalSecrets toPolarisPrincipalSecrets(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return null;
        }

        return new PolarisPrincipalSecrets(
                resultSet.getLong("principal_id"),
                resultSet.getString("principal_client_id"),
                null,
                null,
                resultSet.getString("secret_salt"),
                resultSet.getString("main_secret_hash"),
                resultSet.getString("secondary_secret_hash"));
    }

    // converter from sql result set to GrantRecord
    public static String drop(PolarisPrincipalSecrets principalSecrets) throws SQLException {
        if (principalSecrets == null) {
            return null;
        }

        return "";
    }


    // converter from sql result set to GrantRecord
    public static String readEntities(PolarisPrincipalSecrets principalSecrets) throws SQLException {
        if (principalSecrets == null) {
            return null;
        }

        return "";
    }

}
