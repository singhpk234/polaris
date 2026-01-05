--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file--
--  distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"). You may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- MariaDB schema v1 (based on PostgreSQL schema v3)
-- This is the initial schema version for MariaDB support
-- Features:
--  * Uses JSON instead of JSONB (MariaDB equivalent)
--  * Uses LONGTEXT for text fields
--  * Uses MariaDB-specific syntax for upserts and comments
--  * Includes all tables: version, entities, grant_records, principal_authentication_data,
--    policy_mapping_record, events

CREATE DATABASE IF NOT EXISTS POLARIS_SCHEMA;
USE POLARIS_SCHEMA;

CREATE TABLE IF NOT EXISTS version (
    version_key VARCHAR(255) PRIMARY KEY,
    version_value INT NOT NULL
) COMMENT = 'the version of the JDBC schema in use';

INSERT INTO version (version_key, version_value)
VALUES ('version', 1)
ON DUPLICATE KEY UPDATE version_value = VALUES(version_value);

CREATE TABLE IF NOT EXISTS entities (
    realm_id VARCHAR(255) NOT NULL COMMENT 'realm_id used for multi-tenancy',
    catalog_id BIGINT NOT NULL COMMENT 'catalog id',
    id BIGINT NOT NULL COMMENT 'entity id',
    parent_id BIGINT NOT NULL COMMENT 'entity id of parent',
    name LONGTEXT NOT NULL COMMENT 'entity name',
    entity_version INT NOT NULL COMMENT 'version of the entity',
    type_code INT NOT NULL COMMENT 'type code',
    sub_type_code INT NOT NULL COMMENT 'sub type of entity',
    create_timestamp BIGINT NOT NULL COMMENT 'creation time of entity',
    drop_timestamp BIGINT NOT NULL COMMENT 'time of drop of entity',
    purge_timestamp BIGINT NOT NULL COMMENT 'time to start purging entity',
    to_purge_timestamp BIGINT NOT NULL,
    last_update_timestamp BIGINT NOT NULL COMMENT 'last time the entity is touched',
    properties JSON NOT NULL DEFAULT ('{}') COMMENT 'entities properties json',
    internal_properties JSON NOT NULL DEFAULT ('{}') COMMENT 'entities internal properties json',
    grant_records_version INT NOT NULL COMMENT 'the version of grant records change on the entity',
    location_without_scheme LONGTEXT,
    PRIMARY KEY (realm_id, id),
    UNIQUE KEY constraint_name (realm_id, catalog_id, parent_id, type_code, name(255))
) COMMENT = 'all the entities';

-- TODO: create indexes based on all query pattern.
CREATE INDEX idx_entities ON entities (realm_id, catalog_id, id);
CREATE INDEX idx_locations ON entities (realm_id, parent_id, location_without_scheme(255));

CREATE TABLE IF NOT EXISTS grant_records (
    realm_id VARCHAR(255) NOT NULL,
    securable_catalog_id BIGINT NOT NULL COMMENT 'catalog id of the securable',
    securable_id BIGINT NOT NULL COMMENT 'entity id of the securable',
    grantee_catalog_id BIGINT NOT NULL COMMENT 'catalog id of the grantee',
    grantee_id BIGINT NOT NULL COMMENT 'id of the grantee',
    privilege_code INT COMMENT 'privilege code',
    PRIMARY KEY (realm_id, securable_catalog_id, securable_id, grantee_catalog_id, grantee_id, privilege_code)
) COMMENT = 'grant records for entities';

CREATE TABLE IF NOT EXISTS principal_authentication_data (
    realm_id VARCHAR(255) NOT NULL,
    principal_id BIGINT NOT NULL,
    principal_client_id VARCHAR(255) NOT NULL,
    main_secret_hash VARCHAR(255) NOT NULL,
    secondary_secret_hash VARCHAR(255) NOT NULL,
    secret_salt VARCHAR(255) NOT NULL,
    PRIMARY KEY (realm_id, principal_client_id)
) COMMENT = 'authentication data for client';

CREATE TABLE IF NOT EXISTS policy_mapping_record (
    realm_id VARCHAR(255) NOT NULL,
    target_catalog_id BIGINT NOT NULL,
    target_id BIGINT NOT NULL,
    policy_type_code INT NOT NULL,
    policy_catalog_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,
    parameters JSON NOT NULL DEFAULT ('{}'),
    PRIMARY KEY (realm_id, target_catalog_id, target_id, policy_type_code, policy_catalog_id, policy_id)
);

CREATE INDEX idx_policy_mapping_record ON policy_mapping_record (realm_id, policy_type_code, policy_catalog_id, policy_id, target_catalog_id, target_id);

CREATE TABLE IF NOT EXISTS events (
    realm_id VARCHAR(255) NOT NULL,
    catalog_id VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    request_id VARCHAR(255),
    event_type VARCHAR(255) NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    principal_name VARCHAR(255),
    resource_type VARCHAR(255) NOT NULL,
    resource_identifier VARCHAR(255) NOT NULL,
    additional_properties JSON NOT NULL DEFAULT ('{}'),
    PRIMARY KEY (event_id)
);
