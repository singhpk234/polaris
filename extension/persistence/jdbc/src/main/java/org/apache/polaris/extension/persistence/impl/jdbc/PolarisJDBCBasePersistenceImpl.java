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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.sql.ResultSet;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.extension.persistence.impl.jdbc.models.ModelEntity;
import org.apache.polaris.extension.persistence.impl.jdbc.models.ModelGrantRecord;
import org.apache.polaris.extension.persistence.impl.jdbc.models.ModelPrincipalSecrets;

public class PolarisJDBCBasePersistenceImpl implements BasePersistence, IntegrationPersistence {

  private final DatabaseOperations databaseOperations;
  private final PrincipalSecretsGenerator secretsGenerator;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;

  public PolarisJDBCBasePersistenceImpl(
      DatabaseOperations databaseOperations,
      PrincipalSecretsGenerator secretsGenerator,
      PolarisStorageIntegrationProvider storageIntegrationProvider) {
    this.databaseOperations = databaseOperations;
    this.secretsGenerator = secretsGenerator;
    this.storageIntegrationProvider = storageIntegrationProvider;
  }

  @Override
  public long generateNewId(PolarisCallContext callCtx) {
    return 1;
  }

  @Override
  public void writeEntity(
      PolarisCallContext callCtx,
      PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      PolarisBaseEntity originalEntity) {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity);
    String query;
    if (originalEntity == null) {
      query = JdbcCrudQueryGenerator.generateInsertQuery(modelEntity, "entities");
    } else {
      Map<String, Object> params = new HashMap<>();
      query = JdbcCrudQueryGenerator.generateUpdateQuery(modelEntity, params, "entities");
    }
    databaseOperations.executeUpdate(query);
  }

  @Override
  public void writeEntities(
      PolarisCallContext callCtx,
      List<PolarisBaseEntity> entities,
      List<PolarisBaseEntity> originalEntities) {
    databaseOperations.runWithinTransaction(
        statement -> {
          for (int i = 0; i < entities.size(); i++) {
            PolarisBaseEntity entity = entities.get(i);
            ModelEntity modelEntity = ModelEntity.fromEntity(entity);
            String query;
            if (originalEntities == null || originalEntities.get(i) == null) {
              query = JdbcCrudQueryGenerator.generateInsertQuery(modelEntity, "ENTITIES");
            } else {
              Map<String, Object> params = new HashMap<>();
              query = JdbcCrudQueryGenerator.generateUpdateQuery(modelEntity, params, "ENTITIES");
            }
            databaseOperations.executeUpdate(query, statement);
          }
          return true;
        });
  }

  @Override
  public void writeToGrantRecords(PolarisCallContext callCtx, PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec);
    String query = JdbcCrudQueryGenerator.generateInsertQuery(modelGrantRecord, "GRANT_RECORDS");
    databaseOperations.executeUpdate(query);
  }

  @Override
  public void deleteEntity(PolarisCallContext callCtx, PolarisBaseEntity entity) {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity);
    Map<String, Object> params = new HashMap<>();
    params.put("id", modelEntity.getId());
    params.put("catalog_id", modelEntity.getCatalogId());
    String query = JdbcCrudQueryGenerator.generateDeleteQuery(params, "entities");
    databaseOperations.executeUpdate(query);
  }

  @Override
  public void deleteFromGrantRecords(PolarisCallContext callCtx, PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec);
    String query = JdbcCrudQueryGenerator.generateDeleteQuery(modelGrantRecord, "entities");
    databaseOperations.executeUpdate(query);
  }

  @Override
  public void deleteAllEntityGrantRecords(
      PolarisCallContext callCtx,
      PolarisEntityCore entity,
      List<PolarisGrantRecord> grantsOnGrantee,
      List<PolarisGrantRecord> grantsOnSecurable) {
    // get catalog id and securable id
    // get entity id and securable id
    databaseOperations.executeUpdate(
        "DELETE FROM grant_records WHERE (granteeID = ? AND granteeCatalogId = ?) OR (securableID = ? AND securableCatalogId = ?) ");
  }

  @Override
  public void deleteAll(PolarisCallContext callCtx) {

    databaseOperations.executeUpdate("DROP table grant_records");
    databaseOperations.executeUpdate("DROP table entity_grant_records");
    // all the tables we need

  }

  @Override
  public PolarisBaseEntity lookupEntity(
      PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    Map<String, Object> params = new HashMap<>();
    params.put("catalog_id", catalogId);
    params.put("entity_id", entityId);
    params.put("type_code", typeCode);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(ModelEntity.class, params, null, null, null);
    return getPolarisBaseEntity(query);
  }

  @Override
  public PolarisBaseEntity lookupEntityByName(
      PolarisCallContext callCtx, long catalogId, long parentId, int typeCode, String name) {
    Map<String, Object> params = new HashMap<>();
    params.put("catalog_id", catalogId);
    params.put("parent_id", parentId);
    params.put("type_code", typeCode);
    params.put("name", name);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(ModelEntity.class, params, 1, null, null);
    return getPolarisBaseEntity(query);
  }

  @Nullable
  private PolarisBaseEntity getPolarisBaseEntity(String query) {
    ResultSet result = databaseOperations.executeSelect(query);
    List<ModelEntity> results;
    try {
      results = ResultSetToObjectConverter.convert(result, ModelEntity.class);
    } catch (Exception e) {
      results = null;
    }
    return results == null ? null : ModelEntity.toEntity(results.get(0));
  }

  @Override
  public List<PolarisBaseEntity> lookupEntities(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    StringBuilder condition = new StringBuilder("(catalog_id, id) IN (");
    for (int i = 0; i < entityIds.size(); i++) {
      String in = "(" + entityIds.get(i).getId() + ", " + entityIds.get(i).getId() + ")";
      condition.append(in);
      condition.append(",");
    }
    // extra , removed
    condition.deleteCharAt(condition.length() - 1);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelEntity.class, String.valueOf(condition), 1, null, null);
    ResultSet result = databaseOperations.executeSelect(query);
    List<ModelEntity> results;
    try {
      results = ResultSetToObjectConverter.convert(result, ModelEntity.class);
    } catch (Exception e) {
      results = null;
    }
    return results == null
        ? Collections.emptyList()
        : results.stream().map(ModelEntity::toEntity).collect(Collectors.toList());
  }

  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return lookupEntities(callCtx, entityIds).stream()
        .map(
            entity ->
                new PolarisChangeTrackingVersions(
                    entity.getEntityVersion(), entity.getGrantRecordsVersion()))
        .toList();
  }

  @Nonnull
  @Override
  public List<EntityNameLookupRecord> listEntities(
      PolarisCallContext callCtx, long catalogId, long parentId, PolarisEntityType entityType) {
    return listEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        Integer.MAX_VALUE,
        e -> true,
        EntityNameLookupRecord::new);
  }

  @Nonnull
  @Override
  public List<EntityNameLookupRecord> listEntities(
      PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      Predicate<PolarisBaseEntity> entityFilter) {
    return listEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        Integer.MAX_VALUE,
        entityFilter,
        EntityNameLookupRecord::new);
  }

  @Nonnull
  @Override
  public <T> List<T> listEntities(
      PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      int limit,
      Predicate<PolarisBaseEntity> entityFilter,
      Function<PolarisBaseEntity, T> transformer) {
    Map<String, Object> params = new HashMap<>();
    params.put("catalog_id", catalogId);
    params.put("parent_id", parentId);
    params.put("entity_type", entityType.getCode());
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            EntityNameLookupRecord.class, params, limit, null, null);
    ResultSet result = databaseOperations.executeSelect(query);
    List<ModelEntity> results;
    try {
      results = ResultSetToObjectConverter.convert(result, ModelEntity.class);
    } catch (Exception e) {
      results = null;
    }
    return results == null
        ? Collections.emptyList()
        : results.stream()
            .map(ModelEntity::toEntity)
            .filter(entityFilter)
            .map(transformer)
            .collect(Collectors.toList());
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      PolarisCallContext callCtx, long catalogId, long entityId) {
    String condition =
        String.format(
            "(grantee_catalog_id = %s AND grantee_id = %s) OR (securable_catalog_id = %s AND securable_id = %s) ",
            catalogId, entityId, catalogId, entityId);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(ModelEntity.class, condition, null, null, null);
    return getPolarisBaseEntity(query).getGrantRecordsVersion();
  }

  @Override
  public PolarisGrantRecord lookupGrantRecord(
      PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    Map<String, Object> params = new HashMap<>();
    params.put("securable_catalog_id", securableCatalogId);
    params.put("securable_id", securableId);
    params.put("grantee_catalog_id", granteeCatalogId);
    params.put("grantee_id", granteeId);
    params.put("priviledge_code", privilegeCode);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelGrantRecord.class, params, null, null, null);
    ResultSet result = databaseOperations.executeSelect(query);
    List<ModelGrantRecord> results;
    try {
      results = ResultSetToObjectConverter.convert(result, ModelGrantRecord.class);
    } catch (Exception e) {
      results = null;
    }
    return results == null ? null : ModelGrantRecord.toGrantRecord(results.get(0));
  }

  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    Map<String, Object> params = new HashMap<>();
    params.put("securable_catalog_id", securableCatalogId);
    params.put("securable_id", securableId);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelGrantRecord.class, params, null, null, null);
    ResultSet result = databaseOperations.executeSelect(query);
    List<ModelGrantRecord> results;
    try {
      results = ResultSetToObjectConverter.convert(result, ModelGrantRecord.class);
    } catch (Exception e) {
      results = null;
    }
    return results == null
        ? null
        : results.stream().map(ModelGrantRecord::toGrantRecord).collect(Collectors.toList());
  }

  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    Map<String, Object> params = new HashMap<>();
    params.put("grantee_catalog_id", granteeCatalogId);
    params.put("grantee_id", granteeId);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelGrantRecord.class, params, null, null, null);
    ResultSet result = databaseOperations.executeSelect(query);
    List<ModelGrantRecord> results;
    try {
      results = ResultSetToObjectConverter.convert(result, ModelGrantRecord.class);
    } catch (Exception e) {
      results = null;
    }
    return results == null
        ? null
        : results.stream().map(ModelGrantRecord::toGrantRecord).collect(Collectors.toList());
  }

  @Override
  public boolean hasChildren(
      PolarisCallContext callContext,
      PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    Map<String, Object> params = new HashMap<>();
    params.put("catalog_id", catalogId);
    params.put("parent_id", parentId);
    if (optionalEntityType != null) {
      params.put("entity_type", optionalEntityType.getCode());
    }
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelGrantRecord.class, params, null, null, null);
    ResultSet result = databaseOperations.executeSelect(query);
    List<ModelGrantRecord> results;
    try {
      results = ResultSetToObjectConverter.convert(result, ModelGrantRecord.class);
    } catch (Exception e) {
      results = null;
    }

    return result != null && results.size() > 0;
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    Map<String, Object> params = new HashMap<>();
    params.put("client_id", clientId);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelPrincipalSecrets.class, params, null, null, null);
    ResultSet result = databaseOperations.executeSelect(query);
    List<ModelPrincipalSecrets> results;
    try {
      results = ResultSetToObjectConverter.convert(result, ModelPrincipalSecrets.class);
    } catch (Exception e) {
      results = null;
    }
    return results == null
        ? null
        : results.stream().map(ModelPrincipalSecrets::toPrincipalSecrets).toList().get(0);
  }

  @Nonnull
  @Override
  public PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId) {
    // ensure principal client id is unique
    PolarisPrincipalSecrets principalSecrets;
    ModelPrincipalSecrets lookupPrincipalSecrets;
    do {
      // generate new random client id and secrets
      principalSecrets = secretsGenerator.produceSecrets(principalName, principalId);

      // load the existing secrets
      lookupPrincipalSecrets =
          ModelPrincipalSecrets.fromPrincipalSecrets(
              loadPrincipalSecrets(callCtx, principalSecrets.getPrincipalClientId()));
    } while (lookupPrincipalSecrets != null);

    // write new principal secrets
    String query =
        JdbcCrudQueryGenerator.generateInsertQuery(principalSecrets, "principal_secrets");
    databaseOperations.executeUpdate(query);

    // if not found, return null
    return principalSecrets;
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    // load the existing secrets
    PolarisPrincipalSecrets principalSecrets = loadPrincipalSecrets(callCtx, clientId);

    // should be found
    callCtx
        .getDiagServices()
        .checkNotNull(
            principalSecrets,
            "cannot_find_secrets",
            "client_id={} principalId={}",
            clientId,
            principalId);

    // ensure principal id is matching
    callCtx
        .getDiagServices()
        .check(
            principalId == principalSecrets.getPrincipalId(),
            "principal_id_mismatch",
            "expectedId={} id={}",
            principalId,
            principalSecrets.getPrincipalId());

    // rotate the secrets
    principalSecrets.rotateSecrets(oldSecretHash);
    if (reset) {
      principalSecrets.rotateSecrets(principalSecrets.getMainSecretHash());
    }

    // write back new secrets
    // write new principal secrets
    String query =
        JdbcCrudQueryGenerator.generateInsertQuery(principalSecrets, "principal_secrets");
    databaseOperations.executeUpdate(query);

    // return those
    return principalSecrets;
  }

  @Override
  public void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    Map<String, Object> params = new HashMap<>();
    params.put("client_id", clientId);
    params.put("principal_id", principalId);
    String query = JdbcCrudQueryGenerator.generateDeleteQuery(params, "principal_secrets");
    databaseOperations.executeUpdate(query);
  }

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          @Nonnull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return storageIntegrationProvider.getStorageIntegrationForConfig(
        polarisStorageConfigurationInfo);
  }

  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @Nonnull PolarisCallContext callContext,
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration) {}

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @Nonnull PolarisCallContext callContext, @Nonnull PolarisBaseEntity entity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(callContext, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }
}
