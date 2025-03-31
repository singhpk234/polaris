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
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.persistence.*;
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
    return RandomIdGenerator.INSTANCE.nextId();
  }

  @Override
  public void writeEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      PolarisBaseEntity originalEntity) {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity);
    String query;
    if (originalEntity == null) {
      query = JdbcCrudQueryGenerator.generateInsertQuery(modelEntity, "polaris.entities");
    } else {
      Map<String, Object> params = new HashMap<>();
      params.put("id", originalEntity.getId());
      params.put("catalog_id", originalEntity.getCatalogId());
      params.put("entity_version", originalEntity.getEntityVersion());
      query = JdbcCrudQueryGenerator.generateUpdateQuery(modelEntity, params, "polaris.entities");
    }
    System.out.println("Executing query: " + query);
    int x = databaseOperations.executeUpdate(query);
    System.out.println("Generated id: " + x);
    if (x == 0) {
      if (originalEntity == null) {
        // bad interface.
        throw new EntityAlreadyExistsException(entity);
      } else {
        throw new RetryOnConcurrencyException("CAS failed");
      }
    }
  }

  @Override
  public void writeEntities(
      PolarisCallContext callCtx,
      List<PolarisBaseEntity> entities,
      List<PolarisBaseEntity> originalEntities) {
    try {
      databaseOperations.runWithinTransaction(
          statement -> {
            for (int i = 0; i < entities.size(); i++) {
              PolarisBaseEntity entity = entities.get(i);
              ModelEntity modelEntity = ModelEntity.fromEntity(entity);

              // first, check if the entity has already been created, in which case we will simply
              // return it
              PolarisBaseEntity entityFound =
                  lookupEntity(
                      callCtx, entity.getCatalogId(), entity.getId(), entity.getTypeCode());
              if (entityFound != null) {
                // probably the client retried, simply return it
                // TODO: Check correctness of returning entityFound vs entity here. It may have
                // already
                // been updated after the creation.
                continue;
              }
              // lookup by name
              EntityNameLookupRecord exists =
                  lookupEntityIdAndSubTypeByName(
                      callCtx,
                      entity.getCatalogId(),
                      entity.getParentId(),
                      entity.getTypeCode(),
                      entity.getName());
              if (exists != null) {
                throw new EntityAlreadyExistsException(entity);
              }
              String query;
              if (originalEntities == null || originalEntities.get(i) == null) {
                query = JdbcCrudQueryGenerator.generateInsertQuery(modelEntity, "polaris.entities");
              } else {
                // CAS
                Map<String, Object> params = new HashMap<>();
                params.put("id", originalEntities.get(i).getId());
                params.put("catalog_id", originalEntities.get(i).getCatalogId());
                params.put("entity_version", originalEntities.get(i).getEntityVersion());
                query =
                    JdbcCrudQueryGenerator.generateUpdateQuery(
                        modelEntity, params, "polaris.entities");
              }
              int x = databaseOperations.executeUpdate(query, statement);
              if (x == 0) {
                if (originalEntities == null || originalEntities.get(i) == null) {
                  // bad interface.
                  throw new EntityAlreadyExistsException(entity);
                } else {
                  throw new RetryOnConcurrencyException("CAS failed");
                }
              }
            }
            return true;
          });
    } catch (Exception e) {
      if (e instanceof EntityAlreadyExistsException) {
        throw (EntityAlreadyExistsException) e;
      }
    }
  }

  @Override
  public void writeToGrantRecords(PolarisCallContext callCtx, PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec);
    String query =
        JdbcCrudQueryGenerator.generateInsertQuery(modelGrantRecord, "polaris.GRANT_RECORDS");
    System.out.println("Executing query: " + query);
    databaseOperations.executeUpdate(query);
  }

  @Override
  public void deleteEntity(PolarisCallContext callCtx, PolarisBaseEntity entity) {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity);
    Map<String, Object> params = new HashMap<>();
    params.put("id", modelEntity.getId());
    params.put("catalog_id", modelEntity.getCatalogId());
    String query = JdbcCrudQueryGenerator.generateDeleteQuery(params, "polaris.entities");
    databaseOperations.executeUpdate(query);
  }

  @Override
  public void deleteFromGrantRecords(PolarisCallContext callCtx, PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec);
    String query =
        JdbcCrudQueryGenerator.generateDeleteQuery(modelGrantRecord, "polaris.grant_records");
    databaseOperations.executeUpdate(query);
  }

  @Override
  public void deleteAllEntityGrantRecords(
      PolarisCallContext callCtx,
      PolarisEntityCore entity,
      List<PolarisGrantRecord> grantsOnGrantee,
      List<PolarisGrantRecord> grantsOnSecurable) {
    // generate where clause
    StringBuilder condition = new StringBuilder("(grantee_id, grantee_catalog_id) IN (");
    condition.append("(" + entity.getId() + ", " + entity.getCatalogId() + ")");
    condition.append(",");
    // extra , removed
    condition.deleteCharAt(condition.length() - 1);
    condition.append(")");

    StringBuilder condition2 = new StringBuilder("(securable_catalog_id, securable_id) IN (");

    String in = "(" + entity.getCatalogId() + ", " + entity.getId() + ")";
    condition2.append(in);
    condition2.append(",");

    // extra , removed
    condition2.deleteCharAt(condition2.length() - 1);
    condition2.append(")");

    databaseOperations.executeUpdate(
        "DELETE FROM polaris.grant_records " + "WHERE " + condition + " OR " + condition2);
  }

  @Override
  public void deleteAll(PolarisCallContext callCtx) {

    databaseOperations.executeUpdate("DELETE FROM polaris.grant_records where (1 = 1)");
    databaseOperations.executeUpdate("DELETE FROM polaris.entities where (1 = 1)");
    databaseOperations.executeUpdate("DELETE FROM polaris.principal_secrets where (1 = 1)");
    // all the tables we need

  }

  @Override
  public PolarisBaseEntity lookupEntity(
      PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    Map<String, Object> params = new HashMap<>();
    params.put("catalog_id", catalogId);
    params.put("id", entityId);
    params.put("type_code", typeCode);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelEntity.class, params, null, null, "last_update_timestamp");
    System.out.println("Executing query: " + query);
    PolarisBaseEntity b = getPolarisBaseEntity(query);
    System.out.println("Generated entity: " + b);
    return b;
  }

  @Override
  public PolarisBaseEntity lookupEntityByName(
      PolarisCallContext callCtx, long catalogId, long parentId, int typeCode, String name) {
    Map<String, Object> params = new HashMap<>();
    params.put("catalog_id", catalogId);
    params.put("parent_id", parentId);
    params.put("type_code", typeCode);

    if (name != null) {
      params.put("name", name);
    }
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelEntity.class, params, 1, null, "last_update_timestamp");
    return getPolarisBaseEntity(query);
  }

  @Nullable
  private PolarisBaseEntity getPolarisBaseEntity(String query) {
    List<ModelEntity> results = databaseOperations.executeSelect(query, ModelEntity.class);
    return results == null || results.isEmpty() ? null : ModelEntity.toEntity(results.get(0));
  }

  @Nonnull
  @Override
  public List<PolarisBaseEntity> lookupEntities(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    if (entityIds == null || entityIds.isEmpty()) return new ArrayList<>();
    StringBuilder condition = new StringBuilder("(catalog_id, id) IN (");
    for (int i = 0; i < entityIds.size(); i++) {
      String in = "(" + entityIds.get(i).getCatalogId() + ", " + entityIds.get(i).getId() + ")";
      condition.append(in);
      condition.append(",");
    }
    // extra , removed
    condition.deleteCharAt(condition.length() - 1);
    condition.append(")");
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelEntity.class,
            entityIds.isEmpty() ? "" : String.valueOf(condition),
            null,
            null,
            null);
    List<ModelEntity> results = databaseOperations.executeSelect(query, ModelEntity.class);
    return results == null
        ? Collections.emptyList()
        : results.stream().map(ModelEntity::toEntity).collect(Collectors.toList());
  }

  @Nonnull
  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    Map<PolarisEntityId, ModelEntity> idToEntityMap =
        lookupEntities(callCtx, entityIds).stream()
            .collect(
                Collectors.toMap(
                    entry -> new PolarisEntityId(entry.getCatalogId(), entry.getId()),
                    ModelEntity::fromEntity));
    return entityIds.stream()
        .map(
            entityId -> {
              ModelEntity entity = idToEntityMap.getOrDefault(entityId, null);
              return entity == null
                  ? null
                  : new PolarisChangeTrackingVersions(
                      entity.getEntityVersion(), entity.getGrantRecordsVersion());
            })
        .collect(Collectors.toList());
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
    params.put("type_code", entityType.getCode());
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelEntity.class, params, limit, null, "last_update_timestamp");
    List<ModelEntity> results = databaseOperations.executeSelect(query, ModelEntity.class);
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

    Map<String, Object> params = new HashMap<>();
    params.put("catalog_id", catalogId);
    params.put("id", entityId);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelEntity.class, params, null, null, "last_update_timestamp");
    PolarisBaseEntity b = getPolarisBaseEntity(query);
    return b == null ? 0 : b.getGrantRecordsVersion();
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
    params.put("privilege_code", privilegeCode);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelGrantRecord.class, params, null, null, null);
    List<ModelGrantRecord> results =
        databaseOperations.executeSelect(query, ModelGrantRecord.class);
    return results == null ? null : ModelGrantRecord.toGrantRecord(results.get(0));
  }

  @Nonnull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    Map<String, Object> params = new HashMap<>();
    params.put("securable_catalog_id", securableCatalogId);
    params.put("securable_id", securableId);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelGrantRecord.class, params, null, null, null);
    List<ModelGrantRecord> results =
        databaseOperations.executeSelect(query, ModelGrantRecord.class);
    return results == null
        ? Collections.emptyList()
        : results.stream().map(ModelGrantRecord::toGrantRecord).collect(Collectors.toList());
  }

  @Nonnull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    Map<String, Object> params = new HashMap<>();
    params.put("grantee_catalog_id", granteeCatalogId);
    params.put("grantee_id", granteeId);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelGrantRecord.class, params, null, null, null);
    List<ModelGrantRecord> results =
        databaseOperations.executeSelect(query, ModelGrantRecord.class);
    return results == null
        ? Collections.emptyList()
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
            ModelEntity.class, params, null, null, "last_update_timestamp");
    List<ModelEntity> results = databaseOperations.executeSelect(query, ModelEntity.class);

    return results != null && !results.isEmpty();
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    Map<String, Object> params = new HashMap<>();
    params.put("principal_client_id", clientId);
    String query =
        JdbcCrudQueryGenerator.generateSelectQuery(
            ModelPrincipalSecrets.class, params, null, null, null);
    List<ModelPrincipalSecrets> results =
        databaseOperations.executeSelect(query, ModelPrincipalSecrets.class);
    return results == null || results.isEmpty()
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

    lookupPrincipalSecrets = ModelPrincipalSecrets.fromPrincipalSecrets(principalSecrets);

    // write new principal secrets
    String query =
        JdbcCrudQueryGenerator.generateInsertQuery(
            lookupPrincipalSecrets, "polaris.principal_secrets");
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

    Map<String, Object> params = new HashMap<>();
    params.put("principal_client_id", clientId);
    // write back new secrets
    // write new principal secrets
    String query =
        JdbcCrudQueryGenerator.generateUpdateQuery(
            ModelPrincipalSecrets.fromPrincipalSecrets(principalSecrets),
            params,
            "polaris.principal_secrets");
    databaseOperations.executeUpdate(query);

    // return those
    return principalSecrets;
  }

  @Override
  public void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    Map<String, Object> params = new HashMap<>();
    params.put("principal_client_id", clientId);
    params.put("principal_id", principalId);
    String query = JdbcCrudQueryGenerator.generateDeleteQuery(params, "polaris.principal_secrets");
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
