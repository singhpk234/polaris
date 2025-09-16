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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
// import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.test.PolarisApplicationIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@QuarkusTest
@TestProfile(ScanPlanningIntegrationTest.Profile.class)
public class ScanPlanningIntegrationTest extends PolarisApplicationIntegrationTest {

  @Inject
  private ObjectMapper objectMapper;

  private static final String CATALOG_NAME = "scan_planning_catalog";
  private static final String CATALOG_ROLE = "scan_planning_role";
  private static final String PRINCIPAL_ROLE = "scan_planning_principal_role";
  private static final Namespace NAMESPACE = Namespace.of("scan_planning_ns");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "scan_table");

  @TempDir
  private Path tempDir;

  private RESTSessionCatalog catalog;
  private PolarisClient polarisClient;

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.realm-context.realms", "POLARIS",
          "polaris.features.\"ALLOW_OVERLAPPING_CATALOG_URLS\"", "true",
          "polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "true",
          "polaris.features.\"ALLOW_SPECIFYING_FILE_IO_IMPL\"", "true",
          "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true",
          "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\"]",
          "polaris.readiness.ignore-severe-issues", "true");
    }
  }

  @BeforeEach
  void setUp(PolarisApiEndpoints endpoints, ClientPrincipal clientPrincipal) throws IOException {
    polarisClient = PolarisClient.polarisClient(endpoints, clientPrincipal);
    
    // Create catalog
    String catalogLocation = "file://" + tempDir.toAbsolutePath();
    PolarisCatalog polarisCatalog = PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(CATALOG_NAME)
        .setProperties(Map.of("warehouse-location", catalogLocation))
        .setStorageConfigInfo(
            FileStorageConfigInfo.builder()
                .setStorageType(FileStorageConfigInfo.StorageTypeEnum.FILE)
                .setAllowedLocations(List.of("file://"))
                .build())
        .build();
    
    polarisClient.createCatalog(polarisCatalog);

    // Create catalog role
    CatalogRole catalogRole = CatalogRole.builder()
        .setName(CATALOG_ROLE)
        .build();
    polarisClient.createCatalogRole(CATALOG_NAME, catalogRole);

    // Create principal role  
    PrincipalRole principalRole = PrincipalRole.builder()
        .setName(PRINCIPAL_ROLE)
        .build();
    polarisClient.createPrincipalRole(principalRole);

    // Grant privileges
    polarisClient.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE, "CATALOG_MANAGE_CONTENT");
    polarisClient.grantPrivilegeOnNamespaceToRole(CATALOG_NAME, NAMESPACE, CATALOG_ROLE, "NAMESPACE_FULL_METADATA");
    polarisClient.grantPrivilegeOnTableToRole(CATALOG_NAME, TABLE_IDENTIFIER, CATALOG_ROLE, "TABLE_FULL_METADATA");
    polarisClient.assignCatalogRoleToPrincipalRole(CATALOG_NAME, CATALOG_ROLE, PRINCIPAL_ROLE);
    polarisClient.assignPrincipalRole(clientPrincipal.getName(), PRINCIPAL_ROLE);

    // Initialize REST catalog
    catalog = new RESTSessionCatalog();
    catalog.initialize(CATALOG_NAME, Map.of(
        "uri", endpoints.catalogApiEndpoint() + "/" + CATALOG_NAME,
        "credential", clientPrincipal.getName() + ":" + clientPrincipal.getPassword(),
        "warehouse", catalogLocation,
        "scope", "PRINCIPAL_ROLE:ALL"
    ));

    // Create namespace and table for testing
    catalog.createNamespace(NAMESPACE);
    
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "age", Types.IntegerType.get())
    );
    
    Table table = catalog.createTable(TABLE_IDENTIFIER, schema);
    
    // Add some test data files to make scan planning meaningful
    DataFile dataFile1 = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("/test/data/file1.parquet")
        .withFileSizeInBytes(1024)
        .withRecordCount(100)
        .build();
        
    DataFile dataFile2 = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("/test/data/file2.parquet")
        .withFileSizeInBytes(2048)
        .withRecordCount(200)
        .build();

    table.newAppend()
        .appendFile(dataFile1)
        .appendFile(dataFile2)
        .commit();
  }

  @Test
  void testPlanTableScan_BasicScan() {
    // Prepare request
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .build();

    // Execute scan planning
    Response response = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    assertThat(response.getStatus()).isEqualTo(200);
    
    PlanTableScanResponse planResponse = response.readEntity(PlanTableScanResponse.class);
    assertThat(planResponse.getStatus()).isEqualTo("completed");
    assertThat(planResponse.getPlanId()).isNotNull();
    assertThat(planResponse.getFileScanTasks()).isNotEmpty();
  }

  @Test
  void testPlanTableScan_WithSnapshotId() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    long currentSnapshotId = table.currentSnapshot().snapshotId();

    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withSnapshotId(currentSnapshotId)
        .build();

    Response response = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    assertThat(response.getStatus()).isEqualTo(200);
    
    PlanTableScanResponse planResponse = response.readEntity(PlanTableScanResponse.class);
    assertThat(planResponse.getStatus()).isEqualTo("completed");
    assertThat(planResponse.getFileScanTasks()).isNotEmpty();
  }

  @Test
  void testPlanTableScan_WithColumnSelection() {
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withSelect(List.of("id", "name"))
        .build();

    Response response = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    assertThat(response.getStatus()).isEqualTo(200);
    
    PlanTableScanResponse planResponse = response.readEntity(PlanTableScanResponse.class);
    assertThat(planResponse.getStatus()).isEqualTo("completed");
    assertThat(planResponse.getFileScanTasks()).isNotEmpty();
  }

  @Test
  void testPlanTableScan_WithCaseSensitivity() {
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withCaseSensitive(true)
        .build();

    Response response = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    assertThat(response.getStatus()).isEqualTo(200);
    
    PlanTableScanResponse planResponse = response.readEntity(PlanTableScanResponse.class);
    assertThat(planResponse.getStatus()).isEqualTo("completed");
  }

  @Test
  void testPlanTableScan_InvalidRequest_ConflictingSnapshots() {
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withSnapshotId(123L)
        .withStartSnapshotId(100L)
        .withEndSnapshotId(200L)
        .build();

    Response response = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void testFetchPlanningResult_CompletedPlan() {
    // First create a plan
    PlanTableScanRequest request = PlanTableScanRequest.builder().build();

    Response planResponse = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    PlanTableScanResponse planResult = planResponse.readEntity(PlanTableScanResponse.class);
    String planId = planResult.getPlanId();

    // Fetch planning result
    Response fetchResponse = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .path(planId)
        .request(MediaType.APPLICATION_JSON)
        .get();

    assertThat(fetchResponse.getStatus()).isEqualTo(200);
    
    FetchPlanningResultResponse fetchResult = fetchResponse.readEntity(FetchPlanningResultResponse.class);
    assertThat(fetchResult.getStatus()).isEqualTo("completed");
    assertThat(fetchResult.getFileScanTasks()).isNotEmpty();
  }

  @Test
  void testFetchPlanningResult_NonExistentPlan() {
    String nonExistentPlanId = UUID.randomUUID().toString();

    Response fetchResponse = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .path(nonExistentPlanId)
        .request(MediaType.APPLICATION_JSON)
        .get();

    assertThat(fetchResponse.getStatus()).isEqualTo(404);
  }

  @Test
  void testCancelPlanning() {
    // First create a plan
    PlanTableScanRequest request = PlanTableScanRequest.builder().build();

    Response planResponse = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    PlanTableScanResponse planResult = planResponse.readEntity(PlanTableScanResponse.class);
    String planId = planResult.getPlanId();

    // Cancel the plan
    Response cancelResponse = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .path(planId)
        .request(MediaType.APPLICATION_JSON)
        .delete();

    assertThat(cancelResponse.getStatus()).isEqualTo(204);

    // Verify the plan is cancelled
    Response fetchResponse = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .path(planId)
        .request(MediaType.APPLICATION_JSON)
        .get();

    FetchPlanningResultResponse fetchResult = fetchResponse.readEntity(FetchPlanningResultResponse.class);
    assertThat(fetchResult.getStatus()).isEqualTo("cancelled");
  }

  @Test
  void testCancelPlanning_NonExistentPlan() {
    String nonExistentPlanId = UUID.randomUUID().toString();

    // Cancel should succeed even for non-existent plans (per API spec)
    Response cancelResponse = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .path(nonExistentPlanId)
        .request(MediaType.APPLICATION_JSON)
        .delete();

    assertThat(cancelResponse.getStatus()).isEqualTo(204);
  }

  @Test
  void testFetchScanTasks() {
    FetchScanTasksRequest request = FetchScanTasksRequest.builder()
        .withPlanTask("test-plan-task-id")
        .build();

    Response response = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("tasks")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    assertThat(response.getStatus()).isEqualTo(200);
    
    FetchScanTasksResponse result = response.readEntity(FetchScanTasksResponse.class);
    assertThat(result.getFileScanTasks()).isEmpty(); // Empty for this simplified implementation
    assertThat(result.getPlanTasks()).isEmpty();
    assertThat(result.getDeleteFiles()).isEmpty();
  }

  @Test  
  void testFetchScanTasks_InvalidPlanTask() {
    FetchScanTasksRequest request = FetchScanTasksRequest.builder()
        .withPlanTask("non-existent-plan-task")
        .build();

    Response response = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("tasks")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    assertThat(response.getStatus()).isEqualTo(404);
  }

  @Test
  void testScanPlanningWithIncrementalScan() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    
    // Create another snapshot
    DataFile dataFile3 = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("/test/data/file3.parquet")
        .withFileSizeInBytes(1536)
        .withRecordCount(150)
        .build();
    
    table.newAppend().appendFile(dataFile3).commit();
    
    long startSnapshotId = table.history().get(0).snapshotId();
    long endSnapshotId = table.currentSnapshot().snapshotId();

    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withStartSnapshotId(startSnapshotId)
        .withEndSnapshotId(endSnapshotId)
        .build();

    Response response = polarisClient.getRestClient()
        .target(polarisClient.getEndpoints().catalogApiEndpoint())
        .path(CATALOG_NAME)
        .path("v1")
        .path("namespaces")
        .path(NAMESPACE.level(0))
        .path("tables")
        .path(TABLE_IDENTIFIER.name())
        .path("plan")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(request));

    assertThat(response.getStatus()).isEqualTo(200);
    
    PlanTableScanResponse planResponse = response.readEntity(PlanTableScanResponse.class);
    assertThat(planResponse.getStatus()).isEqualTo("completed");
    assertThat(planResponse.getFileScanTasks()).isNotEmpty();
  }
}