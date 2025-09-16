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
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ScanPlanningServiceTest {

  @Mock private Table mockTable;
  @Mock private TableScan mockTableScan;
  @Mock private FileScanTask mockFileScanTask;
  @Mock private CloseableIterable<FileScanTask> mockScanTasks;

  private ScanPlanningService scanPlanningService;
  private TableIdentifier tableIdentifier;

  @BeforeEach
  void setUp() {
    scanPlanningService = new ScanPlanningService();
    tableIdentifier = TableIdentifier.of("test_namespace", "test_table");
  }

  @Test
  void testCreateScanPlan_Success() {
    // Setup
    PlanTableScanRequest request = PlanTableScanRequest.builder().build();
    
    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.planFiles()).thenReturn(mockScanTasks);
    when(mockScanTasks.iterator()).thenReturn(List.of(mockFileScanTask).iterator());
    when(mockFileScanTask.deletes()).thenReturn(Collections.emptyList());

    // Execute
    ScanPlanningService.ScanPlan result = 
        scanPlanningService.createScanPlan(mockTable, tableIdentifier, request);

    // Verify
    assertThat(result).isNotNull();
    assertThat(result.getTableIdentifier()).isEqualTo(tableIdentifier);
    assertThat(result.getStatus()).isEqualTo(ScanPlanningService.PlanStatus.COMPLETED);
    assertThat(result.getFileScanTasks()).hasSize(1);
    assertThat(result.getFileScanTasks()).contains(mockFileScanTask);
    assertThat(result.getPlanId()).isNotNull();
  }

  @Test
  void testCreateScanPlan_WithSnapshotId() {
    // Setup
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withSnapshotId(12345L)
        .build();
    
    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.useSnapshot(12345L)).thenReturn(mockTableScan);
    when(mockTableScan.planFiles()).thenReturn(mockScanTasks);
    when(mockScanTasks.iterator()).thenReturn(Collections.emptyIterator());

    // Execute
    ScanPlanningService.ScanPlan result = 
        scanPlanningService.createScanPlan(mockTable, tableIdentifier, request);

    // Verify
    assertThat(result.getStatus()).isEqualTo(ScanPlanningService.PlanStatus.COMPLETED);
    verify(mockTableScan).useSnapshot(12345L);
  }

  @Test
  void testCreateScanPlan_WithIncrementalScan() {
    // Setup
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withStartSnapshotId(100L)
        .withEndSnapshotId(200L)
        .build();
    
    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.appendsBetween(100L, 200L)).thenReturn(mockTableScan);
    when(mockTableScan.planFiles()).thenReturn(mockScanTasks);
    when(mockScanTasks.iterator()).thenReturn(Collections.emptyIterator());

    // Execute
    ScanPlanningService.ScanPlan result = 
        scanPlanningService.createScanPlan(mockTable, tableIdentifier, request);

    // Verify
    assertThat(result.getStatus()).isEqualTo(ScanPlanningService.PlanStatus.COMPLETED);
    verify(mockTableScan).appendsBetween(100L, 200L);
  }

  @Test
  void testCreateScanPlan_WithColumnSelection() {
    // Setup
    List<String> columns = List.of("column1", "column2");
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withSelect(columns)
        .build();
    
    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.select(columns)).thenReturn(mockTableScan);
    when(mockTableScan.planFiles()).thenReturn(mockScanTasks);
    when(mockScanTasks.iterator()).thenReturn(Collections.emptyIterator());

    // Execute
    ScanPlanningService.ScanPlan result = 
        scanPlanningService.createScanPlan(mockTable, tableIdentifier, request);

    // Verify
    assertThat(result.getStatus()).isEqualTo(ScanPlanningService.PlanStatus.COMPLETED);
    verify(mockTableScan).select(columns);
  }

  @Test
  void testValidateScanRequest_InvalidPointInTimeAndIncremental() {
    // Setup
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withSnapshotId(123L)
        .withStartSnapshotId(100L)
        .withEndSnapshotId(200L)
        .build();

    // Execute & Verify
    assertThatThrownBy(() -> 
        scanPlanningService.createScanPlan(mockTable, tableIdentifier, request))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Cannot specify both point-in-time");
  }

  @Test
  void testValidateScanRequest_MissingEndSnapshot() {
    // Setup
    PlanTableScanRequest request = PlanTableScanRequest.builder()
        .withStartSnapshotId(100L)
        .build();

    // Execute & Verify
    assertThatThrownBy(() -> 
        scanPlanningService.createScanPlan(mockTable, tableIdentifier, request))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("End snapshot ID is required");
  }

  @Test
  void testGetScanPlan() {
    // Setup
    PlanTableScanRequest request = PlanTableScanRequest.builder().build();
    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.planFiles()).thenReturn(mockScanTasks);
    when(mockScanTasks.iterator()).thenReturn(Collections.emptyIterator());

    // Execute
    ScanPlanningService.ScanPlan createdPlan = 
        scanPlanningService.createScanPlan(mockTable, tableIdentifier, request);
    Optional<ScanPlanningService.ScanPlan> retrievedPlan = 
        scanPlanningService.getScanPlan(createdPlan.getPlanId());

    // Verify
    assertThat(retrievedPlan).isPresent();
    assertThat(retrievedPlan.get()).isEqualTo(createdPlan);
  }

  @Test
  void testGetScanPlan_NotFound() {
    // Execute
    Optional<ScanPlanningService.ScanPlan> result = 
        scanPlanningService.getScanPlan("non-existent-plan-id");

    // Verify
    assertThat(result).isEmpty();
  }

  @Test
  void testCancelScanPlan() {
    // Setup
    PlanTableScanRequest request = PlanTableScanRequest.builder().build();
    when(mockTable.newScan()).thenReturn(mockTableScan);
    when(mockTableScan.planFiles()).thenReturn(mockScanTasks);
    when(mockScanTasks.iterator()).thenReturn(Collections.emptyIterator());

    // Execute
    ScanPlanningService.ScanPlan createdPlan = 
        scanPlanningService.createScanPlan(mockTable, tableIdentifier, request);
    boolean cancelled = scanPlanningService.cancelScanPlan(createdPlan.getPlanId());

    // Verify
    assertThat(cancelled).isTrue();
    assertThat(createdPlan.getStatus()).isEqualTo(ScanPlanningService.PlanStatus.CANCELLED);
    assertThat(scanPlanningService.getScanPlan(createdPlan.getPlanId())).isEmpty();
  }

  @Test
  void testCancelScanPlan_NotFound() {
    // Execute
    boolean cancelled = scanPlanningService.cancelScanPlan("non-existent-plan-id");

    // Verify
    assertThat(cancelled).isFalse();
  }

  @Test
  void testFetchScanTasks() {
    // Execute
    List<FileScanTask> result = scanPlanningService.fetchScanTasks("some-plan-task-id");

    // Verify - for now, this returns empty list as implementation is simplified
    assertThat(result).isEmpty();
  }
}