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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for managing scan planning operations, including plan lifecycle management,
 * task generation, and state persistence.
 */
@ApplicationScoped
public class ScanPlanningService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScanPlanningService.class);

  private final Map<String, ScanPlan> activePlans = new ConcurrentHashMap<>();
  private final Map<String, List<String>> planTasks = new ConcurrentHashMap<>();

  /**
   * Represents a scan plan with its current state and metadata.
   */
  public static class ScanPlan {
    private final String planId;
    private final TableIdentifier tableIdentifier;
    private final PlanTableScanRequest request;
    private final OffsetDateTime createdAt;
    private PlanStatus status;
    private List<FileScanTask> fileScanTasks;
    private List<String> planTaskIds;
    private List<DeleteFile> deleteFiles;
    private Optional<String> errorMessage;

    public ScanPlan(String planId, TableIdentifier tableIdentifier, PlanTableScanRequest request) {
      this.planId = planId;
      this.tableIdentifier = tableIdentifier;
      this.request = request;
      this.createdAt = OffsetDateTime.now(ZoneOffset.UTC);
      this.status = PlanStatus.SUBMITTED;
      this.fileScanTasks = new ArrayList<>();
      this.planTaskIds = new ArrayList<>();
      this.deleteFiles = new ArrayList<>();
      this.errorMessage = Optional.empty();
    }

    public String getPlanId() { return planId; }
    public TableIdentifier getTableIdentifier() { return tableIdentifier; }
    public PlanTableScanRequest getRequest() { return request; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public PlanStatus getStatus() { return status; }
    public void setStatus(PlanStatus status) { this.status = status; }
    public List<FileScanTask> getFileScanTasks() { return fileScanTasks; }
    public void setFileScanTasks(List<FileScanTask> fileScanTasks) { this.fileScanTasks = fileScanTasks; }
    public List<String> getPlanTaskIds() { return planTaskIds; }
    public void setPlanTaskIds(List<String> planTaskIds) { this.planTaskIds = planTaskIds; }
    public List<DeleteFile> getDeleteFiles() { return deleteFiles; }
    public void setDeleteFiles(List<DeleteFile> deleteFiles) { this.deleteFiles = deleteFiles; }
    public Optional<String> getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = Optional.ofNullable(errorMessage); }
  }

  /**
   * Status of a scan planning operation.
   */
  public enum PlanStatus {
    SUBMITTED, COMPLETED, CANCELLED, FAILED
  }

  /**
   * Creates a new scan plan for the specified table and request.
   */
  public ScanPlan createScanPlan(Table table, TableIdentifier tableIdentifier, PlanTableScanRequest request) {
    String planId = UUID.randomUUID().toString();
    ScanPlan scanPlan = new ScanPlan(planId, tableIdentifier, request);
    
    try {
      // Validate the request
      validateScanRequest(request);
      
      // Create table scan based on request parameters
      TableScan tableScan = createTableScan(table, request);
      
      // Execute planning
      List<FileScanTask> tasks = new ArrayList<>();
      List<DeleteFile> deleteFiles = new ArrayList<>();
      
      try (CloseableIterable<FileScanTask> scanTasks = tableScan.planFiles()) {
        for (FileScanTask task : scanTasks) {
          tasks.add(task);
          // Collect delete files from scan tasks
          deleteFiles.addAll(task.deletes());
        }
      }
      
      scanPlan.setFileScanTasks(tasks);
      scanPlan.setDeleteFiles(deleteFiles.stream().distinct().collect(Collectors.toList()));
      
      // For simplicity, complete immediately. In a real implementation,
      // this could be async with plan tasks for large result sets
      scanPlan.setStatus(PlanStatus.COMPLETED);
      
      LOGGER.info("Created scan plan {} for table {} with {} file scan tasks", 
          planId, tableIdentifier, tasks.size());
      
    } catch (Exception e) {
      LOGGER.error("Failed to create scan plan for table {}", tableIdentifier, e);
      scanPlan.setStatus(PlanStatus.FAILED);
      scanPlan.setErrorMessage(e.getMessage());
    }
    
    activePlans.put(planId, scanPlan);
    return scanPlan;
  }

  /**
   * Retrieves an existing scan plan by its ID.
   */
  public Optional<ScanPlan> getScanPlan(String planId) {
    return Optional.ofNullable(activePlans.get(planId));
  }

  /**
   * Cancels a scan plan and removes it from active plans.
   */
  public boolean cancelScanPlan(String planId) {
    ScanPlan plan = activePlans.get(planId);
    if (plan != null) {
      plan.setStatus(PlanStatus.CANCELLED);
      activePlans.remove(planId);
      planTasks.remove(planId);
      LOGGER.info("Cancelled scan plan {}", planId);
      return true;
    }
    return false;
  }

  /**
   * Fetches scan tasks for a specific plan task ID.
   */
  public List<FileScanTask> fetchScanTasks(String planTaskId) {
    // For now, return empty list since we're completing plans immediately
    // In a real implementation, this would fetch tasks associated with the plan task
    return Collections.emptyList();
  }

  private void validateScanRequest(PlanTableScanRequest request) {
    // Validate that we don't have both point-in-time and incremental scan properties
    boolean hasSnapshotId = request.snapshotId() != null;
    boolean hasStartSnapshot = request.startSnapshotId() != null;
    boolean hasEndSnapshot = request.endSnapshotId() != null;
    
    if (hasSnapshotId && (hasStartSnapshot || hasEndSnapshot)) {
      throw new IllegalArgumentException(
          "Cannot specify both point-in-time (snapshot-id) and incremental scan properties");
    }
    
    if (hasStartSnapshot && !hasEndSnapshot) {
      throw new IllegalArgumentException(
          "End snapshot ID is required when start snapshot ID is specified");
    }
    
    if (hasEndSnapshot && !hasStartSnapshot) {
      throw new IllegalArgumentException(
          "Start snapshot ID is required when end snapshot ID is specified");
    }
  }

  private TableScan createTableScan(Table table, PlanTableScanRequest request) {
    TableScan scan = table.newScan();
    
    // Apply snapshot-based filtering
    if (request.snapshotId() != null) {
      scan = scan.useSnapshot(request.snapshotId());
    } else if (request.startSnapshotId() != null && request.endSnapshotId() != null) {
      scan = scan.appendsBetween(request.startSnapshotId(), request.endSnapshotId());
    }
    
    // Apply column selection
    if (request.select() != null && !request.select().isEmpty()) {
      scan = scan.select(request.select());
    }
    
    // Apply case sensitivity
    if (request.caseSensitive() != null) {
      scan = scan.caseSensitive(request.caseSensitive());
    }
    
    return scan;
  }
}