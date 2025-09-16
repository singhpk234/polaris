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
package org.apache.polaris.service.catalog.iceberg.rest;

import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.catalog.iceberg.CatalogHandlerUtils;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandler;
import org.apache.polaris.service.catalog.iceberg.ScanPlanningService;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST resource for handling scan planning operations on Iceberg tables.
 * Implements the Iceberg REST API specification for scan planning.
 */
@Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ScanPlanningResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScanPlanningResource.class);

  @Inject
  private PolarisDiagnostics diagnostics;

  @Inject
  private CallContext callContext;

  @Inject
  private ResolutionManifestFactory resolutionManifestFactory;

  @Inject
  private PolarisMetaStoreManager metaStoreManager;

  @Inject
  private UserSecretsManager userSecretsManager;

  @Inject
  private CallContextCatalogFactory catalogFactory;

  @Inject
  private PolarisAuthorizer authorizer;

  @Inject
  private ReservedProperties reservedProperties;

  @Inject
  private CatalogHandlerUtils catalogHandlerUtils;

  @Inject
  private PolarisEventListener polarisEventListener;

  @Inject
  private ScanPlanningService scanPlanningService;

  @Inject
  private Instance<ExternalCatalogFactory> externalCatalogFactories;

  @Context
  private SecurityContext securityContext;

  /**
   * Submit a scan for planning.
   * 
   * @param prefix the catalog prefix
   * @param namespace the namespace name
   * @param table the table name
   * @param request the scan planning request
   * @return the scan planning response
   */
  @POST
  @Path("/plan")
  public Response planTableScan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      PlanTableScanRequest request) {
    
    try {
      TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), table);
      
      try (IcebergCatalogHandler handler = createHandler(prefix)) {
        PlanTableScanResponse response = handler.planTableScan(tableIdentifier, request);
        return Response.ok(response).build();
      }
      
    } catch (NoSuchTableException e) {
      LOGGER.debug("Table not found: {}.{}", namespace, table, e);
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", Map.of(
              "message", "Table does not exist: " + namespace + "." + table,
              "type", "NoSuchTableException",
              "code", 404)))
          .build();
    } catch (NoSuchNamespaceException e) {
      LOGGER.debug("Namespace not found: {}", namespace, e);
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", Map.of(
              "message", "Namespace does not exist: " + namespace,
              "type", "NoSuchNamespaceException",  
              "code", 404)))
          .build();
    } catch (IllegalArgumentException e) {
      LOGGER.debug("Invalid scan planning request", e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(Map.of("error", Map.of(
              "message", e.getMessage(),
              "type", "BadRequestException",
              "code", 400)))
          .build();
    } catch (Exception e) {
      LOGGER.error("Failed to plan table scan for {}.{}", namespace, table, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Map.of("error", Map.of(
              "message", "Internal server error: " + e.getMessage(),
              "type", "InternalServerErrorException",
              "code", 500)))
          .build();
    }
  }

  /**
   * Fetch the result of scan planning for a plan-id.
   * 
   * @param prefix the catalog prefix  
   * @param namespace the namespace name
   * @param table the table name
   * @param planId the plan ID to fetch results for
   * @return the planning result
   */
  @GET
  @Path("/plan/{planId}")
  public Response fetchPlanningResult(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    
    try {
      TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), table);
      
      try (IcebergCatalogHandler handler = createHandler(prefix)) {
        FetchPlanningResultResponse response = handler.fetchPlanningResult(tableIdentifier, planId);
        return Response.ok(response).build();
      }
      
    } catch (IcebergCatalogHandler.NoSuchPlanIdException e) {
      LOGGER.debug("Plan ID not found: {}", planId, e);
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", Map.of(
              "message", "The plan id does not exist",
              "type", "NoSuchPlanIdException",
              "code", 404)))
          .build();
    } catch (NoSuchTableException e) {
      LOGGER.debug("Table not found: {}.{}", namespace, table, e);
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", Map.of(
              "message", "Table does not exist: " + namespace + "." + table,
              "type", "NoSuchTableException",
              "code", 404)))
          .build();
    } catch (Exception e) {
      LOGGER.error("Failed to fetch planning result for plan {}", planId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Map.of("error", Map.of(
              "message", "Internal server error: " + e.getMessage(),
              "type", "InternalServerErrorException",
              "code", 500)))
          .build();
    }
  }

  /**
   * Cancel scan planning for a plan-id.
   * 
   * @param prefix the catalog prefix
   * @param namespace the namespace name 
   * @param table the table name
   * @param planId the plan ID to cancel
   * @return 204 No Content on success
   */
  @DELETE
  @Path("/plan/{planId}")
  public Response cancelPlanning(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    
    try {
      TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), table);
      
      try (IcebergCatalogHandler handler = createHandler(prefix)) {
        handler.cancelPlanning(tableIdentifier, planId);
        return Response.noContent().build();
      }
      
    } catch (Exception e) {
      LOGGER.error("Failed to cancel planning for plan {}", planId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Map.of("error", Map.of(
              "message", "Internal server error: " + e.getMessage(),
              "type", "InternalServerErrorException", 
              "code", 500)))
          .build();
    }
  }

  /**
   * Fetch result tasks for a plan task.
   * 
   * @param prefix the catalog prefix
   * @param namespace the namespace name
   * @param table the table name
   * @param request the fetch scan tasks request
   * @return the scan tasks response  
   */
  @POST
  @Path("/tasks")
  public Response fetchScanTasks(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      FetchScanTasksRequest request) {
    
    try {
      TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), table);
      
      try (IcebergCatalogHandler handler = createHandler(prefix)) {
        FetchScanTasksResponse response = handler.fetchScanTasks(tableIdentifier, request);
        return Response.ok(response).build();
      }
      
    } catch (IcebergCatalogHandler.NoSuchPlanTaskException e) {
      LOGGER.debug("Plan task not found: {}", request.planTask(), e);
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", Map.of(
              "message", "The plan task does not exist",
              "type", "NoSuchPlanTaskException",
              "code", 404)))
          .build();
    } catch (NoSuchTableException e) {
      LOGGER.debug("Table not found: {}.{}", namespace, table, e);
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", Map.of(
              "message", "Table does not exist: " + namespace + "." + table,
              "type", "NoSuchTableException",
              "code", 404)))
          .build();
    } catch (Exception e) {
      LOGGER.error("Failed to fetch scan tasks for plan task {}", request.planTask(), e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Map.of("error", Map.of(
              "message", "Internal server error: " + e.getMessage(),
              "type", "InternalServerErrorException",
              "code", 500)))
          .build();
    }
  }

  private IcebergCatalogHandler createHandler(String catalogName) {
    return new IcebergCatalogHandler(
        diagnostics,
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        userSecretsManager,
        securityContext,
        catalogFactory,
        catalogName,
        authorizer,
        reservedProperties,
        catalogHandlerUtils,
        externalCatalogFactories,
        polarisEventListener,
        scanPlanningService);
  }
}