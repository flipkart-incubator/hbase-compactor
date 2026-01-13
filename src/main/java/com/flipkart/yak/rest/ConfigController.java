package com.flipkart.yak.rest;

import com.flipkart.yak.config.*;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import com.flipkart.yak.interfaces.Factory;
import com.flipkart.yak.commons.CompactionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Controller class for all REST APIs.
 * Takes in {@link Factory} as parameter, and uses to obtain DAOs to store/retrieve data in/from Store.
 */
@Slf4j
@Path("/manage")
public class ConfigController {
    private final AbstractConfigWriter abstractConfigWriter;
    private final AbstractConfigLoader abstractConfigLoader;
    private final Object storeResource;

    public ConfigController(Factory storeFactory, String storeResourceProperty) throws ConfigurationException {
        abstractConfigWriter = storeFactory.getWriter();
        abstractConfigLoader = storeFactory.getLoader();
        storeResource = abstractConfigWriter.init(storeResourceProperty);
    }

    @POST
    @Path("/context")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addContext(CompactionContext compactionContext) {
        try {
            boolean success = abstractConfigWriter.storeContext(storeResource, compactionContext);
            if (success) {
                log.info("Successfully added compaction context: clusterID={}, profileID={}",
                    compactionContext.getClusterID(), compactionContext.getCompactionProfileID());
                return Response.status(Response.Status.CREATED)
                    .entity(compactionContext)
                    .build();
            } else {
                log.warn("Failed to add compaction context: clusterID={}, profileID={}",
                    compactionContext.getClusterID(), compactionContext.getCompactionProfileID());
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to store compaction context")
                    .build();
            }
        } catch (Exception e) {
            log.error("Error adding compaction context: clusterID={}, profileID={}, error={}",
                compactionContext != null ? compactionContext.getClusterID() : "null",
                compactionContext != null ? compactionContext.getCompactionProfileID() : "null",
                e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Error adding compaction context: " + e.getMessage())
                .build();
        }
    }

    @DELETE
    @Path("/context")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeContext(CompactionContext compactionContext) {
        try {
            boolean success = abstractConfigWriter.deleteContext(storeResource, compactionContext);

            if (success) {
                log.info("Successfully removed compaction context: clusterID={}, profileID={}",
                    compactionContext.getClusterID(), compactionContext.getCompactionProfileID());
                return Response.status(Response.Status.OK)
                    .entity(true)
                    .build();
            } else {
                log.warn("Failed to remove compaction context: clusterID={}, profileID={}. Context may not exist.",
                    compactionContext.getClusterID(), compactionContext.getCompactionProfileID());
                return Response.status(Response.Status.NOT_FOUND)
                    .entity("Compaction context not found or could not be deleted")
                    .build();
            }
        } catch (Exception e) {
            log.error("Error removing compaction context: clusterID={}, profileID={}, error={}",
                compactionContext.getClusterID(), compactionContext.getCompactionProfileID(),
                e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Error removing compaction context: " + e.getMessage())
                .build();
        }
    }

    @POST
    @Path("/trigger")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerImmediateCompaction(PromptCompactionRequest promptCompactionRequest) {
        try {
            CompactionContext compactionContext = CompactionUtils.getCompactionContext(promptCompactionRequest);
            boolean success = abstractConfigWriter.storeContext(storeResource, compactionContext);

            if (success) {
                log.info("Successfully triggered immediate compaction: clusterID={}, profileID={}",
                    compactionContext.getClusterID(), compactionContext.getCompactionProfileID());
                return Response.status(Response.Status.CREATED)
                    .entity(compactionContext)
                    .build();
            } else {
                log.warn("Failed to trigger immediate compaction: clusterID={}, profileID={}",
                    compactionContext.getClusterID(), compactionContext.getCompactionProfileID());
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to trigger immediate compaction")
                    .build();
            }
        } catch (Exception e) {
            log.error("Error triggering immediate compaction: request={}, error={}",
                promptCompactionRequest, e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Error triggering immediate compaction: " + e.getMessage())
                .build();
        }
    }

    @DELETE
    @Path("/deleteAllStaleContexts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeStaleContexts() {
        try {
            boolean success = abstractConfigWriter.deleteAllStaleContexts(storeResource);

            if (success) {
                log.info("Successfully removed all stale compaction contexts");
                return Response.status(Response.Status.OK)
                    .entity(true)
                    .build();
            } else {
                log.warn("Failed to remove stale compaction contexts");
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to remove stale compaction contexts")
                    .build();
            }
        } catch (Exception e) {
            log.error("Error removing stale compaction contexts: error={}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Error removing stale compaction contexts: " + e.getMessage())
                .build();
        }
    }

    @POST
    @Path("/profile")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addProfile(CompactionProfileConfig compactionProfileConfig) {
        try {
            boolean success = abstractConfigWriter.storeProfile(storeResource, compactionProfileConfig);

            if (success) {
                log.info("Successfully added compaction profile: profileID={}", compactionProfileConfig.getID());
                return Response.status(Response.Status.CREATED)
                    .entity(compactionProfileConfig)
                    .build();
            } else {
                log.warn("Failed to add compaction profile: profileID={}", compactionProfileConfig.getID());
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Failed to store compaction profile")
                    .build();
            }
        } catch (RuntimeException e) {
            log.error("Error while adding compaction profile: profileID={}, error={}",
                compactionProfileConfig.getID(), e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Invalid compaction profile: " + e.getMessage())
                .build();
        }
    }

    @GET
    @Path("/contexts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getContexts() {
        try {
            List<CompactionContext> contexts = abstractConfigLoader.getCompactionContexts(storeResource);
            log.info("Successfully retrieved {} compaction contexts", contexts.size());
            return Response.status(Response.Status.OK)
                .entity(contexts)
                .build();
        } catch (ConfigurationException e) {
            log.error("Configuration error retrieving compaction contexts: error={}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Error retrieving compaction contexts: " + e.getMessage())
                .build();
        }
    }


    @GET
    @Path("/profiles")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getProfiles() {
        try {
            List<CompactionProfileConfig> profiles = abstractConfigLoader.getProfiles(storeResource);
            log.info("Successfully retrieved {} compaction profiles", profiles.size());
            return Response.status(Response.Status.OK)
                .entity(profiles)
                .build();
        } catch (ConfigurationException e) {
            log.error("Configuration error retrieving compaction profiles: error={}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Error retrieving compaction profiles: " + e.getMessage())
                .build();
        }
    }
}
