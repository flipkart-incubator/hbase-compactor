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
        boolean response = abstractConfigWriter.storeContext(storeResource, compactionContext);
        return Response.status(Response.Status.CREATED).entity(response).build();
    }

    @DELETE
    @Path("/context")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeContext(CompactionContext compactionContext) {
        boolean response = abstractConfigWriter.deleteContext(storeResource, compactionContext);
        return Response.status(Response.Status.OK).entity(response).build();
    }

    @POST
    @Path("/trigger")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerImmediateCompaction(PromptCompactionRequest promptCompactionRequest) {
        CompactionContext compactionContext = CompactionUtils.getCompactionContext(promptCompactionRequest);
        boolean response = abstractConfigWriter.storeContext(storeResource, compactionContext);
        return Response.status(Response.Status.ACCEPTED).entity(response).build();
    }

    @DELETE
    @Path("/deleteAllStaleContexts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeStaleContexts() {
        boolean response = abstractConfigWriter.deleteAllStaleContexts(storeResource);
        return Response.status(Response.Status.OK).entity(response).build();
    }

    @POST
    @Path("/profile")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addProfile(CompactionProfileConfig compactionProfileConfig) {
        boolean response = abstractConfigWriter.storeProfile(storeResource, compactionProfileConfig);
        return Response.status(Response.Status.CREATED).entity(response).build();
    }

    @GET
    @Path("/contexts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getContexts() {
        try {
            List<CompactionContext> contexts = abstractConfigLoader.getCompactionContexts(storeResource);
            return Response.status(Response.Status.OK).entity(contexts).build();
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @GET
    @Path("/profiles")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getProfiles() {
        try {
            List<CompactionProfileConfig> profiles = abstractConfigLoader.getProfiles(storeResource);
            return Response.status(Response.Status.OK).entity(profiles).build();
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
}