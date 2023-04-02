package com.flipkart.yak.rest;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.config.StoreFactory;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Path("/manage")
public class ConfigController {
    private final AbstractConfigWriter abstractConfigWriter;

    public ConfigController(StoreFactory storeFactory) {
         abstractConfigWriter = storeFactory.getWriter();
    }

    @POST
    @Path("/context")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public boolean addContext(CompactionContext compactionContext) {
        return false;
    }

    @POST
    @Path("/profiles")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public boolean addProfile(CompactionProfileConfig compactionProfileConfig) {
        return false;
    }

    @GET
    @Path("/contexts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<CompactionContext> getContexts() {
        CompactionSchedule compactionSchedule = new CompactionSchedule(10,12);
        CompactionContext compactionContext = new CompactionContext("localhost:2181", compactionSchedule, "profile");
        List<CompactionContext> response = new ArrayList<>();
        response.add(compactionContext);
        return response;
    }
}
