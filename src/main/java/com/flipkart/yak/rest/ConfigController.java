package com.flipkart.yak.rest;

import com.flipkart.yak.config.*;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.util.Pair;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Slf4j
@Path("/manage")
public class ConfigController {
    private final AbstractConfigWriter abstractConfigWriter;
    private final AbstractConfigLoader abstractConfigLoader;
    private final Object storeResource;

    public ConfigController(StoreFactory storeFactory, String storeResourceProperty) throws ConfigurationException {
         abstractConfigWriter = storeFactory.getWriter();
         abstractConfigLoader = storeFactory.getLoader();
         storeResource = abstractConfigWriter.init(storeResourceProperty);
    }

    @POST
    @Path("/context")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public boolean addContext(CompactionContext compactionContext) {
        boolean response = abstractConfigWriter.storeContext(storeResource, compactionContext);
        return response;
    }

    @POST
    @Path("/profile")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public boolean addProfile(CompactionProfileConfig compactionProfileConfig) {
        boolean response = abstractConfigWriter.storeProfile(storeResource, compactionProfileConfig);
        return response;
    }

    @GET
    @Path("/contexts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<CompactionContext> getContexts() {
        try {
            return abstractConfigLoader.getCompactionContexts(storeResource);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @GET
    @Path("/profiles")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<CompactionProfileConfig> getProfiles() {

//        List<Pair<String, String>> test = new ArrayList<>();
//        test.add(new Pair<>("aggregator.chain.policy.order", "com.flipkart.yak.policies.TimestampAwareSelectionPolicy,com.flipkart.yak.policies.NaiveRegionSelectionPolicy"));
//        SerializedConfigurable aggregator = new SerializedConfigurable("com.flipkart.yak.aggregator.ChainReportAggregator", test);
//        List<Pair<String, String>> test1 = new ArrayList<>();
//        SerializedConfigurable policy1 = new SerializedConfigurable("com.flipkart.yak.policies.NaiveRegionSelectionPolicy", test1);
//        SerializedConfigurable policy2 = new SerializedConfigurable("com.flipkart.yak.policies.TimestampAwareSelectionPolicy", new ArrayList<>());
//        CompactionProfileConfig compactionProfileConfig = new CompactionProfileConfig("default", new HashSet<>(),aggregator);
//        List<CompactionProfileConfig> response = new ArrayList<>();
//        compactionProfileConfig.getPolicies().add(policy1);
//        compactionProfileConfig.getPolicies().add(policy2);
//        response.add(compactionProfileConfig);
        try {
            return abstractConfigLoader.getProfiles(storeResource);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
}
