package com.flipkart.yak.config.xml;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfile;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.config.CompactionTriggerConfig;
import com.flipkart.yak.config.loader.AbstractFileBasedConfigLoader;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class XMLConfigLoader extends AbstractFileBasedConfigLoader {

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();



    @Override
    protected CompactionTriggerConfig loadConfigFromFile(File xmlFileName) throws ParserConfigurationException, IOException {
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc;
        try {
             doc = db.parse(xmlFileName);
             doc.getDocumentElement().normalize();
        } catch (SAXException e) {
            log.error(e.getMessage());
            throw new IOException(e);
        }
        List<CompactionContext> allContexts = this.getAllContexts(doc);
        List<CompactionProfile> allProfiles = this.getAllProfiles(doc);
        CompactionTriggerConfig.Builder builder = new CompactionTriggerConfig.Builder();
        return builder.withCompactionContexts(new HashSet<>(allContexts)).withCompactionProfiles(new HashSet<>(allProfiles)).build();
    }

    private List<CompactionContext> getAllContexts(Document doc) {
        List<CompactionContext> compactionContexts = new ArrayList<>();
        NodeList allContexts = doc.getElementsByTagName(XMLConfigTags.CONTEXTS_LIST_TAG.name);
        for (int i=0; i < allContexts.getLength(); i++) {
            Node currContextList = allContexts.item(i);
            if(currContextList.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            NodeList allContextsUnderCurrContextList = currContextList.getChildNodes();
            for (int j=0; j < allContextsUnderCurrContextList.getLength(); j++) {
                Node contextElement = allContextsUnderCurrContextList.item(j);
                if (contextElement.getNodeType() != Node.ELEMENT_NODE) {
                    continue;
                }
                NodeList contextDetails = contextElement.getChildNodes();
                compactionContexts.add(this.prepareCompactionContext(contextDetails));
            }
        }
        return compactionContexts;
    }

    private List<CompactionProfile> getAllProfiles(Document doc) {
        List<CompactionProfile> compactionProfiles = new ArrayList<>();
        NodeList allProfileMentionedInConfig = doc.getElementsByTagName(XMLConfigTags.PROFILE_LIST_TAG.name);
        for (int i=0; i < allProfileMentionedInConfig.getLength(); i++) {
            Node currentProfileDetails = allProfileMentionedInConfig.item(i);
            if(currentProfileDetails.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            NodeList children = currentProfileDetails.getChildNodes();
            String profileID=null;
            Set<RegionSelectionPolicy> policies = null ;
            PolicyAggregator aggregator = null;
            CompactionProfile compactionProfile;
            for (int j=0; j< children.getLength(); j++) {
                Node currentChild = children.item(j);
                if (currentChild.getNodeType() == Node.ELEMENT_NODE &&
                        currentChild.getNodeName().equals(XMLConfigTags.CONTEXT_PROFILE_ID.name)) {
                    profileID = currentChild.getTextContent();
                }
                if (currentChild.getNodeType() == Node.ELEMENT_NODE &&
                        currentChild.getNodeName().equals(XMLConfigTags.POLICY_LIST_TAG.name)) {
                    policies = this.preparePolicies(currentChild);
                }
                if (currentChild.getNodeType() == Node.ELEMENT_NODE &&
                        currentChild.getNodeName().equals(XMLConfigTags.AGGREGATOR_TAG.name)) {
                    aggregator = this.prepareAggregator(currentChild);
                }
            }
            if (profileID!=null) {
                compactionProfile = new CompactionProfile(profileID, policies, aggregator);
                compactionProfiles.add(compactionProfile);
            }
        }
        return compactionProfiles;
    }

    private PolicyAggregator prepareAggregator(Node aggregator) {
        PolicyAggregator policyAggregator = null;
        AtomicReference<String> clazzToLoad = new AtomicReference<>();
        AtomicReference<List<Pair<String, String>>> configuration = new AtomicReference<>();
        XMLUtils.forEach(aggregator.getChildNodes(), p-> {
            XMLUtils.forTag(XMLConfigTags.CONFIG_LIST_TAGS, p, config -> {configuration.set(this.prepareConfigList(config));});
            XMLUtils.forTag(XMLConfigTags.NAME_TAG, p , name -> { clazzToLoad.set(name.getTextContent());});
        });
        policyAggregator = this.loadAggregator(clazzToLoad.get());
        policyAggregator.setFromConfig(configuration.get());
        return policyAggregator;
    }

    private Set<RegionSelectionPolicy> preparePolicies(Node policies) {
        Set<RegionSelectionPolicy> regionSelectionPolicySet = new HashSet<>();
        NodeList policyList  = policies.getChildNodes();
        XMLUtils.forEach(policyList, c-> {
            NodeList children = c.getChildNodes();
            RegionSelectionPolicy regionSelectionPolicy;
            AtomicReference<String> clazzToLoad = new AtomicReference<>();
            AtomicReference<List<Pair<String, String>>> configuration = new AtomicReference<>();
            XMLUtils.forEach(children, item -> { XMLUtils.forTag(XMLConfigTags.CONFIG_LIST_TAGS, item, config -> {
                configuration.set(this.prepareConfigList(config));});});
            XMLUtils.forEach(children, item -> { XMLUtils.forTag(XMLConfigTags.NAME_TAG, item , name -> {
                clazzToLoad.set(name.getTextContent());
            }); });
            regionSelectionPolicy = this.loadPolicy(clazzToLoad.get());
            regionSelectionPolicy.setFromConfig(configuration.get());
            regionSelectionPolicySet.add(regionSelectionPolicy);
        });
        return regionSelectionPolicySet;
    }

    private List<Pair<String, String>> prepareConfigList(Node node) {
        List<Pair<String, String>> configs = new ArrayList<>();
        XMLUtils.forEach(node.getChildNodes(),  config -> {
            XMLUtils.forEach(config.getChildNodes(), item -> {
                String name = XMLUtils.forTagReturnString(XMLConfigTags.NAME_TAG, item, Node::getTextContent);
                String value = XMLUtils.forTagReturnString(XMLConfigTags.VALUE_TAG, item, Node::getTextContent);
                Pair<String, String> pair = new Pair<>(name, value);
                configs.add(pair);
            });
        });
        return configs;
    }

    private CompactionContext prepareCompactionContext(NodeList contextElements) {
        Map<XMLConfigTags, String> configLoadedMap = this.prepareConfigMap(contextElements);
        int startTime = Integer.parseInt(configLoadedMap.get(XMLConfigTags.CONTEXT_START_TIME));
        int endTime = Integer.parseInt(configLoadedMap.get(XMLConfigTags.CONTEXT_END_TIME));
        CompactionSchedule compactionSchedule = new CompactionSchedule(startTime, endTime);
        String cluster = configLoadedMap.get(XMLConfigTags.CONTEXT_CLUSTER_ID);
        String profile = configLoadedMap.get(XMLConfigTags.CONTEXT_PROFILE_ID);
        CompactionContext compactionContext = new CompactionContext(cluster, compactionSchedule, profile);
        this.setContextWithPriorityOrder(compactionContext, configLoadedMap);
        return compactionContext;
    }

    private void setContextWithPriorityOrder(CompactionContext compactionContext, Map<XMLConfigTags, String> configLoadedMap) {
        if (configLoadedMap.containsKey(XMLConfigTags.CONTEXT_RSGROUP)) {
            compactionContext.setRsGroup(configLoadedMap.get(XMLConfigTags.CONTEXT_RSGROUP));
        } else if (configLoadedMap.containsKey(XMLConfigTags.CONTEXT_NAMESPACE)) {
            compactionContext.setRsGroup(configLoadedMap.get(XMLConfigTags.CONTEXT_NAMESPACE));
        } else if (configLoadedMap.containsKey(XMLConfigTags.CONTEXT_TABLE_NAME)) {
            compactionContext.setRsGroup(configLoadedMap.get(XMLConfigTags.CONTEXT_TABLE_NAME));
        }
    }

    private Map<XMLConfigTags, String> prepareConfigMap(NodeList contextElements) {
        Map<XMLConfigTags, String> configLoadedMap = new HashMap<>();
        configLoadedMap.put(XMLConfigTags.CONTEXT_PROFILE_ID,"default");
        for (int k =0; k < contextElements.getLength(); k++) {
            if (contextElements.item(k).getNodeType() == Node.ELEMENT_NODE) {
                Node node = contextElements.item(k);
                XMLConfigTags key = XMLConfigTags.getEnum(node.getNodeName());
                if (key != null) {
                    configLoadedMap.put(key, node.getTextContent());
                }
            }
        }
        return configLoadedMap;
    }

}
