package com.flipkart.yak.config.xml;

import com.flipkart.yak.config.*;
import com.flipkart.yak.config.loader.AbstractFileBasedConfigLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
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

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class XMLConfigLoader extends AbstractFileBasedConfigLoader {

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    private final String DEFAULT_RESOURCE_FILE = "compactor-config.xml";

    private Document loadDocument(InputStream file) throws ConfigurationException {
        Document doc = null;
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            doc = db.parse(file);
            doc.getDocumentElement().normalize();
            file.close();
        } catch (SAXException | ParserConfigurationException | IOException e) {
            log.error(e.getMessage());
            throw new ConfigurationException(e);
        }
        return doc;
    }


    @Override
    public List<CompactionProfileConfig> getProfiles(File resource) throws ConfigurationException {
        Document doc = this.loadDocument(ClassLoader.getSystemResourceAsStream(resource.getName()));
        return this.getAllProfiles(doc);
    }

    @Override
    public List<CompactionContext> getCompactionContexts(File resource) throws ConfigurationException {
        Document doc = this.loadDocument(ClassLoader.getSystemResourceAsStream(resource.getName()));
        return this.getAllContexts(doc);
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


    private List<CompactionProfileConfig> getAllProfiles(Document doc) {
        List<CompactionProfileConfig> compactionProfileConfigs = new ArrayList<>();
        NodeList allProfileMentionedInConfig = doc.getElementsByTagName(XMLConfigTags.PROFILE_LIST_TAG.name);
        XMLUtils.forEach(allProfileMentionedInConfig, currProfile -> {
            NodeList children = currProfile.getChildNodes();
            AtomicReference<String> profileID= new AtomicReference<>();
            AtomicReference<Set<SerializedConfigurable>> policies = new AtomicReference<>();
            AtomicReference<SerializedConfigurable> aggregator = new AtomicReference<>();
            CompactionProfileConfig compactionProfileConfig;
            XMLUtils.forEach(children, currentChild -> {
                XMLUtils.forTag(XMLConfigTags.CONTEXT_PROFILE_ID, currentChild, item -> profileID.set(currentChild.getTextContent()));
                XMLUtils.forTag(XMLConfigTags.POLICY_LIST_TAG, currentChild, item -> policies.set(this.preparePolicies(currentChild)));
                XMLUtils.forTag(XMLConfigTags.AGGREGATOR_TAG, currentChild, item -> aggregator.set(this.prepareAggregator(currentChild)));
            });
            if (profileID.get() !=null) {
                compactionProfileConfig = new CompactionProfileConfig(profileID.get(), policies.get(), aggregator.get());
                compactionProfileConfigs.add(compactionProfileConfig);
            }
        });
        return compactionProfileConfigs;
    }

    private SerializedConfigurable prepareAggregator(Node aggregator) {
        SerializedConfigurable policyAggregator = null;
        AtomicReference<String> clazzToLoad = new AtomicReference<>();
        AtomicReference<List<Pair<String, String>>> configuration = new AtomicReference<>();
        XMLUtils.forEach(aggregator.getChildNodes(), p-> {
            XMLUtils.forTag(XMLConfigTags.CONFIG_LIST_TAGS, p, config -> {configuration.set(this.prepareConfigList(config));});
            XMLUtils.forTag(XMLConfigTags.NAME_TAG, p , name -> { clazzToLoad.set(name.getTextContent());});
        });
        policyAggregator = new SerializedConfigurable(clazzToLoad.get(), configuration.get());
        return policyAggregator;
    }

    private Set<SerializedConfigurable> preparePolicies(Node policies) {
        Set<SerializedConfigurable> regionSelectionPolicySet = new HashSet<>();
        NodeList policyList  = policies.getChildNodes();
        XMLUtils.forEach(policyList, c-> {
            NodeList children = c.getChildNodes();
            AtomicReference<String> clazzToLoad = new AtomicReference<>();
            AtomicReference<List<Pair<String, String>>> configuration = new AtomicReference<>();
            XMLUtils.forEach(children, item -> { XMLUtils.forTag(XMLConfigTags.CONFIG_LIST_TAGS, item, config -> {
                configuration.set(this.prepareConfigList(config));});});
            XMLUtils.forEach(children, item -> { XMLUtils.forTag(XMLConfigTags.NAME_TAG, item , name -> {
                clazzToLoad.set(name.getTextContent());
            }); });
            SerializedConfigurable  regionSelectionPolicyToLoad = new SerializedConfigurable(clazzToLoad.get(), configuration.get());
            regionSelectionPolicySet.add(regionSelectionPolicyToLoad);
        });
        return regionSelectionPolicySet;
    }

    private List<Pair<String, String>> prepareConfigList(Node node) {
        List<Pair<String, String>> configs = new ArrayList<>();
        XMLUtils.forEach(node.getChildNodes(),  config -> {
            AtomicReference<String> name = new AtomicReference<>();
            AtomicReference<String> value = new AtomicReference<>();
            XMLUtils.forEach(config.getChildNodes(), item -> {
                if (XMLUtils.forTagReturnString(XMLConfigTags.NAME_TAG, item, Node::getTextContent) != null ) {
                    name.set(XMLUtils.forTagReturnString(XMLConfigTags.NAME_TAG, item, Node::getTextContent));
                }
                if (XMLUtils.forTagReturnString(XMLConfigTags.VALUE_TAG, item, Node::getTextContent) != null ) {
                    value.set(XMLUtils.forTagReturnString(XMLConfigTags.VALUE_TAG, item, Node::getTextContent));
                }
            });
            Pair<String, String> pair = new Pair<>(name.get(), value.get());
            configs.add(pair);
        });
        return configs;
    }

    private CompactionContext prepareCompactionContext(NodeList contextElements) {
        Map<XMLConfigTags, String> configLoadedMap = this.prepareConfigMap(contextElements);
        float startTime = Float.parseFloat(configLoadedMap.get(XMLConfigTags.CONTEXT_START_TIME));
        float endTime = Float.parseFloat(configLoadedMap.get(XMLConfigTags.CONTEXT_END_TIME));
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
            compactionContext.setNameSpace(configLoadedMap.get(XMLConfigTags.CONTEXT_NAMESPACE));
        }
        if (configLoadedMap.containsKey(XMLConfigTags.CONTEXT_TABLE_NAME)) {
            compactionContext.setTableNames(configLoadedMap.get(XMLConfigTags.CONTEXT_TABLE_NAME));
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

    @Override
    public void loadDefaults() {
        this.addResource(this.DEFAULT_RESOURCE_FILE);
    }
}
