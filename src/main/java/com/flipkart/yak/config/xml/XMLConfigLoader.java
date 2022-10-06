package com.flipkart.yak.config.xml;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfile;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.config.CompactionTriggerConfig;
import com.flipkart.yak.config.loader.AbstractFileBasedConfigLoader;
import lombok.extern.slf4j.Slf4j;
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
        return compactionProfiles;
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
