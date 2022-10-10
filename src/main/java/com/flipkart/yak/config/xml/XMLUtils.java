package com.flipkart.yak.config.xml;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


class XMLUtils {

    static void forEach(NodeList list, Consumer<Node> consumer) {
        for (int i =0; i< list.getLength(); i++) {
            Node item = list.item(i);
            if(item.getNodeType() == Node.ELEMENT_NODE) {
                consumer.accept(item);
            }
        }
    }

    static List<String> forEachReturnString(NodeList list, Function<Node, String> function) {
        List<String> response = new ArrayList<>();
        for (int i =0; i< list.getLength(); i++) {
            Node item = list.item(i);
            if(item.getNodeType() == Node.ELEMENT_NODE) {
                response.add(function.apply(item));
            }
        }
        return response;
    }

    static void forTag(XMLConfigTags tag, Node node, Consumer<Node> consumer) {
        if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals(tag.name)) {
            consumer.accept(node);
        }
    }

    static String forTagReturnString(XMLConfigTags tag, Node node, Function<Node, String> function) {
        if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals(tag.name)) {
            return function.apply(node);
        }
        return null;
    }
}
