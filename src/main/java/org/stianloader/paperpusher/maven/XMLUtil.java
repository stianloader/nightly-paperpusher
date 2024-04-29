package org.stianloader.paperpusher.maven;

import java.util.Iterator;
import java.util.Optional;

import org.jetbrains.annotations.NotNull;
import org.w3c.dom.Element;

public class XMLUtil {

    public static Optional<String> getValue(Element parent, String key) {
        Iterator<Element> it = new ChildElementIterable.NodeListElementIterator(parent.getElementsByTagName(key));

        if (!it.hasNext()) {
            return Optional.empty();
        }

        Element e = it.next();
        return Optional.of(e.getTextContent());
    }

    public static boolean updateValue(Element parent, String key, String value) {
        Iterator<Element> it = new ChildElementIterable.NodeListElementIterator(parent.getElementsByTagName(key));

        while (it.hasNext()) {
            Element e = it.next();
            // Yes, getElementsByTagName is recursive
            if (e.getParentNode() != parent) {
                continue;
            }
            e.setTextContent(value);
            return true;
        }

        return false;
    }

    public static Optional<Element> getElement(Element parent, String key) {
        Iterator<@NotNull Element> it = new ChildElementIterable.NodeListElementIterator(parent.getElementsByTagName(key));

        if (!it.hasNext()) {
            return Optional.empty();
        }

        Element e = it.next();
        return Optional.of(e);
    }
}
