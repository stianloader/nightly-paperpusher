package org.stianloader.paperpusher.maven;

import java.util.Optional;

import xmlparser.model.XmlElement;

public class XMLUtil {

    public static Optional<String> getValue(XmlElement parent, String key) {
        XmlElement element = parent.findChildForName(key, null);
        if (element == null) {
            return Optional.empty();
        }

        String text = element.getText();
        return Optional.of(text == null ? "" : text);
    }

    public static boolean updateValue(XmlElement parent, String key, String value) {
        XmlElement element = parent.findChildForName(key, null);
        if (element == null) {
            return false;
        }

        element.setText(value);
        return true;
    }

    public static Optional<XmlElement> getElement(XmlElement parent, String key) {
        return Optional.ofNullable(parent.findChildForName(key, null));
    }
}
