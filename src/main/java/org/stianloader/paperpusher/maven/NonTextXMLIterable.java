package org.stianloader.paperpusher.maven;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import xmlparser.model.XmlElement;
import xmlparser.model.XmlElement.XmlTextElement;

public class NonTextXMLIterable implements Iterable<XmlElement> {

    public static class NonTextXMLIterator implements Iterator<@NotNull XmlElement> {

        private int index = 0;
        private final List<XmlElement> list;

        public NonTextXMLIterator(XmlElement parent) {
            this.list = parent.children;
        }

        @Override
        public boolean hasNext() {
            if (this.list.size() <= this.index) {
                return false;
            }
            if (!(this.list.get(this.index) instanceof XmlTextElement)) {
                return true;
            }
            while (this.list.size() > ++this.index) {
                if (!(this.list.get(this.index) instanceof XmlTextElement)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        @NotNull
        public XmlElement next() {
            while (this.hasNext()) {
                XmlElement e = this.list.get(this.index++);
                if (!(e instanceof XmlTextElement)) {
                    assert e != null;
                    return e;
                }
            }
            throw new NoSuchElementException("Node list exhausted");
        }
    }

    private final XmlElement parent;

    public NonTextXMLIterable(XmlElement parent) {
        if (parent == null) {
            throw new NullPointerException("The argument \"parent\" is null.");
        }
        this.parent = parent;
    }

    @Override
    public Iterator<XmlElement> iterator() {
        return new NonTextXMLIterator(this.parent);
    }
}
