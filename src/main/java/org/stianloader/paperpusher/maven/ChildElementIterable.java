package org.stianloader.paperpusher.maven;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ChildElementIterable implements Iterable<Element> {

    public static class NodeListElementIterator implements Iterator<@NotNull Element> {

        private int index = 0;
        private final NodeList list;

        public NodeListElementIterator(NodeList list) {
            this.list = list;
        }

        @Override
        public boolean hasNext() {
            if (this.list.getLength() <= this.index) {
                return false;
            }
            if (this.list.item(index) instanceof Element) {
                return true;
            }
            while (this.list.getLength() > ++this.index) {
                if (this.list.item(index) instanceof Element) {
                    return true;
                }
            }
            return false;
        }

        @Override
        @NotNull
        public Element next() {
            while (this.hasNext()) {
                if (this.list.item(this.index++) instanceof Element e) {
                    return e;
                }
            }
            throw new NoSuchElementException("Node list exhausted");
        }
    }

    public static class NodeListIterator implements Iterator<Node> {

        private int index = 0;
        private final NodeList list;

        public NodeListIterator(NodeList list) {
            this.list = list;
        }

        @Override
        public boolean hasNext() {
            return this.list.getLength() > this.index;
        }

        @Override
        public Node next() {
            return this.list.item(this.index++);
        }
    }

    private final Element parent;

    public ChildElementIterable(Element parent) {
        if (parent == null) {
            throw new NullPointerException("The argument \"parent\" is null.");
        }
        this.parent = parent;
    }

    @Override
    public Iterator<Element> iterator() {
        return new NodeListElementIterator(this.parent.getChildNodes());
    }
}
