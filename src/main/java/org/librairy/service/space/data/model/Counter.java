package org.librairy.service.space.data.model;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class Counter {
    private String label;
    private long size;

    public Counter(String label, long size) {
        this.label = label;
        this.size = size;
    }

    public Counter() {
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
