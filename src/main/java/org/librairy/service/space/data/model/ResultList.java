package org.librairy.service.space.data.model;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class ResultList<T> {

    private List<T> values;

    private String page;

    private long total;

    public ResultList(long total, List<T> values, String page) {
        this.values = values;
        this.page = page;
        this.total = total;
    }

    public List<T> getValues() {
        return values;
    }

    public void setValues(List<T> values) {
        this.values = values;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
