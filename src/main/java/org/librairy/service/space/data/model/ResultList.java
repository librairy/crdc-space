package org.librairy.service.space.data.model;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class ResultList<T> {

    private List<T> values;

    private String page;

    public ResultList(List<T> values, String page) {
        this.values = values;
        this.page = page;
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
}
