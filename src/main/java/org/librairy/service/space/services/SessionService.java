package org.librairy.service.space.services;

import com.datastax.driver.core.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class SessionService {

    @Autowired
    CassandraClusterFactoryBean clusterFactoryBean;

    private Session session;

    @PostConstruct
    public void setup(){
        session = clusterFactoryBean.getObject().connect("space");
    }

    @PreDestroy
    public void close(){
        session.close();
    }

    public Session getSession() {
        return session;
    }
}
