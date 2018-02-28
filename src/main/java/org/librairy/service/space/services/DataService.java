package org.librairy.service.space.services;

import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.librairy.service.space.data.access.Dao;
import org.librairy.service.space.tools.ResourceWaiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Configuration("dbSession")
public class DataService extends AbstractCassandraConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(DataService.class);

    @Value("#{environment['DB_HOST']?:'${db.host}'}")
    String hosts;

    @Value("#{environment['DB_PORT']?:${db.port}}")
    Integer port;

    @Autowired
    List<Dao> daoList;

    @Bean
    @DependsOn("dbChecker")
    public CassandraClusterFactoryBean cluster(){
        CassandraClusterFactoryBean cluster = new CassandraClusterFactoryBean();
        try{
            LOG.info("Initializying connection to database: " + hosts + " " + port + " ..");
            cluster.setContactPoints(hosts);
            cluster.setPort(port);
            List<CreateKeyspaceSpecification> specifications = new ArrayList<>();
            specifications.add(
                    new CreateKeyspaceSpecification(getKeyspaceName())
                            .ifNotExists()
                            .withSimpleReplication(1l)
            );
            cluster.setKeyspaceCreations(specifications);

//            SocketOptions options = new SocketOptions();
//            options.setReadTimeoutMillis(60000);
//            options.setConnectTimeoutMillis(20000);
//            cluster.setSocketOptions(options);
            LOG.info("Connected to Database");
        }catch (Exception e){
            LOG.error("Error configuring cassandra connection parameters: ",e);
        }
        return cluster;
    }

    @Bean("dbChecker")
    public Boolean waitForService(){
        return ResourceWaiter.waitFor(StringUtils.substringBefore(hosts,","), port);
    }

    @Override
    protected String getKeyspaceName() {
        return "space";
    }

    @Override
    protected List<String> getStartupScripts() {

        LOG.info("Loading database schema ...");

        List<String> schemaScripts = new ArrayList<>();

        //"CREATE KEYSPACE space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1  };"

        for(Dao dao: daoList){
            String statement = dao.createTable();
            if (!Strings.isNullOrEmpty(statement)) schemaScripts.add(statement);
        }

        return schemaScripts;
    }


}
