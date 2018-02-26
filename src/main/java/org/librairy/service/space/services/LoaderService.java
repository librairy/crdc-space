package org.librairy.service.space.services;

import org.librairy.service.space.data.access.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class LoaderService implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(LoaderService.class);

    @Autowired
    List<Dao> daoList;

    @Autowired
    List<BootService> bootServices;

    public void onApplicationEvent(ContextRefreshedEvent event) {
        daoList.forEach(d -> d.prepareQueries());

        bootServices.forEach( bs -> bs.prepare());
        LOG.info("All is up and running!");
    }
}
