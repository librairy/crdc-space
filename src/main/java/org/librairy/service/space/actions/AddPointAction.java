package org.librairy.service.space.actions;

import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.services.MyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class AddPointAction implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AddPointAction.class);

    private final MyService service;
    private final Double threshold;
    private final Point point;

    public AddPointAction(MyService service, Point point, Double threshold) {
        this.service            = service;
        this.threshold          = threshold;
        this.point              = point;
    }

    @Override
    public void run() {
        try{
            service.getPointsDao().save(point);
            service.getShapesDao().save(point, threshold);
        }catch (Exception e){
            LOG.error("Unexpected error adding point", e);
        }
    }
}
