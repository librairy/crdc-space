package org.librairy.service.space.data.access;

import com.datastax.driver.core.ResultSetFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public abstract class Dao {

    private static final Logger LOG = LoggerFactory.getLogger(Dao.class);

    public abstract String createTable();

    public abstract void prepareQueries();

    ConcurrentLinkedQueue<ResultSetFuture> futures = new ConcurrentLinkedQueue();

    protected boolean enqueue(ResultSetFuture future){
        try{
            futures.add(future);
            if (futures.size() < 500) return true;

            for(ResultSetFuture t: futures){
                try {
                    t.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    LOG.warn("Interrupted saving data on db",e);
                }
            }
            futures.clear();
            return true;
        }catch (Exception e){
            LOG.error("Error saving data on db",e);
            return false;
        }
    }
}
