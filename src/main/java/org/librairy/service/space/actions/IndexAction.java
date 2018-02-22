package org.librairy.service.space.actions;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.avro.AvroRemoteException;
import org.librairy.service.space.data.model.ShapebyCluster;
import org.librairy.service.space.data.model.Space;
import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.services.MyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class IndexAction implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexAction.class);

    private final MyService service;
    private final Double threshold;

    private final SimpleDateFormat dateFormatter;
    private int total;

    public IndexAction(MyService service, Double threshold) {
        this.service            = service;
        this.threshold          = threshold;
        this.total              = 0;
        this.dateFormatter      = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
    }

    @Override
    public void run() {

        Instant startModel          = Instant.now();

        service.getShapesDao().removeAll();
        service.getCountersDao().remove("shapes");
        service.getCountersDao().remove("neighbours");

        LOG.info("Indexing points..");

        Statement statement = new SimpleStatement("select id, name, type, shape from points;");
        statement.setFetchSize(1000);

        ResultSet rs = service.getPointsDao().getSession().execute(statement);
        for (Row row : rs) {
            if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
                rs.fetchMoreResults(); // this is asynchronous

            // Process the row ...
            service.getExecutors().submit(() -> {
                Point point = service.getPointsDao().pointFrom(row);
                service.getShapesDao().save(point, threshold);
            });


        }

        service.setIndexing(false);

        // Create space
        Space space = new Space();
        space.setDate(dateFormatter.format(new Date()));
        space.setThreshold(threshold);

        service.getSpacesDao().save(space);

        LOG.info("waiting to persist indexes..");
        try {
            while(!service.isIndexed()){
                Thread.sleep(2000);
            }
        } catch (AvroRemoteException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Instant endModel    = Instant.now();
        LOG.info("All points ("+this.total+") indexed in: "
                + ChronoUnit.HOURS.between(startModel,endModel) + "hours "
                + ChronoUnit.MINUTES.between(startModel,endModel)%60 + "min "
                + (ChronoUnit.SECONDS.between(startModel,endModel)%3600) + "secs");
    }
}
