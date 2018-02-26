package org.librairy.service.space.services;

import com.google.common.primitives.Doubles;
import org.apache.avro.AvroRemoteException;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.librairy.service.space.Application;
import org.librairy.service.space.data.model.ShapebyCluster;
import org.librairy.service.space.facade.model.Neighbour;
import org.librairy.service.space.facade.model.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@WebAppConfiguration
public class MyServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(MyServiceTest.class);

    Integer dimension   = 100;
    Integer numVectors  = 100000;//1000000;
    Double threshold    = 0.7;

    @Autowired
    MyService service;


    @Test
    @Ignore
    public void addPoints() throws InterruptedException, AvroRemoteException {

        service.removeAll();

        LOG.info("creating points..");
        List<Point> points = IntStream.range(0, numVectors).mapToObj(i -> new DirichletDistribution("doc" + i, dimension)).map(d -> Point.newBuilder().setId(d.getId()).setName("document-" + d.getId()).setShape(d.getVector()).build()).collect(Collectors.toList());


        // Prepare points
        LOG.info("adding points..");
        Instant startModel          = Instant.now();
        points.stream().forEach(point -> {
            try {
                service.addPoint(point);
            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }
        });

        LOG.info("Waiting for finish..");
        while(!service.isIndexed()){
            Thread.sleep(500);
        }

        Instant endModel    = Instant.now();
        LOG.info("All points ("+this.numVectors+") added and indexed in: "
                + ChronoUnit.HOURS.between(startModel,endModel) + "hours "

                + ChronoUnit.MINUTES.between(startModel,endModel)%60 + "min "
                + (ChronoUnit.SECONDS.between(startModel,endModel)%3600) + "secs");

    }

    @Test
    @Ignore
    public void index() throws InterruptedException, AvroRemoteException {

        try {
            service.index(threshold);
            LOG.info("Space request sent!");
        } catch (AvroRemoteException e) {
            e.printStackTrace();
        }

        LOG.info("Waiting for indexing");
        Thread.currentThread().sleep(Long.MAX_VALUE);

        LOG.info("Operation completed!");
    }

    @Test
    @Ignore
    public void getNeighbours(){

        // Request Neighbours
        IntStream.range(0, numVectors).forEach(p -> {
            try {
                LOG.info("Neighbour of " + p);
                service.getNeighbours("doc"+p, 10, Collections.emptyList(), false).forEach(n -> LOG.info("\t " + n));
            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    @Ignore
    public void getSimilarPoints(){
        IntStream.range(0, numVectors).parallel().mapToObj(i -> new DirichletDistribution("shape" + i, dimension)).forEach( d -> {
            try {
                LOG.info("Similar points of " + Arrays.toString(Doubles.toArray(d.getVector())));
                service.getSimilar(d.getVector(), 10, Collections.emptyList(), false).forEach( n -> LOG.info("\t " + n));
            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }
        });
    }


    @Test
    @Ignore
    public void hybridSimilarPoints(){

        Point point = service.getPointsDao().read("doc1");

        LOG.info("Point:" + point);

        String cluster = ShapebyCluster.getSortedTopics(point.getShape(), threshold);
        LOG.info("Cluster: " + cluster);

//        service.getPointsDao().listAll().stream().limit(1).forEach(point -> {

            try {
                List<Neighbour> neighbours = service.getNeighbours(point.getId(), 10, Collections.emptyList(), false);
                LOG.info("Neighbours: ");
                neighbours.forEach(n -> LOG.info("\t " + n));

                List<Neighbour> similarPoints = service.getSimilar(point.getShape(), 10, Collections.emptyList(), false);
                LOG.info("Similar: ");
                similarPoints.forEach(n -> LOG.info("\t " + n));

                if (neighbours.size() != similarPoints.size()) LOG.error("Neighbours: " + neighbours.size() + " / SimilarPoints: " + similarPoints.size());

            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }

//        });
    }
}