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

import java.util.Arrays;
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

    Integer dimension   = 10;
    Integer numVectors  = 100000;//1000000;
    Double threshold    = 0.7;

    @Autowired
    MyService service;


    @Test
    @Ignore
    public void addPointsAndIndex() throws InterruptedException, AvroRemoteException {

        service.removeAll();

        // Prepare points
        LOG.info("adding points..");
        List<Point> points = IntStream.range(0, numVectors).mapToObj(i -> new DirichletDistribution("doc" + i, dimension)).map(d -> Point.newBuilder().setId(d.getId()).setName("document-" + d.getId()).setShape(d.getVector()).build()).collect(Collectors.toList());


        // Add points
        points.forEach(p -> {
            try {
                service.addPoint(p);
            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }
        });

        // Create a space
        try {
            service.index(threshold);
            LOG.info("Space request sent!");
        } catch (AvroRemoteException e) {
            e.printStackTrace();
        }
        LOG.info("Operation completed!");
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
        IntStream.range(0, numVectors).parallel().forEach(p -> {
            try {
                LOG.info("Neighbour of " + p);
                service.getNeighbours("doc"+p, 10, null).forEach( n -> LOG.info("\t " + n));
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
                service.getSimilar(d.getVector(), 10, null).forEach( n -> LOG.info("\t " + n));
            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }
        });
    }


    @Test
//    @Ignore
    public void hybridSimilarPoints(){

        Point point = service.getPointsDao().read("doc1");

        LOG.info("Point:" + point);

        String cluster = ShapebyCluster.getSortedTopics(point.getShape(), threshold);
        LOG.info("Cluster: " + cluster);

//        service.getPointsDao().listAll().stream().limit(1).forEach(point -> {

            try {
                List<Neighbour> neighbours = service.getNeighbours(point.getId(), 10, null);
                LOG.info("Neighbours: ");
                neighbours.forEach(n -> LOG.info("\t " + n));

                List<Neighbour> similarPoints = service.getSimilar(point.getShape(), 10, null);
                LOG.info("Similar: ");
                similarPoints.forEach(n -> LOG.info("\t " + n));

                if (neighbours.size() != similarPoints.size()) LOG.error("Neighbours: " + neighbours.size() + " / SimilarPoints: " + similarPoints.size());

            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }

//        });
    }
}