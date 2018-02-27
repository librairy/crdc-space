package org.librairy.service.space.controllers;


import org.apache.avro.AvroRemoteException;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.librairy.service.space.Application;
import org.librairy.service.space.AvroClient;
import org.librairy.service.space.data.model.ShapebyCluster;
import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.services.CSVReader;
import org.librairy.service.space.services.CSVWriter;
import org.librairy.service.space.services.DirichletDistribution;
import org.librairy.service.space.services.EndOfFileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class AvroTest {

    private static final Logger LOG = LoggerFactory.getLogger(AvroTest.class);

    @Test
    @Ignore
    public void writePoints() throws IOException {
        Integer numVectors  = 500000;
        Integer dimension   = 10;

        CSVWriter writer = new CSVWriter();

        writer.open();

        // Prepare points
        LOG.info("creating points..");
        Instant startModel          = Instant.now();
        IntStream.range(0, numVectors).parallel().mapToObj(i -> new DirichletDistribution("doc" + i, dimension)).forEach(p -> writer.write(p));

        writer.close();

        Instant endModel    = Instant.now();
        LOG.info("All points ("+numVectors+") created in: "
                + ChronoUnit.HOURS.between(startModel,endModel) + "hours "

                + ChronoUnit.MINUTES.between(startModel,endModel)%60 + "min "
                + ChronoUnit.SECONDS.between(startModel,endModel)%60 + "secs");


    }


    @Test
    @Ignore
    public void addPoint() throws InterruptedException, IOException {

        AvroClient client = new AvroClient();


        //String host     = "librairy.linkeddata.es";
        String host     = "localhost";
        Integer port    = 65411;

        client.open(host,port);

        List<Double> vector = Arrays.asList(new Double[]{0.1,0.2});

        client.addPoint(Point.newBuilder().setId("sample").setShape(vector).build());

        client.close();
        LOG.info("completed!");
    }

    @Test
    @Ignore
    public void addPoints() throws InterruptedException, IOException {

        AvroClient client = new AvroClient();


        //String host     = "librairy.linkeddata.es";
        String host     = "localhost";
        Integer port    = 65115;
        Integer max     = 10000;

        client.open(host,port);

        LOG.info("removing points..");
        client.removeAll();


        LOG.info("adding points..");
        CSVReader reader = new CSVReader();
        Instant startModel          = Instant.now();
        reader.open();
        AtomicInteger counter =new AtomicInteger();
        while(true){
            try{
                DirichletDistribution point = reader.readLine();
                if (point.getVector().isEmpty()) continue;
                if (counter.incrementAndGet() > max) break;
                client.addPoint(Point.newBuilder().setId(point.getId()).setShape(point.getVector()).build());
            }catch (EndOfFileException e){
                break;
            }catch (Exception e){
                e.printStackTrace();
                break;
            }
        }


        /**
         * 500K -> 6min 26sec / 8min 36secs
         * 1M   -> 35min 28secs
         */
        LOG.info("waiting for finish...");
        while(!client.isIndexed()){
            Thread.sleep(5000);
        }

        Instant endModel    = Instant.now();
        LOG.info("All points ("+counter.get()+") added and indexed in: "
                + ChronoUnit.HOURS.between(startModel,endModel) + "hours "

                + ChronoUnit.MINUTES.between(startModel,endModel)%60 + "min "
                + ChronoUnit.SECONDS.between(startModel,endModel)%60 + "secs");

        client.close();
        LOG.info("completed!");
    }

    @Test
    @Ignore
    public void isIndexed() throws AvroRemoteException, InterruptedException {
        AvroClient client = new AvroClient();


        String host     = "localhost";
        Integer port    = 65115;

        while(!client.isIndexed()){
            LOG.info("waiting for index space...");
            Thread.sleep(2000);
        }

        client.close();
        LOG.info("completed!");
    }

    @Test
    @Ignore
    public void getSimilarPoints() throws IOException {
        AvroClient client = new AvroClient();


        //String host     = "librairy.linkeddata.es";
        String host     = "localhost";
        Integer port    = 65115;
        Integer max     = 10;

        client.open(host,port);

        LOG.info("getting similar points..");
        CSVReader reader = new CSVReader();
        reader.open();
        AtomicInteger counter =new AtomicInteger();
        while(true){
            try{
                DirichletDistribution point = reader.readLine();
                if (point.getVector().isEmpty()) continue;
                if (counter.incrementAndGet() > max) break;

                String cluster = ShapebyCluster.getSortedTopics(point.getVector(),0.5);

                LOG.info("Similar points in cluster: " + cluster);
                client.getNeighbours(point.getId(),10, Collections.emptyList(),true).forEach(sim -> LOG.info("\t " + sim));
            }catch (EndOfFileException e){
                break;
            }catch (Exception e){
                e.printStackTrace();
                break;
            }
        }
        client.close();
    }

    @Test
    @Ignore
    public void reindex() throws IOException, InterruptedException {
        AvroClient client = new AvroClient();


        //String host     = "librairy.linkeddata.es";
        String host     = "localhost";
        Integer port    = 65115;


        client.open(host,port);

        client.index(0.4);

        LOG.info("waiting for index space...");
        while(!client.isIndexed()){
            Thread.sleep(2000);
        }

        client.close();

    }

}