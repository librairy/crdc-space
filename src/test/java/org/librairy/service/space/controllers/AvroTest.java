package org.librairy.service.space.controllers;


import org.apache.avro.AvroRemoteException;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.librairy.service.space.Application;
import org.librairy.service.space.AvroClient;
import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.services.DirichletDistribution;
import org.librairy.service.space.services.MyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {Application.class})
@WebAppConfiguration
public class AvroTest {

    private static final Logger LOG = LoggerFactory.getLogger(AvroTest.class);

    @Test
//    @Ignore
    public void addTest() throws InterruptedException, IOException {

        AvroClient client = new AvroClient();


        String host     = "localhost";
        Integer port    = 65115;

        Integer numVectors  = 100000;
        Integer dimension   = 100;
        Double threshold    = 0.8;
        client.open(host,port);



        // Prepare points
        LOG.info("adding points..");
        List<Point> points = IntStream.range(0, numVectors).mapToObj(i -> new DirichletDistribution("doc" + i, dimension)).map(d -> Point.newBuilder().setId(d.getId()).setName("document-" + d.getId()).setShape(d.getVector()).build()).collect(Collectors.toList());


        // Add points
        points.forEach(p -> {
            try {
                client.add(p);
            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }
        });

        // Create a space
        try {
            client.index(threshold);
            LOG.info("Space request sent!");
        } catch (AvroRemoteException e) {
            e.printStackTrace();
        }



        while(!client.isIndexed()){
            LOG.info("waiting for index space...");
            Thread.sleep(2000);
        }

        client.close();
        LOG.info("completed!");
    }

    @Test
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

}