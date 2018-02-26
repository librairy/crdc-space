package org.librairy.service.space.metrics;

import org.apache.avro.AvroRemoteException;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.librairy.service.space.Application;
import org.librairy.service.space.data.model.ShapebyCluster;
import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.services.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@WebAppConfiguration
public class Evaluation {

    private static final Logger LOG = LoggerFactory.getLogger(Evaluation.class);

    Integer total = 10000;

    @Autowired
    MyService service;

    @Test
    @Ignore
    public void writePoints() throws IOException {
        Integer dimension   = 10;

        CSVWriter writer = new CSVWriter();

        writer.open();

        // Prepare points
        LOG.info("creating points..");
        Instant startModel          = Instant.now();
        IntStream.range(0, total).parallel().mapToObj(i -> new DirichletDistribution("doc" + i, dimension)).forEach(p -> writer.write(p));

        writer.close();

        Instant endModel    = Instant.now();
        LOG.info("All points ("+total+") created in: "
                + ChronoUnit.HOURS.between(startModel,endModel) + "hours "

                + ChronoUnit.MINUTES.between(startModel,endModel)%60 + "min "
                + ChronoUnit.SECONDS.between(startModel,endModel)%60 + "secs");


    }


    @Test
    @Ignore
    public void addPoints() throws InterruptedException, IOException {


        LOG.info("removing points..");
        service.removeAll();


        LOG.info("adding points..");
        CSVReader reader = new CSVReader();
        Instant startModel          = Instant.now();
        reader.open();
        AtomicInteger counter =new AtomicInteger();
        while(true){
            try{
                DirichletDistribution point = reader.readLine();
                if (point.getVector().isEmpty()){
                    LOG.warn("Vector is empty!!" + point.getId());
                    continue;
                }
                if (counter.incrementAndGet() > total) break;
                service.addPoint(Point.newBuilder().setId(point.getId()).setShape(point.getVector()).build());
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
        while(!service.isIndexed()){
            Thread.sleep(2000);
        }

        Instant endModel    = Instant.now();
        LOG.info("All points ("+counter.get()+") added and indexed in: "
                + ChronoUnit.HOURS.between(startModel,endModel) + "hours "

                + ChronoUnit.MINUTES.between(startModel,endModel)%60 + "min "
                + ChronoUnit.SECONDS.between(startModel,endModel)%60 + "secs");

        LOG.info("completed!");
    }


    public void reindex() throws AvroRemoteException, InterruptedException {
        service.index(0.8);
        LOG.info("waiting for finish...");
        while(!service.isIndexed()){
            Thread.sleep(5000);
        }
    }

    @Test
    @Ignore
    public void fmeasure() {

        Integer sampleSize = 100;
        Integer max = 10;

        Integer truePositive = 0;
        Integer trueNegative = 0;
        Integer falsePositive = 0;
        Integer falseNegative = 0;

        for (int i = 0; i < sampleSize; i++) {
            String id = "doc" + i;
            try {
                List<String> estimatedList = service.getNeighbours(id, max, Collections.emptyList(), false).stream().map(n -> n.getId()).collect(Collectors.toList());
                List<String> realList = service.getNeighbours(id, max, Collections.emptyList(), true).stream().map(n -> n.getId()).collect(Collectors.toList());

                LOG.info("evaluating point: " + i);

                truePositive += Long.valueOf(estimatedList.stream().filter(n -> realList.contains(n)).count()).intValue();
                falsePositive += Long.valueOf(estimatedList.stream().filter(n -> !realList.contains(n)).count()).intValue();
                falseNegative += Long.valueOf(realList.stream().filter(n -> !estimatedList.contains(n)).count()).intValue();
                trueNegative += total - (truePositive + falseNegative + falsePositive);

            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }
        }

        Double precision    = (Double.valueOf(truePositive) + Double.valueOf(falsePositive)) == 0.0? 0.0 : Double.valueOf(truePositive) / (Double.valueOf(truePositive) + Double.valueOf(falsePositive));
        Double recall       = (Double.valueOf(truePositive) + Double.valueOf(falseNegative)) == 0.0? 0.0 : Double.valueOf(truePositive) / (Double.valueOf(truePositive) + Double.valueOf(falseNegative));
        Double fmeasure     = (precision+recall == 0.0)? 0.0 : 2* (precision*recall)/(precision+recall);

        LOG.info("Precision: "  + precision);
        LOG.info("Recall: "     + recall);
        LOG.info("FMeasure: "   + fmeasure);

    }

    @Test
    @Ignore
    public void clusterLabel(){

        for(int i=0; i<10;i++){
            DirichletDistribution d = new DirichletDistribution("id", 10);

            String cluster = ShapebyCluster.getSortedTopics(d.getVector(), 0.9);

            LOG.info("Vector: [" + cluster + "] " + d.getVector());
        }

    }
}
