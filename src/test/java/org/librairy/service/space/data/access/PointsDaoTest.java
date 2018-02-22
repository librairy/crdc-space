package org.librairy.service.space.data.access;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.librairy.service.space.Application;
import org.librairy.service.space.data.model.ResultList;
import org.librairy.service.space.facade.model.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@WebAppConfiguration
public class PointsDaoTest {


    private static final Logger LOG = LoggerFactory.getLogger(PointsDaoTest.class);

    @Autowired
    PointsDao pointsDao;

    @Test
    @Ignore
    public void paginatedList() throws InterruptedException {

        pointsDao.removeAll();

        List<Point> points = IntStream.range(0, 5000).mapToObj(i -> Point.newBuilder().setId("id" + i).setShape(Arrays.asList(new Double[]{0.2, 0.3})).build()).collect(Collectors.toList());


        points.parallelStream().forEach(p -> pointsDao.save(p));

        Thread.sleep(2000);

        Integer max = 500;
        Optional<String> page = Optional.empty();
        AtomicInteger counter = new AtomicInteger(0);

        while(true){
            ResultList<Point> pointsReturned = pointsDao.list(max, page);
            Integer count = pointsReturned.getValues().size();
            LOG.info("["+counter.getAndIncrement()+"] "+ count + " points returned!");
            if (count<max) break;

            page = Optional.of(pointsReturned.getPage());
        }

    }
}
