package org.librairy.service.space.data.access;

import com.datastax.driver.core.Session;
import com.google.common.primitives.Doubles;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.librairy.service.space.data.model.Cluster;
import org.librairy.service.space.data.model.Counter;
import org.librairy.service.space.data.model.ResultList;
import org.librairy.service.space.data.model.Space;
import org.librairy.service.space.facade.model.Stats;
import org.librairy.service.space.facade.model.Summary;
import org.librairy.service.space.services.SessionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
@DependsOn("dbSession")
public class SummaryDao extends Dao{

    private static final Logger LOG = LoggerFactory.getLogger(SummaryDao.class);

    @Autowired
    SessionService sessionService;

    @Autowired
    SpacesDao spacesDao;

    @Autowired
    CountersDao countersDao;

    @Autowired
    TypesDao typesDao;

    @Autowired
    ClustersDao clustersDao;

    private Session session;

    @PostConstruct
    public void setup(){
        session = sessionService.getSession();
    }

    @Override
    public String createTable() {
        return "";
    }

    public void prepareQueries(){
    }

    public Summary getSummary(){


        Summary summary = new Summary();
        summary.setThreshold(spacesDao.readThreshold(new Space().getId()));
        summary.setPoints(countersDao.get("points"));
        summary.setIndexes(countersDao.get("shapes"));
        summary.setDimensions(spacesDao.readDimensions(new Space().getId()));
        summary.setEdges(0l);

        // Types
        Optional<String> page = Optional.empty();
        Map<String,Long> types = new HashMap<>();
        while(true) {
            ResultList<Counter> result = typesDao.list(100, page);

            result.getValues().forEach( counter -> types.put(counter.getLabel(), counter.getSize()));

            if (result.getValues().isEmpty() || result.getValues().size()<100) break;

            page = Optional.of(result.getPage());
        }

        summary.setTypes(types);


        // Clusters
        page = Optional.empty();

        List<Double> values = new ArrayList<>();

        while(true){
            ResultList<Cluster> result = clustersDao.list(100, page);

            values.addAll(result.getValues().stream().map(counter -> Double.valueOf(counter.getSize())).collect(Collectors.toList()));

            if (result.getValues().isEmpty() || result.getValues().size()<100) break;

            page = Optional.of(result.getPage());
        }


        Stats stats = new Stats();
        if (!values.isEmpty()){
            double[] valuesArray = Doubles.toArray(values);
            StandardDeviation stdDev = new StandardDeviation();
            stats.setMin(Double.valueOf(StatUtils.min(valuesArray)).longValue());
            stats.setMax(Double.valueOf(StatUtils.max(valuesArray)).longValue());
            stats.setDev(stdDev.evaluate(valuesArray));
            stats.setMode(StatUtils.mode(valuesArray)[0]);
            stats.setMean(StatUtils.mean(valuesArray));
            stats.setMedian(StatUtils.geometricMean(valuesArray));
            stats.setVar(StatUtils.variance(valuesArray));
            stats.setTotal(Long.valueOf(values.size()));
        }
        summary.setClusters(stats);

        return summary;
    }

}
