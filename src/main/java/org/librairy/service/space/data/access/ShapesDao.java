package org.librairy.service.space.data.access;

import com.datastax.driver.core.*;
import com.google.common.base.Strings;
import org.librairy.service.space.data.model.ResultList;
import org.librairy.service.space.data.model.ShapebyCluster;
import org.librairy.service.space.facade.model.Neighbour;
import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.metrics.JensenShannonSimilarity;
import org.librairy.service.space.services.SessionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
@DependsOn("dbSession")
public class ShapesDao extends Dao {

    private static final Logger LOG = LoggerFactory.getLogger(ShapesDao.class);

    @Autowired
    SessionService sessionService;

    @Autowired
    CountersDao countersDao;

    @Autowired
    ClustersDao clustersDao;

    private Session session;

    private PreparedStatement saveQuery;
    private PreparedStatement deleteQuery;

    @PostConstruct
    public void setup(){
        session = sessionService.getSession();
    }

    @Override
    public String createTable() {
        return "CREATE TABLE if not exists space.shapes (" +
                "cluster text, " +
                "type text, " +
                "id text, " +
                "name text, " +
                "shape list<double>, " +
                "PRIMARY KEY (cluster, type, id));";
    }

    public void prepareQueries(){
        saveQuery   = session.prepare("insert into shapes (cluster, type, id, shape) values (?, ?, ?, ?)");
        deleteQuery = session.prepare("delete from shapes where cluster = ? and type = ? and id = ?");
    }

    public ShapebyCluster save(Point point, Double threshold){

        ShapebyCluster shapebyCluster = new ShapebyCluster(point.getType(),point.getId(), point.getName(),point.getShape(),threshold);

        BoundStatement statement = saveQuery.bind(
                shapebyCluster.getCluster(),
                shapebyCluster.getType(),
                shapebyCluster.getId(),
                shapebyCluster.getShape());
        statement.setIdempotent(true);
//        enqueue(session.executeAsync(statement));
        session.execute(statement);
        countersDao.increment("shapes");
        clustersDao.increment(shapebyCluster.getCluster());

        return shapebyCluster;
    }


    public void removeAll(){
        LOG.info("Deleting shapes..");
        session.execute("truncate shapes;");
    }


    public void remove(Point point,Double threshold){

        String cluster  = ShapebyCluster.getSortedTopics(point.getShape(),threshold);
        String type     = point.getType();
        String id       = point.getId();

        enqueue(session.executeAsync(deleteQuery.bind(cluster, type, id)));
        clustersDao.decrement(ShapebyCluster.getSortedTopics(point.getShape(),threshold));
    }

    public ResultList<Point> list(String cluster, Integer max, Optional<String> page){

        Statement statement = new SimpleStatement("select id, name, type, shape from shapes where cluster='"+cluster+"';");
        statement.setFetchSize(max);
        if(page.isPresent()){
            statement.setPagingState(PagingState.fromString(page.get()));
        }

        ResultSet result = session.execute(statement);
        PagingState pagingState = result.getExecutionInfo().getPagingState();
        List<Row> rows = result.all();

        if (rows == null || rows.isEmpty()) return new ResultList<>(Collections.emptyList(),pagingState!= null? pagingState.toString() : "");

        List<Point> values = rows.stream().limit(max).map(row -> pointFrom(row)).collect(Collectors.toList());
        return new ResultList<>(values,pagingState!= null? pagingState.toString() : "");
    }


    public List<Neighbour> get(List<Double> shape, Double threshold, Integer max, List<String> types){


        String cluster = ShapebyCluster.getSortedTopics(shape, threshold);

        List<Neighbour> neigbours = new ArrayList<>();
        Statement statement = new SimpleStatement("select id, name, type, shape from shapes where cluster='"+cluster+"';");
        statement.setFetchSize(500);
        ResultSet rs = session.execute(statement);

        for(Row row: rs){
            if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
                rs.fetchMoreResults();
            String id   = row.getString(0);
            String name = row.getString(1);
            String type = row.getString(2);
            if (types.isEmpty()|| (types.contains(row.getString(2)))) neigbours.add(new Neighbour(id, Strings.isNullOrEmpty(name)?"":name, Strings.isNullOrEmpty(type)?"":type, JensenShannonSimilarity.calculate(shape,row.getList(3,Double.class))));

        }

        return neigbours.stream().limit(max).sorted((a,b)-> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());

    }


    private Point pointFrom(Row row){
        return new Point(row.getString(0),row.getString(1),row.getString(2),row.getList(3,Double.class));
    }

}
