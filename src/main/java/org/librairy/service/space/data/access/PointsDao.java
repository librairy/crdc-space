package org.librairy.service.space.data.access;

import com.datastax.driver.core.*;
import org.librairy.service.space.data.model.ResultList;
import org.librairy.service.space.data.model.ShapebyCluster;
import org.librairy.service.space.data.model.Space;
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
public class PointsDao extends Dao{

    private static final Logger LOG = LoggerFactory.getLogger(PointsDao.class);

    @Autowired
    SessionService sessionService;

    private Session session;

    @Autowired
    ShapesDao shapesDao;

    @Autowired
    CountersDao countersDao;

    @Autowired
    TypesDao typesDao;

    @Autowired
    ClustersDao clustersDao;

    @Autowired
    SpacesDao spacesDao;

    private PreparedStatement insertQuery;
    private PreparedStatement readQuery;
    private PreparedStatement deleteQuery;



    @PostConstruct
    public void setup(){
        session = sessionService.getSession();
    }

    @Override
    public String createTable() {
        return "CREATE TABLE if not exists space.points (" +
                "id text, " +
                "name text, " +
                "description text, " +
                "type text, " +
                "shape list<double>, " +
                "PRIMARY KEY (id));";
    }

    public Session getSession() {
        return session;
    }

    public void prepareQueries(){
        insertQuery = session.prepare("insert into points (id, name, type, shape) values (?, ?, ?, ?)");
        readQuery   = session.prepare("select id, name, type, shape from points where id =?");
        deleteQuery = session.prepare("delete from points where id =? if exists");
    }

    public void save(Point point) {
        BoundStatement statement = insertQuery.bind(
                point.getId(),
                point.getName(),
                point.getType(),
                point.getShape());
        statement.setIdempotent(true);//for retries
//        enqueue(session.executeAsync(statement));
        session.execute(statement);
        countersDao.increment("points");
        typesDao.increment(point.getType());
    }

    public Point read(String id){
        ResultSet result = session.execute(readQuery.bind(id));
        Row row = result.one();
        if (row == null) throw new RuntimeException("No point found by id: " + id);
        return pointFrom(row);
    }

    public void remove(String id){
        try{
            Point point = read(id);
            enqueue(session.executeAsync(deleteQuery.bind(id)));
            countersDao.decrement("points");
            typesDao.decrement(point.getType());
        }catch (RuntimeException e){
            LOG.warn("No point found by id:'"+id+"'");
        }
    }

    public void removeAll(){
        session.execute("truncate points;");
        shapesDao.removeAll();
        countersDao.removeAll();
        clustersDao.removeAll();
        typesDao.removeAll();
        spacesDao.removeAll();
    }

    public ResultList<Point> list(Integer max, Optional<String> page){

        Statement statement = new SimpleStatement("select id, name, type, shape from points;");
        statement.setFetchSize(max);
        if(page.isPresent()){
            statement.setPagingState(PagingState.fromString(page.get()));
        }

        ResultSet result = session.execute(statement);
        PagingState pagingState = result.getExecutionInfo().getPagingState();
        List<Row> rows = result.all();

        if (rows == null || rows.isEmpty()) return new ResultList<>(0l,Collections.emptyList(), pagingState!= null? pagingState.toString() : "");

        List<Point> values = rows.stream().limit(max).map(row -> pointFrom(row)).collect(Collectors.toList());
        long total = countersDao.get("points");
        return new ResultList<>(total,values, pagingState!= null? pagingState.toString() : "");
    }


    public List<Neighbour> compareAll(List<Double> shape, Integer max, List<String> types){
        Statement statement = new SimpleStatement("select id, name, type, shape from points;");
        statement.setFetchSize(1000);

        List<Neighbour> similarPoints = new ArrayList<>();

        Double minValue     = 0.0;

        ResultSet rs = session.execute(statement);
        for (Row row : rs) {
            if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
                rs.fetchMoreResults(); // this is asynchronous

            // filter by type
            if (!types.isEmpty() && !types.contains(row.getString(2))) continue;

            // Process the row ...
            Point point = pointFrom(row);

            Double score = JensenShannonSimilarity.calculate(shape,point.getShape());
            if (similarPoints.size() < max){
                similarPoints.add(Neighbour.newBuilder().setId(point.getId()).setName(point.getName()).setScore(score).setType(point.getType()).build());
            }
            else if (score > minValue) {
                similarPoints.remove(max-1);
                similarPoints.add(Neighbour.newBuilder().setId(point.getId()).setName(point.getName()).setScore(score).setType(point.getType()).build());
                similarPoints.sort((a,b) -> -a.getScore().compareTo(b.getScore()));
                minValue = similarPoints.get(max-1).getScore();
            }


        }

        return similarPoints.stream().sorted((a,b) -> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());


    }

    public Point pointFrom(Row row){
        Point point = new Point();
        point.setId(row.getString(0));
        point.setName(row.getString(1));
        point.setType(row.getString(2));
        point.setShape(row.getList(3, Double.class));
        return point;
    }

}
