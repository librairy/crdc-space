package org.librairy.service.space.data.access;

import com.datastax.driver.core.*;
import org.librairy.service.space.data.model.*;
import org.librairy.service.space.data.model.Cluster;
import org.librairy.service.space.facade.model.Point;
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
public class ClustersDao extends Dao{

    private static final Logger LOG = LoggerFactory.getLogger(ClustersDao.class);

    @Autowired
    SessionService sessionService;

    private Session session;

    private PreparedStatement incrementQuery;
    private PreparedStatement decrementQuery;
    private PreparedStatement readQuery;

    @PostConstruct
    public void setup(){
        session = sessionService.getSession();
    }

    @Override
    public String createTable() {
        return "CREATE TABLE if not exists space.clusters (" +
                "id text PRIMARY KEY, " +
                "value counter);";
    }

    public void prepareQueries(){
        incrementQuery  = session.prepare("update clusters set value = value + 1 where id = ?");
        decrementQuery  = session.prepare("update clusters set value = value - 1 where id = ?");
        readQuery       = session.prepare("select value from clusters where id = ?");
    }

    public void removeAll(){
        LOG.info("Deleting clusters");
        session.execute("truncate clusters;");
    }

    public void increment(String id){
        enqueue(session.executeAsync(incrementQuery.bind(id)));

    }

    public void decrement(String id){
        enqueue(session.executeAsync(decrementQuery.bind(id)));
    }

    public long get(String id){
        ResultSet result = session.execute(readQuery.bind(id));
        Row row = result.one();
        if (row != null) return row.getLong(0);
        return 0;
    }

    public ResultList<Cluster> list(Integer max, Optional<String> page){

        Statement statement = new SimpleStatement("select id, value from clusters;");
        statement.setFetchSize(max);
        if(page.isPresent()){
            statement.setPagingState(PagingState.fromString(page.get()));
        }

        ResultSet result = session.execute(statement);
        PagingState pagingState = result.getExecutionInfo().getPagingState();
        List<Row> rows = result.all();

        if (rows == null || rows.isEmpty()) return new ResultList<>(0l, Collections.emptyList(), pagingState!= null? pagingState.toString() : "");

        List<org.librairy.service.space.data.model.Cluster> values = rows.stream().limit(max).map(row -> new org.librairy.service.space.data.model.Cluster(row.getString(0),row.getLong(1))).collect(Collectors.toList());
        long total = 0l;
        return new ResultList(total,values, pagingState!= null? pagingState.toString() : "");
    }

}
