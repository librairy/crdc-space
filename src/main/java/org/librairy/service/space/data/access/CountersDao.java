package org.librairy.service.space.data.access;

import com.datastax.driver.core.*;
import org.librairy.service.space.services.SessionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
@DependsOn("dbSession")
public class CountersDao extends Dao{

    private static final Logger LOG = LoggerFactory.getLogger(CountersDao.class);

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
        return "CREATE TABLE if not exists space.counters (" +
                "id text PRIMARY KEY, " +
                "value counter);";
    }

    public void prepareQueries(){
        incrementQuery  = session.prepare("update counters set value = value + 1 where id = ?");
        decrementQuery  = session.prepare("update counters set value = value - 1 where id = ?");
        readQuery       = session.prepare("select value from counters where id = ?");
    }

    public void removeAll(){
        LOG.info("Deleting counters");
        session.execute("truncate counters;");
    }

    public void remove(String id){
        enqueue(session.executeAsync("delete from counters where id='"+id+"';"));
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

}
