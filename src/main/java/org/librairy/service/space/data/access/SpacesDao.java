package org.librairy.service.space.data.access;

import com.datastax.driver.core.*;
import org.librairy.service.space.data.model.ResultList;
import org.librairy.service.space.data.model.Space;
import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.services.BootService;
import org.librairy.service.space.services.SessionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
@DependsOn("dbSession")
public class SpacesDao extends Dao implements BootService{

    private static final Logger LOG = LoggerFactory.getLogger(SpacesDao.class);

    @Autowired
    SessionService sessionService;

    private Session session;

    private PreparedStatement insertQuery;
    private PreparedStatement readQuery;
    private PreparedStatement readThresholdQuery;
    private PreparedStatement deleteQuery;

    private SimpleDateFormat dateFormatter      = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");



    @PostConstruct
    public void setup(){
        session = sessionService.getSession();
    }

    @Override
    public String createTable() {
        return "CREATE TABLE if not exists space.spaces (" +
                "id text, " +
                "name text, " +
                "threshold double, " +
                "date text, " +
                "PRIMARY KEY (id));";
    }

    @Override
    public boolean prepare() {
        Space defaultSpace = new Space();
        defaultSpace.setName("initial-space");
        defaultSpace.setThreshold(0.8);
        defaultSpace.setDate(dateFormatter.format(new Date()));

        save(defaultSpace);
        return true;
    }


    public void prepareQueries(){
        insertQuery         = session.prepare("insert into spaces (id, name, threshold, date) values (?, ?, ?, ?)");
        readThresholdQuery  = session.prepare("select threshold from spaces where id=?");
        readQuery           = session.prepare("select id, name, threshold, date from spaces where id=?");
        deleteQuery         = session.prepare("delete from spaces where id=?");
    }

    public void save(Space space){
        enqueue(session.executeAsync(insertQuery.bind(
                    space.getId(),
                    space.getName(),
                    space.getThreshold(),
                    space.getDate()
                )));


    }

    public Double readThreshold(String id){
        ResultSet result = session.execute(readThresholdQuery.bind(id));
        Row row = result.one();
        if (row == null) return -1.0;
        return row.getDouble(0);
    }

    public Space read(String id){
        ResultSet result = session.execute(readQuery.bind(id));
        Row row = result.one();
        if (row == null) throw new RuntimeException("No space found by id: " + id);
        return spaceFrom(row);
    }

    public void remove(String id){
        enqueue(session.executeAsync(deleteQuery.bind(id)));
    }

    public void removeAll(){
        session.execute("truncate spaces;");
    }



    private Space spaceFrom(Row row){
        Space space = new Space();
        space.setName(row.getString(1));
        space.setThreshold(row.getDouble(2));
        space.setDate(row.getString(3));
        return space;
    }


}
