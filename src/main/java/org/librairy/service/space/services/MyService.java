package org.librairy.service.space.services;

import com.google.common.base.Strings;
import org.apache.avro.AvroRemoteException;
import org.librairy.service.space.actions.IndexAction;
import org.librairy.service.space.data.access.CountersDao;
import org.librairy.service.space.data.access.PointsDao;
import org.librairy.service.space.data.access.ShapesDao;
import org.librairy.service.space.data.access.SpacesDao;
import org.librairy.service.space.data.model.Space;
import org.librairy.service.space.facade.model.Neighbour;
import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.facade.model.SpaceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class MyService implements SpaceService {

    private static final Logger LOG = LoggerFactory.getLogger(MyService.class);

    @Value("#{environment['RESOURCE_FOLDER']?:'${resource.folder}'}")
    String resourceFolder;

    @Autowired
    PointsDao pointsDao;

    @Autowired
    ShapesDao shapesDao;

    @Autowired
    SpacesDao spacesDao;

    @Autowired
    CountersDao countersDao;

    private Double threshold = -1.0;

    private ExecutorService executors;

    private boolean indexing = false;

    @PostConstruct
    public void setup() throws IOException {
        this.executors = Executors.newWorkStealingPool();
        LOG.info("Service initialized");
    }

    public SpacesDao getSpacesDao() {
        return spacesDao;
    }

    public PointsDao getPointsDao() {
        return pointsDao;
    }

    public ShapesDao getShapesDao() {
        return shapesDao;
    }

    public CountersDao getCountersDao() {
        return countersDao;
    }

    public ExecutorService getExecutors() {
        return executors;
    }

    public void initialize(){
        this.threshold = spacesDao.readThreshold(new Space().getId());
    }

    @Override
    public boolean addPoint(Point point) throws AvroRemoteException {
        pointsDao.save(point);
        return true;
    }

    @Override
    public Point getPoint(String pointId) throws AvroRemoteException {
        return pointsDao.read(pointId);
    }

    @Override
    public boolean removePoint(String pointId) throws AvroRemoteException {
        pointsDao.remove(pointId);
        //TODO remove from shapes
        return true;
    }

    @Override
    public boolean removeAll() throws AvroRemoteException {
        pointsDao.removeAll();
        return true;
    }

    @Override
    public List<Point> listPoints(int size, String offset) throws AvroRemoteException {
        Optional<String> offsetId = Strings.isNullOrEmpty(offset)? Optional.empty() : Optional.of(offset);
        return pointsDao.list(size,offsetId).getValues();
    }

    @Override
    public boolean index(double threshold) throws AvroRemoteException {
        if (isIndexing()) return false;
        this.threshold = threshold;
        setIndexing(true);
        executors.submit(new IndexAction(this,threshold));
        return true;
    }

    @Override
    public boolean isIndexed() throws AvroRemoteException {
        if (isIndexing()) return false;
        long shapes = countersDao.get("shapes");
        long points = countersDao.get("points");
        return shapes > 0 && points > 0 && shapes == points;
    }

    @Override
    public List<Neighbour> getNeighbours(String pointId, int max, String refType) throws AvroRemoteException {
        if (threshold < 0 || isIndexing()){
            LOG.warn("No space created yet");
            return Collections.emptyList();
        }
        Point point = pointsDao.read(pointId);
        List<Neighbour> res = getSimilar(point.getShape(), max + 1, refType);
        if (res.size() <= 1) return Collections.emptyList();
        return res.subList(1,res.size());
    }

    @Override
    public List<Neighbour> getSimilar(List<Double> shape, int max, String refType) throws AvroRemoteException {
        if (threshold < 0 || isIndexing()){
            LOG.warn("No space created yet");
            return Collections.emptyList();
        }
        return shapesDao.get(shape,threshold,max,Strings.isNullOrEmpty(refType)?Optional.empty(): Optional.of(refType));
    }

    public synchronized void setIndexing(Boolean status){
        this.indexing = status;
    }

    public synchronized Boolean isIndexing(){
        return this.indexing;
    }
}
