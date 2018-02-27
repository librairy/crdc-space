package org.librairy.service.space.services;

import com.google.common.base.Strings;
import org.apache.avro.AvroRemoteException;
import org.librairy.service.space.actions.AddPointAction;
import org.librairy.service.space.actions.IndexAction;
import org.librairy.service.space.data.access.*;
import org.librairy.service.space.data.model.ResultList;
import org.librairy.service.space.data.model.Space;
import org.librairy.service.space.facade.model.Neighbour;
import org.librairy.service.space.facade.model.Point;
import org.librairy.service.space.facade.model.PointList;
import org.librairy.service.space.facade.model.SpaceService;
import org.librairy.service.space.metrics.JensenShannonSimilarity;
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
import java.util.stream.Collectors;

@Component
public class MyService implements SpaceService, BootService {

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

    @Autowired
    ClustersDao clustersDao;

    private Double threshold = -1.0;

    private ExecutorService executors;

    private Boolean indexing = false;

    @PostConstruct
    public void setup() throws IOException {
        this.executors = Executors.newWorkStealingPool();
        LOG.info("Service initialized");
    }

    public ClustersDao getClustersDao() {
        return clustersDao;
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

    @Override
    public boolean prepare() {
        Double dbThreshold = spacesDao.readThreshold(new Space().getId());
        this.threshold = dbThreshold >= 0.0? dbThreshold : 0.8;
        return true;
    }

    @Override
    public boolean addPoint(Point point) throws AvroRemoteException {
        executors.submit(new AddPointAction(this,point,threshold));
        return true;
    }

    @Override
    public Point getPoint(String pointId) throws AvroRemoteException {
        return pointsDao.read(pointId);
    }

    @Override
    public boolean removePoint(String pointId) throws AvroRemoteException {
        try{
            Point point = pointsDao.read(pointId);
            pointsDao.remove(pointId);
            shapesDao.remove(point,threshold);
            return true;
        }catch (RuntimeException e){
            LOG.warn("Point not found by id: " + pointId);
            return false;
        }
    }

    @Override
    public boolean removeAll() throws AvroRemoteException {
        pointsDao.removeAll();
        return true;
    }

    @Override
    public PointList listPoints(int size, String offset) throws AvroRemoteException {
        Optional<String> offsetId = Strings.isNullOrEmpty(offset)? Optional.empty() : Optional.of(offset);
        ResultList<Point> resultList = pointsDao.list(size, offsetId);
        return new PointList(resultList.getPage(), resultList.getValues());
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
    public double compare(List<Double> v1, List<Double> v2) throws AvroRemoteException {
        if (v1.isEmpty() || v2.isEmpty() || v1.size() != v2.size()) return 0.0;
        return JensenShannonSimilarity.calculate(v1,v2);
    }

    @Override
    public List<Neighbour> getNeighbours(String pointId, int max, List<String> types, boolean force) throws AvroRemoteException {
        if (threshold < 0 || isIndexing()){
            LOG.warn("No space created yet");
            return Collections.emptyList();
        }
        Point point = pointsDao.read(pointId);
        List<Neighbour> res = getSimilar(point.getShape(), max + 1, types,force);
        if (res.size() <= 1) return Collections.emptyList();
        return res.stream().filter(n -> !n.getId().equalsIgnoreCase(point.getId())).collect(Collectors.toList());
    }

    @Override
    public List<Neighbour> getSimilar(List<Double> shape, int max, List<String> types, boolean force) throws AvroRemoteException {
        if (threshold < 0 || isIndexing()){
            LOG.warn("No space created yet");
            return Collections.emptyList();
        }
        if (!force)
            return shapesDao.get(shape,threshold,max,types);

        return pointsDao.compareAll(shape,max,types);
    }

    public synchronized void setIndexing(Boolean status){
        this.indexing = status;
    }

    public synchronized Boolean isIndexing(){
        return this.indexing;
    }


}
