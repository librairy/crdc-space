package org.librairy.service.space.data.model;

import groovy.lang.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class ShapebyCluster {

    private String cluster;

    private String type;

    private String id;

    private String name;

    private List<Double> shape;

    public ShapebyCluster(String type, String id, String name, List<Double> shape, Double threshold) {
        this.cluster = getSortedTopics(shape,threshold);
        this.type = type;
        this.id = id;
        this.name = name;
        this.shape = shape;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Double> getShape() {
        return shape;
    }

    public void setShape(List<Double> shape) {
        this.shape = shape;
    }

    public static String getSortedTopics(List<Double> vector, Double threshold){
        List<Tuple2<Integer, Double>> topics = IntStream
                .range(0, vector.size())
                .mapToObj(i -> new Tuple2<Integer, Double>(i, vector.get(i)))
                .sorted((a, b) -> -a.getSecond().compareTo(b.getSecond()))
                .collect(Collectors.toList());

        Double accumulated = 0.0;

        StringBuilder cluster = new StringBuilder();
        for(Tuple2<Integer, Double> topic : topics){

            accumulated += topic.getSecond();
            cluster.append(topic.getFirst()).append("|");

            if (accumulated >= threshold) break;

        }
        return cluster.toString();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShapebyCluster that = (ShapebyCluster) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "ShapebyCluster{" +
                "cluster='" + cluster + '\'' +
                ", type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", shape=" + shape +
                '}';
    }
}
