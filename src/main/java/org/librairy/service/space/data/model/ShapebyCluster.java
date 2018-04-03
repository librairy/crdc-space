package org.librairy.service.space.data.model;

import groovy.lang.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
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

//    public static String getSortedTopics(List<Double> vector, Double threshold){
//        List<Tuple2<Integer, Double>> topics = IntStream
//                .range(0, vector.size())
//                .mapToObj(i -> new Tuple2<Integer, Double>(i, vector.get(i)))
//                .sorted((a, b) -> -a.getSecond().compareTo(b.getSecond()))
//                .collect(Collectors.toList());
//
//
//        String cluster = topics.subList(0,5).stream().map(t -> t.getFirst()).sorted((a,b)-> a.compareTo(b)).map(i -> String.valueOf(i)).collect(Collectors.joining("|"));
//
//        return cluster;
//
//    }

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

//    public static String getSortedTopics(List<Double> vector, Double threshold){
//        List<Tuple2<Integer, Double>> topics = IntStream
//                .range(0, vector.size())
//                .mapToObj(i -> new Tuple2<Integer, Double>(i, vector.get(i)))
//                .sorted((a, b) -> -a.getSecond().compareTo(b.getSecond()))
//                .collect(Collectors.toList());
//
//        Double accumulated = 0.0;
//
//        List<Integer> cluster = new ArrayList<>();
//        for(Tuple2<Integer, Double> topic : topics){
//
//            accumulated += topic.getSecond();
//            cluster.add(topic.getFirst());
//
//            if (accumulated >= threshold) break;
//
//        }
//        return cluster.stream().sorted((a,b)->a.compareTo(b)).map(i -> String.valueOf(i)).collect(Collectors.joining("|"));
//
//    }

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

    public static void main(String[] args){

        List<Double> v1 = Arrays.asList(new Double[]{0.5170500304375164,
                0.01599728924958733,
                0.02638541439612864,
                0.0036881811256474473,
                0.021333019486424104,
                0.0008083528013745158,
                0.032177209229131955,
                0.022963751949536707,
                0.02826552347508106,
                0.00225870342949005,
                6.794340373852136e-7,
                0.0004978508572178078,
                0.030798653606529357,
                0.00019711399002678422,
                0.29757822653226706});

        List<Double> v2_crdc = Arrays.asList(new Double[]{0.6179757832326896,
                0.0038015306943692978,
                0.00041240740992645725,
                0.0019404587011289864,
                0.01320176388286571,
                0.005093856378300684,
                0.005822975339701762,
                0.007494340535315106,
                0.0006709381423211175,
                0.002495360517002289,
                9.24236612025723e-7,
                0.0005951216127214837,
                0.0032377393169565373,
                0.00029711373045226855,
                0.3369596862696319});

        List<Double> v2_real = Arrays.asList(new Double[]{0.5097638135172098,
                0.014519262706005861,
                0.04145971609039952,
                0.016838104780385315,
                0.03844207694871062,
                0.002331776910440782,
                0.03702848059794992,
                0.024208399478070148,
                0.01469365286470953,
                0.0005980714782213713,
                8.128899710679357e-7,
                0.00023894070055268798,
                0.02533736336179171,
                0.0006343541634738655,
                0.2739051735121124});


        Double threshold = 0.9;

        System.out.println("Cluster of reference: " + getSortedTopics(v1,threshold));
        System.out.println("Cluster of CRDC: " + getSortedTopics(v2_crdc,threshold));
        System.out.println("Cluster of Real: " + getSortedTopics(v2_real,threshold));




    }
}
