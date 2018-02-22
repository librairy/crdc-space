/*
 * Copyright (c) 2017. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.service.space.services;

import com.fasterxml.jackson.annotation.JsonIgnore;
import groovy.lang.Tuple2;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class DirichletDistribution {

    private List<Double> vector;

    private String id;

    public DirichletDistribution(){}

    public DirichletDistribution(String id, Integer dimension){
        this.id = id;

        Random random = new Random();

        List<Integer> probabilities = IntStream.range(0, dimension).mapToObj(i -> random.nextInt(Double.valueOf(Math.pow(10,random.nextInt(3)+1)).intValue())+1).collect(Collectors.toList());

        Integer total = probabilities.stream().reduce((a,b) -> a+b).get();

        this.vector = probabilities.stream().map(val -> {
            double ratio = Double.valueOf(val) / Double.valueOf(total);

            if (ratio == 0.0){
                System.out.println("val:" + val + " / total: " + total);

            }

            return ratio;

        }).collect(Collectors.toList());
    }

    public DirichletDistribution(String id, List<Double> vector){
        this.id = id;
        this.vector = vector;
    }

    @JsonIgnore
    public Integer getHighestTopic(){
        return IntStream.range(0,vector.size())
                .reduce((a,b) -> (vector.get(a) > vector.get(b)? a : b))
                .getAsInt();
    }

    @JsonIgnore
    public Integer getLowestTopic(){
        return IntStream.range(0,vector.size())
                .reduce((a,b) -> (vector.get(a) < vector.get(b)? a : b))
                .getAsInt();
    }

    @JsonIgnore
    public String getSortedTopics(Integer top){
        return IntStream
                .range(0,vector.size())
                .mapToObj(i -> new Tuple2<Integer,Double>(i,vector.get(i)))
                .sorted( (a,b) -> -a.getSecond().compareTo(b.getSecond()))
                .map( t -> String.valueOf(t.getFirst()))
                .limit(top)
                .collect(Collectors.joining("|"));
    }

    @JsonIgnore
    public String getSortedTopics(Double threshold){
        List<Tuple2<Integer, Double>> topics = IntStream
                .range(0, vector.size())
                .mapToObj(i -> new Tuple2<Integer, Double>(i, vector.get(i)))
                .sorted((a, b) -> -a.getSecond().compareTo(b.getSecond()))
                .collect(Collectors.toList());

        Integer maxIndex = 0;
        Double accumulated = 0.0;
        for(Tuple2<Integer, Double> topic : topics){

            accumulated += topic.getSecond();
            maxIndex += 1;

            if (accumulated >= threshold) break;

        }
        return topics.stream()
                .map( t -> String.valueOf(t.getFirst()))
                .limit(maxIndex)
                .collect(Collectors.joining("|"));

    }

    @JsonIgnore
    public DoubleSummaryStatistics getStats(){
        return vector.stream().collect(DoubleSummaryStatistics::new, DoubleSummaryStatistics::accept, DoubleSummaryStatistics::combine);
    }

    public List<Double> getVector() {
        return vector;
    }

    public String getId() {
        return id;
    }
}
