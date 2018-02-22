package org.librairy.service.space.metrics;

import cc.mallet.util.Maths;
import com.google.common.primitives.Doubles;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class JensenShannonSimilarity {

    public static double calculate(List<Double> v1, List<Double> v2){
        return 1- Maths.jensenShannonDivergence(Doubles.toArray(v1),Doubles.toArray(v2));
    }
}
