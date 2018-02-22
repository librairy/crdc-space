package org.librairy.service.space.metrics;

import org.junit.Assert;
import org.junit.Test;
import org.librairy.service.space.services.DirichletDistribution;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class JensenShannonSimilarityTest {

    @Test
    public void sameVector(){
        DirichletDistribution d1 = new DirichletDistribution("id1",10);
        DirichletDistribution d2 = new DirichletDistribution("id2",d1.getVector());

        double score = JensenShannonSimilarity.calculate(d1.getVector(), d2.getVector());
        Assert.assertEquals(1.0,score,0.0);

        double score2 = JensenShannonSimilarity.calculate(d2.getVector(), d1.getVector());
        Assert.assertEquals(1.0,score2,0.0);
    }
}
