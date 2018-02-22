package org.librairy.service.space.metrics;

import org.junit.Test;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class CombinationsTest {

    @Test
    public void permutations(){

        Long n = 10l;
        Long k = 2l;

        Long c = Combinations.of(n,k);
        System.out.println(c);
    }
}
