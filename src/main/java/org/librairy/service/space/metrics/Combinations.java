package org.librairy.service.space.metrics;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class Combinations {

    public static Long of(Long n, Long k){
        return permutation(n) / (permutation(k) * permutation(n - k));
    }

    public static Long permutation(Long i)
    {
        if (i == 1)
        {
            return 1l;
        }
        return i * permutation(i - 1);
    }
}
