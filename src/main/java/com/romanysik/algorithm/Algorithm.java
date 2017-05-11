package com.romanysik.algorithm;

import java.io.IOException;

/**
 * Common interface for matrix factorization algorithms
 *
 * Created by romm on 06.05.17.
 */
public interface Algorithm {

    /**
     * Compute matrix factorization
     * of given matrix X with shape (n, m)
     *
     * @param iterations - num iterations
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public void compute(int iterations) throws IOException, InterruptedException, ClassNotFoundException;

}
