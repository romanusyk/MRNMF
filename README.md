# MapReduce Nonnegative Matrix Factorizations (MRNMF)
Zhengguo Sun, Tao Li, and Naphtali Rishe 

This code provides a MapReduce implementation of the large-scale nonnegative matrix factorizations described in [Large-Scale Matrix Factorization using MapReduce](http://cake.fiu.edu/Publications/Sun+al-10-LM.Large-Scale.Matrix.Factorization.using.MapReduce.ICDMW2010.published.paper.pdf). The implementation uses Hadoop with Java.

This code implements NMF algorithm from paper above.

## Usage

With input martix
```
hadoop jar MRNMF.jar MRNMF -i input.txt -o output -t mrnmf -n 100 -m 20 -k 8 -it 3 -r 5
```

With random martix
```
hadoop jar MRNMF.jar MRNMF -s 0.3 -o output -t mrnmf -n 100 -m 20 -k 8 -it 3 -r 5
```

# **Options**
    -h           print help for this application
    -i           path to the input matrix file
    -s           sparsity of random generated matrix
    -o           path to the output directory
    -t           path to the temporary directory
    -n           matrix row number
    -m           matrix column number
    -it          iterations number
    -k           the dimension number to be reduced to
    -r           the range value for the matrix elements
    
  ## Examples
  
  See [Examples](https://github.com/Romm17/MRNMF/tree/master/examples) for details
