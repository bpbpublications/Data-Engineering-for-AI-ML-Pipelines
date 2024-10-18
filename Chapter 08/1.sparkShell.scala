// Example of using the Spark Shell
// Let's try a simple Scala code to create an RDD and perform a transformationval data = Array(1, 2, 3, 4, 5)

val rdd = sc.parallelize(data)

val doubledRdd = rdd.map(x => x * 2)

doubledRdd.collect().foreach(println)
