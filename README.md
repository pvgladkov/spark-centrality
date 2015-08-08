# Spark Centrality Library

## Linking

Using SBT: `libraryDependencies += "cc.p2k" %% "spark-centrality" % "0.1"`

## Algorithms

### Harmonic Centrality

Harmonic centrality of a node x is the sum of the reciprocal of the shortest path distances from all other nodes to x.

Math formula:

![formula](http://upload.wikimedia.org/math/b/b/0/bb039f0850211e3f763c648178cb30b4.png)

It uses HyperLogLog algorithm.

Example:

```scala
val vertices: RDD[(Long, Double)] = sc.parallelize(Array(
  (1L, 1.0), (2L, 2.0), (3L, 3.0), (4L, 4.0)
))

val edges: RDD[Edge[Int]] = sc.parallelize(Array(
  Edge(1L, 2L, 1), Edge(2L, 3L, 1),
  Edge(2L, 1L, 1), Edge(3L, 2L, 1)
))

val graph = Graph(vertices, edges)

val hc = center.harmonicCentrality(graph)

println(hc.vertices.collect().mkString("\n"))

```

Output:

```
(4,0.0)
(1,1.50068)
(3,1.50068)
(2,2.00098)
```

links:

1. https://events.yandex.ru/lib/talks/1287/
2. http://infoscience.epfl.ch/record/200525/files/%5BEN%5DASNA09.pdf
3. https://en.wikipedia.org/wiki/Centrality
