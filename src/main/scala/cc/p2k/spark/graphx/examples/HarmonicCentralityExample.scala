package cc.p2k.spark.graphx.examples

import cc.p2k.spark.graphx.lib.HarmonicCentrality
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._



object HarmonicCentralityExample {


  def vertexNeighbours[VD, ED](graph: Graph[VD, ED]): VertexRDD[Int] = {
    graph.aggregateMessages[Int](
      triplet => {
        triplet.sendToDst(1)
      },
      (a, b) => a + b
    )
  }

//	def shortestPath[VD, ED](sourceId: Int, graph: Graph[VD, ED]): Unit = {
//		val g = graph.mapVertices( (id, _) =>
//			if (id == sourceId){
//				0.0
//			}
//			else {
//				Double.PositiveInfinity
//			}
//		)
//
//		val sssp = g.pregel(Double.PositiveInfinity)(
//			(id, dist, newDist) => math.min(dist, newDist),
//			triplet => {
//				if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//					Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
//				}
//				else {
//					Iterator.empty
//				}
//			},
//			(a, b) => math.min(a, b)
//		)
//	}

  def main(args: Array[String]): Unit =	{
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)

    val vertices: RDD[(Long, Double)] = sc.parallelize(Array(
      (1L, 1.toDouble), (2L, 2.toDouble), (3L, 3.toDouble), (4L, 4.toDouble)
    ))

    val edges: RDD[Edge[Double]] = sc.parallelize(Array(
      Edge(1L, 2L, 1.toDouble), Edge(2L, 3L, 1.toDouble),
      Edge(2L, 1L, 1.toDouble), Edge(3L, 2L, 1.toDouble)
    ))

    val graph = Graph(vertices, edges)

//		val ranks = graph.pageRank(0.0001).vertices.collect()
//		val ranks_1 = graph.pageRank(0.0001).edges.collect()
//
//		for (f<-ranks){
//			println(f.toString())
//		}
//
//		for (f<-ranks_1){
//			println(f.toString())
//		}

    val neighbors = vertexNeighbours(graph).collect()

    for (n<-neighbors){
      println(n.toString())
    }

    val sourceId = 1L
    val center = HarmonicCentrality
    val hr = center.personalizedHarmonicCentrality(sourceId, graph)

    println(hr)

    val hc = center.harmonicCentrality(graph)

    println(hc.vertices.collect().mkString("\n"))
    println(hc.edges.collect().mkString("\n"))

    sc.stop()
  }
}
