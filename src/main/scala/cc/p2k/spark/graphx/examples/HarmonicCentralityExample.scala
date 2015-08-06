package cc.p2k.spark.graphx.examples

import cc.p2k.spark.graphx.lib.HarmonicCentrality
import cc.p2k.spark.graphx.lib.HarmonicCentrality.NMap
import com.twitter.algebird.{HyperLogLogMonoid, HLL}
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

    sc.setLogLevel("WARN")

    val vertices: RDD[(Long, Double)] = sc.parallelize(Array(
      (1L, 1.0), (2L, 2.0), (3L, 3.0), (4L, 4.0)
    ))

    val edges: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 1), Edge(2L, 3L, 1),
      Edge(2L, 1L, 1), Edge(3L, 2L, 1)
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

    val center = HarmonicCentrality

    println((1L, center.personalizedHarmonicCentrality(1L, graph)))
    println((2L, center.personalizedHarmonicCentrality(2L, graph)))
    println((3L, center.personalizedHarmonicCentrality(3L, graph)))

    val hc = center.harmonicCentrality(graph)

    println(hc.vertices.collect().mkString("\n"))

  }
}
