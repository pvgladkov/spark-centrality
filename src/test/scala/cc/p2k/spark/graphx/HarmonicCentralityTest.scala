package cc.p2k.spark.graphx

import cc.p2k.spark.graphx.lib.HarmonicCentrality
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class HarmonicCentralityTest extends FunSuite {

  test("1") {

    val conf = new SparkConf().setAppName("Spark").setMaster("local")
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
    val center = HarmonicCentrality

    val valid = Map(1->1.5, 2->2.0, 3->1.5, 4->0)
    val hc = center.harmonicCentrality(graph)

    for ((k,v) <- hc.vertices.collect().iterator){
      assert(
        BigDecimal(v).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble == valid(k.toInt)
      )
    }
  }

}
