package cc.p2k.spark.graphx

import cc.p2k.spark.graphx.lib.Diameter
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class DiameterTest extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf().setAppName("Spark").setMaster("local")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  override def afterAll(): Unit ={
    this.sc.stop()
  }

  test("dia_1") {

    val vertices: RDD[(Long, Double)] = sc.parallelize(Array(
      (1L, 1.0), (2L, 2.0), (3L, 3.0)
    ))

    val edges: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 1), Edge(2L, 3L, 1),
      Edge(2L, 1L, 1), Edge(3L, 2L, 1)
    ))

    val graph = Graph(vertices, edges)

    assert(2 == Diameter(graph))
  }

  test("dia_2") {

    val vertices: RDD[(Long, Double)] = sc.parallelize(Array(
      (1L, 1.0), (2L, 2.0), (3L, 3.0), (4L, 1.0), (5L, 1.0)
    ))

    val edges: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(2L, 4L, 1), Edge(3L, 5L, 1),
      Edge(2L, 1L, 1), Edge(3L, 2L, 1), Edge(4L, 2L, 1), Edge(5L, 3L, 1)
    ))

    val graph = Graph(vertices, edges)

    assert(3 == Diameter(graph))
  }

}
