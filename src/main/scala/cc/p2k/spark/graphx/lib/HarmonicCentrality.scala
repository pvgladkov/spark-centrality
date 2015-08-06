package cc.p2k.spark.graphx.lib

import org.apache.spark.Logging
import org.apache.spark.graphx._
import collection.immutable
import scala.language.postfixOps
import com.twitter.algebird._


object HarmonicCentrality extends Logging {

  /** маппинг соседей по расстоянию */
  type NMap = immutable.Map[Int, HLL]

  val BIT_SIZE = 12

  /**
   * Harmonic Centrality for node
   * @param vertexId VertexId
   * @param graph Graph
   * @return
   */
  def personalizedHarmonicCentrality(vertexId: VertexId, graph: Graph[Double, Double]): Double = {
    val initialGraph = graph.mapVertices(
      (id, _) => {
        if (id == vertexId) 0.0
        else Double.PositiveInfinity
      }
    )

    val vp = (id: VertexId, dist: Double, newDist: Double) => math.min(dist, newDist)

    def mergeMsg(a: Double, b: Double) = math.min(a, b)

    def sendMsg(triplet: EdgeTriplet[Double, Double]) = {
      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    }

    val paths = initialGraph.pregel(Double.PositiveInfinity)(vp, sendMsg, mergeMsg)

    val hr = paths.vertices
      .map[Double](
        tuple => {
          if (tuple._1 == vertexId) 0
          else 1 / tuple._2
        }
      )
      .reduce((a, b) => a + b)

    hr
  }

  /**
   * @param graph Graph
   * @param maxDistance Int
   * @return
   */
  def harmonicCentrality(graph: Graph[Double, Int], maxDistance: Int = 6): Graph[NMap, Int] = {

    val hll = new HyperLogLogMonoid(BIT_SIZE)

    val initGraph: Graph[NMap, Int] = graph.mapVertices(
      (id: VertexId, v: Double) => immutable.Map[Int, HLL](
        (0, hll.create(id.toString.getBytes))
      )
    )

    val initMessage = immutable.Map[Int, HLL](0 -> new HyperLogLogMonoid(BIT_SIZE).zero)

    def incrementNMap(p: NMap): NMap = p.filterKeys(_ < maxDistance).map {
      case (v, d) => (v + 1) -> d
    }

    /**
     * @param x VertexId
     * @param vertexValue Double VD
     * @param message Double message type
     * @return VD
     */
    def vertexProgram(x: VertexId, vertexValue: NMap, message: NMap) = {
      println(x + " rec " + message)
      println(x + " has val: " + vertexValue)

      addMaps(vertexValue, message)
    }

    /**
     * @param edge EdgeTriplet  EdgeTriplet[VD, ED]
     * @return Iterator[(VertexId, A)]
     */
    def sendMessage(edge: EdgeTriplet[NMap, Int]): Iterator[(VertexId, NMap)] = {
      println(edge.srcId + " before send " + edge.srcAttr)
      val newAttr = incrementNMap(edge.srcAttr)
      println(edge.srcId + " send " + newAttr + " to " + edge.dstId)

      if (!isEqual(edge.dstAttr, newAttr)) {
        Iterator((edge.dstId, newAttr))
      } else {
        Iterator.empty
      }
    }

    /**
     * @param a NMap
     * @param b NMap
     * @return VD
     */
    def messageCombiner(a: NMap, b: NMap): NMap = {
      addMaps(a, b)
    }

    // для каждого узла посчитали какие узлы доступны за конкретное число шагов
    Pregel(initGraph, initMessage, activeDirection = EdgeDirection.In)(
      vertexProgram, sendMessage, messageCombiner)
  }

  private def addMaps(nmap1: NMap, nmap2: NMap): NMap = {
    (nmap1.keySet ++ nmap2.keySet).map({
      k => k -> (
        nmap1.getOrElse(k, new HyperLogLogMonoid(BIT_SIZE).zero) +
          nmap2.getOrElse(k, new HyperLogLogMonoid(BIT_SIZE).zero))
    }).toMap
  }

  /**
   * проверка, что b не меняет число соседей
   * @param a NMap
   * @param b NMap
   * @return
   */
  private def isEqual(a: NMap, b: NMap): Boolean = {
    val newVal = addMaps(a, b)
    for (key <- b.keySet) {
      val oldSize = a.getOrElse(key, new HyperLogLogMonoid(BIT_SIZE).zero).estimatedSize
      val newSize = newVal.getOrElse(key, new HyperLogLogMonoid(BIT_SIZE).zero).estimatedSize
      if (oldSize != newSize) {
        return false
      }
    }
    true
  }

  private def calculateForNode(distances: NMap) = {
    var harmonic = 0.0
    val sorted = distances.filterKeys(_ > 0).toSeq.sortBy(_._1)
    var total = new HyperLogLogMonoid(BIT_SIZE).zero
    for ((step, v) <- sorted) {
      val before = total.estimatedSize
      total += v
      val after = total.estimatedSize
      harmonic += BigDecimal((after - before) / step)
        .setScale(5, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
    }
  }

}
