package cc.p2k.spark.graphx.lib

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.language.postfixOps


object HarmonicCentrality extends Logging {

	def personalizedHarmonicCentrality(vertexId: Long, graph: Graph[Double, Double]): Double ={
		val initialGraph = graph.mapVertices(
			(id, _) => {
				if (id == vertexId) 0.0
				else Double.PositiveInfinity
			}
		)

		val paths = initialGraph.pregel(Double.PositiveInfinity)(
			(id, dist, newDist) => math.min(dist, newDist), // Vertex Program
			triplet => {  // Send Message
				if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
					Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
				} else {
					Iterator.empty
				}
			},
			(a,b) => math.min(a,b) // Merge Message
		)

		val hr = paths.vertices
			.map[Double](
			  tuple => {
			    if (tuple._1 == vertexId) 0
			    else 1 / tuple._2
			  }
			)
			.reduce( (a, b) => a + b )

		hr
	}

	def harmonicCentrality(graph: Graph[Double, Double]): Graph[Double, Double] = {
		val res = graph.mapVertices[Double](
			(id, _) => personalizedHarmonicCentrality(id, graph)
		)

		res
	}

}
