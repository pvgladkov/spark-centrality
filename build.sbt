name := "spark-centrality"

version := "0.12"

scalaVersion := "2.11.7"

organization := "cc.p2k"

libraryDependencies += "com.twitter" %% "algebird-core" % "0.11.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

sparkComponents += "graphx"

spName := "webgeist/spark-centrality"

sparkVersion := "1.5.0"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

parallelExecution in Test := false