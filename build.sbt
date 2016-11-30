
lazy val root = (project in file(".")).
  settings(
    name := "parallel-stateful-streaming-example",
    version := "1.0",
    scalaVersion := "2.11.8",
    resolvers += Resolver.bintrayRepo("cakesolutions", "maven"),
    resolvers += Resolver.bintrayRepo("tabdulradi", "maven"),
    libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-akka" % "0.10.1.1",
    libraryDependencies += "io.github.cassandra-scala" %% "troy" % "0.4.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3",
    libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.4.14"
  )