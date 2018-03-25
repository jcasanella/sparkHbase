lazy val root = (project in file(".")).
  settings(
    name := "streaming_hbase",
    version := "0.1",
    scalaVersion := "2.11.8",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    mainClass in Compile := Some("com.learn.streaming.parquet.StreamParquetApp") ,
    mainClass in assembly := Some("com.learn.streaming.parquet.StreamparquetApp"),
    assemblyJarName := "streaming_hbase.jar"
  )

val sparkVersion = "2.1.0"
val hbaseVersion = "1.2.0-cdh5.12.0"
val hadoopVersion = "2.6.0-cdh5.12.0"
val kafkaVersion = "0.11.0.0-cp1"
val confluentVersion = "3.3.0"
val jacksonVersion = "2.8.7"

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
)

libraryDependencies ++= Seq(
  "com.github.scopt" % "scopt_2.11" % "3.6.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "connect-json" % kafkaVersion,
  "org.apache.avro" % "avro" % "1.8.2",
  "io.confluent" % "kafka-avro-serializer" % confluentVersion,
  "org.apache.hbase" % "hbase" % hbaseVersion,
  "org.apache.hbase" % "hbase-client" % hbaseVersion,
  "org.apache.hbase" % "hbase-common" % hbaseVersion,
  "org.apache.hbase" % "hbase-server" % hbaseVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
)

resolvers ++= Seq(
  // Maven local
  Resolver.mavenLocal,
  // Cloudera
  "Cloudera"             at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  // Typesafe
  Resolver.typesafeRepo("releases"),
  Resolver.typesafeRepo("snapshots"),
  Resolver.typesafeIvyRepo("releases"),
  Resolver.typesafeIvyRepo("snapshots"),
  // Sonatype
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  // Scalaz
  "Scalaz Bintray Repo"  at "https://dl.bintray.com/scalaz/releases",
  // Apache
  "Apache Releases"      at "https://repository.apache.org/content/repositories/releases/",
  "Apache Staging"       at "https://repository.apache.org/content/repositories/staging",
  "Apache Snapshots"     at "https://repository.apache.org/content/repositories/snapshots",
  // Confluent
  "confluent"            at "http://packages.confluent.io/maven/"
)

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

