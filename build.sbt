lazy val root = (project in file(".")).
  settings(
    name := "streaming_hbase",
    version := "0.1",
    scalaVersion := "2.11.8",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    mainClass in Compile := Some("com.learn.streaming.StreamApp") ,
    mainClass in assembly := Some("com.learn.streaming.StreamApp"),
    assemblyJarName := "streaming_hbase.jar"
  )

val sparkVersion = "2.1.0"
val hbaseVersion = "1.2.0-cdh5.12.0"
val hadoopVersion = "2.6.0-cdh5.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.hbase" %% "hbase" % hbaseVersion,
  "org.apache.hbase" %% "hbase-client" % hbaseVersion,
  "org.apache.hbase" %% "hbase-common" % hbaseVersion,
  "org.apache.hbase" %% "hbase-server" % hbaseVersion,
  "org.apache.hadoop" %% "hadoop-common" % hadoopVersion
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
  "Apache Snapshots"     at "https://repository.apache.org/content/repositories/snapshots"
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

