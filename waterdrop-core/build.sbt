name := "Waterdrop-core"
version := "1.2.3"
organization := "io.github.interestinglab.waterdrop"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"
lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

// Change dependepcy scope to "provided" by : sbt -DprovidedDeps=true <task>
val providedDeps = Option(System.getProperty("providedDeps")).getOrElse("true")
//val providedDeps = "true"
providedDeps match {
  case "true" => {
    println("providedDeps = true")
    libraryDependencies ++= providedDependencies.map(_ % "provided")
  }
  case "false" => {
    println("providedDeps = false")
    libraryDependencies ++= providedDependencies.map(_ % "compile")
  }
}

libraryDependencies ++= Seq(

  //  "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
    exclude("org.spark-project.spark", "unused"),
  "com.typesafe" % "config" % "1.3.1",
  "com.alibaba" % "QLExpress" % "3.2.0",
  "com.alibaba" % "fastjson" % "1.2.47",
  "commons-lang" % "commons-lang" % "2.6",
  "io.thekraken" % "grok" % "0.1.5",
  "org.apache.kafka" % "kafka-clients" % "1.1.0",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "6.2.1",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.apache.commons" % "commons-compress" % "1.15",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.39" excludeAll (ExclusionRule(organization = "com.fasterxml.jackson.core")),
  "mysql" % "mysql-connector-java" % "5.1.42",
  "org.apache.httpcomponents" % "httpclient" % "4.5.4" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "junit" % "junit" % "4.12" % "test",
  "com.hualala.spark" % "streaming-metrics" % "17.4.1-SNAPSHOT"
  //  , "com.oracle.jdbc" % "ojdbc8" % "12.2.0.1",
  //  "com.oracle.jdbc" % "ucp" % "12.2.0.1"
)

// For binary compatible conflicts, sbt provides dependency overrides.
// They are configured with the dependencyOverrides setting.
dependencyOverrides += "com.google.guava" % "guava" % "15.0"
dependencyOverrides += "org.apache.kafka" % "kafka-clients" % "1.1.0"

resolvers += Resolver.mavenLocal

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")


// automatically check coding style before compile
scalastyleFailOnError := true
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

// antlr4 source code generatioin is invoked in command: sbt compile
antlr4Settings
antlr4Version in Antlr4 := "4.5.3"
antlr4PackageName in Antlr4 := Some("io.github.interestinglab.waterdrop.configparser")
antlr4GenListener in Antlr4 := false
antlr4GenVisitor in Antlr4 := true

publishTo := Some(
  if (isSnapshot.value) {
    Opts.resolver.sonatypeSnapshots
  } else {
    Opts.resolver.sonatypeStaging
  }
)

unmanagedJars in Compile ++= Seq(
  Attributed.blank[File](file("D:\\oracle\\ojdbc8-full\\ojdbc8-full\\ojdbc8.jar")),
  Attributed.blank[File](file("D:\\oracle\\ojdbc8-full\\ojdbc8-full\\ucp.jar"))
)

assemblyMergeStrategy in assembly := {
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".fmpp" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".thrift" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first

  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}