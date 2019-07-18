name         := "Waterdrop-apis"
version      := "1.2.0"
organization := "io.github.interestinglab.waterdrop"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)

// Change dependepcy scope to "provided" by : sbt -DprovidedDeps=true <task>
val providedDeps = Option(System.getProperty("providedDeps")).getOrElse("false")

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
  "com.typesafe" % "config" % "1.3.1",
  "com.alibaba" % "fastjson" % "1.2.47"
)

dependencyOverrides += "com.google.guava" % "guava" % "15.0"

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}