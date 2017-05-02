name := "FG_dataset"

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
"org.apache.spark" % "spark-sql_2.11" % "2.0.0" % "provided",
"org.apache.spark" % "spark-core_2.11" % "2.0.0" % "provided",
"com.databricks"%"spark-xml_2.10"%"0.4.1",
"com.amazonaws" % "aws-java-sdk-pom" % "1.10.34",
"org.apache.hadoop" % "hadoop-aws" % "2.7.2",
"com.databricks" % "spark-redshift_2.11" % "3.0.0-preview1",
"com.typesafe" % "config" % "1.3.1"
)

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
