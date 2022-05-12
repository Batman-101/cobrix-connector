
import sbtrelease.ReleaseStateTransformations.{checkSnapshotDependencies, commitNextVersion, commitReleaseVersion, inquireVersions, publishArtifacts, pushChanges, setNextVersion, setReleaseVersion, tagRelease}

organization := "com.walmart.finance.cill"

name := "cill-cobrix-connector"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
  "za.co.absa.cobrix" %% "spark-cobol" % "2.1.0",
  "za.co.absa.cobrix" %% "cobol-parser" % "2.1.0",
  "org.scodec" %% "scodec-core" % "1.10.3",
  "org.scodec" %% "scodec-bits" % "1.1.5",
  "com.typesafe" % "config" % "1.3.1",
  "com.walmart.cill" %% "cill-common" % "latest.integration",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3.2.6.5.3004-13",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.3",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-2.2.0",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo := {
  val nexus = "https://repository.com/content/repositories/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "pangaea_snapshots/cill/")
  else
    Some("releases"  at nexus + "pangaea_releases/cill/")
}

updateOptions := updateOptions.value.withGigahorse(false)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


// Note that we are skippins some steps as we are using the maven plugin. Should remove that later when we get publishArtifacts working
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  tagRelease,                             // : ReleaseStep
  publishArtifacts,                       // : ReleaseStep, checks whether `publishTo` is properly set up
  setNextVersion,                         // : ReleaseStep
  commitNextVersion,                      // : ReleaseStep
  pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
)

releaseIgnoreUntrackedFiles := true

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"assembly/${name.value}_${version.value}.jar"


assembly / artifact := {
  val art = (assembly / artifact).value
  art.withClassifier(Some("assembly"))
}

addArtifact(assembly / artifact, assembly)

testOptions in Test += Tests.Argument("-P10")
logBuffered in Test := false
parallelExecution in Test := true
fork in Test := true
Test / testForkedParallel := true
concurrentRestrictions := Seq(Tags.limit(Tags.ForkedTestGroup, 1))

Test / testOptions += Tests.Cleanup(() => {
  import scala.reflect.io.Path

  println("**** CLEANUP STARTED ****")
  val scalaPath: Path = Path("target/testCaseRunDirET")
  scalaPath.deleteRecursively()
  val scalaPathJe: Path = Path("target/testCaseRunDir")
  scalaPathJe.deleteRecursively()
})

coverageHighlighting := true
coverageMinimum := 85
coverageFailOnMinimum := true
coverageEnabled in Test := true
