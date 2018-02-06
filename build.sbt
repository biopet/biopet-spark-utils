organization := "com.github.biopet"
name := "SparkUtils"

biopetUrlName := "spark-utils"

biopetIsTool := false

developers += Developer(id="ffinfo", name="Peter van 't Hof", email="pjrvanthof@gmail.com", url=url("https://github.com/ffinfo"))

scalaVersion := "2.11.11"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7"

libraryDependencies += "com.github.biopet" %% "NgsUtils" % "0.3-SNAPSHOT" changing()
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1" % Provided

libraryDependencies += "com.github.biopet" %% "TestUtils" % "0.3-SNAPSHOT" % Test changing()
