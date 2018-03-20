organization := "com.github.biopet"
organizationName := "Biopet"
name := "spark-utils"

biopetUrlName := "spark-utils"

startYear := Some(2014)

biopetIsTool := false

developers += Developer(id = "ffinfo",
                        name = "Peter van 't Hof",
                        email = "pjrvanthof@gmail.com",
                        url = url("https://github.com/ffinfo"))

scalaVersion := "2.11.12"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7"

libraryDependencies += "com.github.biopet" %% "ngs-utils" % "0.4-SNAPSHOT" changing ()
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % Provided
libraryDependencies += "org.bdgenomics.adam" %% "adam-core-spark2" % "0.23.0" % Provided

libraryDependencies += "com.github.biopet" %% "test-utils" % "0.3" % Test
