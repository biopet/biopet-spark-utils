package nl.biopet.utils

import java.net.URLClassLoader

import org.apache.spark.{SparkConf, SparkContext}

package object spark {
  def loadSparkContext(name: String,
                       master: Option[String] = None,
                       sparkConfig: Map[String, String] = Map(),
                       localThreads: Int = 1): SparkContext = {
    val jars = ClassLoader.getSystemClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .map(_.getFile)
      .filter(_.endsWith(".jar"))
    val conf = sparkConfig.foldLeft(
      new SparkConf()
        .setExecutorEnv(sys.env.toArray)
        .setAppName(name)
        .setMaster(
          master.getOrElse(s"local[${localThreads}]"))
        .setJars(jars))((a, b) => a.set(b._1, b._2))
    new SparkContext(conf)
  }
}
