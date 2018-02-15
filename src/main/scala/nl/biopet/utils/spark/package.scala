/*
 * Copyright (c) 2014 Biopet
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package nl.biopet.utils

import java.net.URLClassLoader

import org.apache.spark.{SparkConf, SparkContext}

package object spark {

  def getConf(name: String,
              master: Option[String] = None,
              sparkConfig: Map[String, String] = Map(),
              localThreads: Int = 1): SparkConf = {

    val jars = ClassLoader.getSystemClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .map(_.getFile)
      .filter(_.endsWith(".jar"))

    sparkConfig.foldLeft(
      new SparkConf()
        .setExecutorEnv(sys.env.toArray)
        .setAppName(name)
        .setMaster(master.getOrElse(s"local[$localThreads]"))
        .setJars(jars))((a, b) => a.set(b._1, b._2))
  }

  def loadSparkContext(name: String,
                       master: Option[String] = None,
                       sparkConfig: Map[String, String] = Map(),
                       localThreads: Int = 1): SparkContext = {
    val conf = getConf(name, master, sparkConfig, localThreads)
    new SparkContext(conf)
  }
}
