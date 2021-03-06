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

package nl.biopet.utils.spark.vcf

import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.test.BiopetTest
import nl.biopet.utils.ngs.intervals.BedRecord
import nl.biopet.utils.ngs.vcf.{
  FieldMethod,
  GeneralStats,
  GenotypeStats,
  VcfField
}
import org.apache.spark.SparkContext
import org.testng.annotations.{DataProvider, Test}
import nl.biopet.utils.spark

class VcfTest extends BiopetTest {
  @DataProvider(name = "loadRecordsProvider")
  def loadRecordsProvider: Array[Array[Any]] = Array(
    Array(false, false),
    Array(false, true),
    Array(true, false),
    Array(true, true)
  )

  @Test(dataProvider = "loadRecordsProvider")
  def testLoadRecords(cache: Boolean, sorted: Boolean): Unit = {
    val inputVcf = resourceFile("/chrQ.vcf.gz")
    implicit val sc: SparkContext = spark.loadSparkContext("test")

    try {
      val rdd = loadRecords(inputVcf,
                            sc.broadcast(List(BedRecord("chrQ", 1000, 1100))),
                            100,
                            cached = cache,
                            sorting = sorted)
      rdd.count() shouldBe 2L
    } finally {
      sc.stop()
    }
  }

  @Test
  def testSampleCompare(): Unit = {
    val inputVcf = resourceFile("/chrQ.vcf.gz")
    val vcfReader = new VCFFileReader(inputVcf, false)
    implicit val sc: SparkContext = spark.loadSparkContext("test")

    try {
      val header = sc.broadcast(vcfReader.getFileHeader)
      val regions = sc.broadcast(List(BedRecord("chrQ", 1, 16000)))
      val records = loadRecords(inputVcf, regions, 16000)
      val compare =
        sampleCompare(records, header, regions).collectAsMap()("chrQ")
      compare.samples.size shouldBe 3
      compare.genotypesCount(0)(0) shouldBe 2
    } finally {
      sc.stop()
      vcfReader.close()
    }
  }

  @Test
  def testGeneralStats(): Unit = {
    val inputVcf = resourceFile("/chrQ.vcf.gz")
    implicit val sc: SparkContext = spark.loadSparkContext("test")

    try {
      val regions = sc.broadcast(List(BedRecord("chrQ", 1, 16000)))
      val records = loadRecords(inputVcf, regions, 16000)
      val stats = generalStats(records, regions).collectAsMap()("chrQ")
      stats.toMap(GeneralStats.values.find(_.toString == "Total").get) shouldBe 2L
    } finally {
      sc.stop()
    }
  }

  @Test
  def testSampleDistributions(): Unit = {
    val inputVcf = resourceFile("/chrQ.vcf.gz")
    implicit val sc: SparkContext = spark.loadSparkContext("test")

    try {
      val regions = sc.broadcast(List(BedRecord("chrQ", 1, 16000)))
      val records = loadRecords(inputVcf, regions, 16000)
      val stats = sampleDistributions(records, regions).collectAsMap()("chrQ")
      stats.toMap(GenotypeStats.values.find(_.toString == "Total").get) shouldBe Map(
        3 -> 2L)
    } finally {
      sc.stop()
    }
  }

  @Test
  def testGenotypeStats(): Unit = {
    val inputVcf = resourceFile("/chrQ.vcf.gz")
    val vcfReader = new VCFFileReader(inputVcf, false)
    implicit val sc: SparkContext = spark.loadSparkContext("test")

    try {
      val regions = sc.broadcast(List(BedRecord("chrQ", 1, 16000)))
      val header = sc.broadcast(vcfReader.getFileHeader)
      val records = loadRecords(inputVcf, regions, 16000)
      val stats = genotypeStats(records, header, regions).collectAsMap()("chrQ")
      stats.samples.size shouldBe 3
      stats.toMap("Sample_101")(
        GenotypeStats.values.find(_.toString == "Total").get) shouldBe 2L
    } finally {
      sc.stop()
      vcfReader.close()
    }
  }

  @Test
  def testInfoFieldCounts(): Unit = {
    val inputVcf = resourceFile("/chrQ.vcf.gz")
    val vcfReader = new VCFFileReader(inputVcf, false)
    implicit val sc: SparkContext = spark.loadSparkContext("test")

    try {
      val regions = sc.broadcast(List(BedRecord("chrQ", 1, 16000)))
      val header = sc.broadcast(vcfReader.getFileHeader)
      val records = loadRecords(inputVcf, regions, 16000)
      val vcfField = sc.broadcast(VcfField("DP", FieldMethod.All))
      val counts = infoFieldCounts(records, header, vcfField, regions)
        .collectAsMap()("chrQ")
      counts.countsMap shouldBe Map("124" -> 2)
    } finally {
      sc.stop()
      vcfReader.close()
    }
  }

  @Test
  def testGenotypeFieldCounts(): Unit = {
    val inputVcf = resourceFile("/chrQ.vcf.gz")
    val vcfReader = new VCFFileReader(inputVcf, false)
    implicit val sc: SparkContext = spark.loadSparkContext("test")

    try {
      val regions = sc.broadcast(List(BedRecord("chrQ", 1, 16000)))
      val header = sc.broadcast(vcfReader.getFileHeader)
      val records = loadRecords(inputVcf, regions, 16000)
      val vcfField = sc.broadcast(VcfField("DP", FieldMethod.All))
      val counts = genotypeFieldCounts(records, header, vcfField, regions)
        .collectAsMap()("chrQ")
      counts.samples.size shouldBe 3
      counts.countsMap("Sample_101") shouldBe Map("45" -> 2)
    } finally {
      sc.stop()
      vcfReader.close()
    }
  }

}
