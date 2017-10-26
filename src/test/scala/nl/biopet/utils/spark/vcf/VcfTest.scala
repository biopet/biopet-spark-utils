package nl.biopet.utils.spark.vcf

import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.test.BiopetTest
import nl.biopet.utils.ngs.intervals.BedRecord
import nl.biopet.utils.ngs.vcf.{GeneralStats, GenotypeStats}
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
      val rdd = loadRecords(inputVcf, sc.broadcast(List(BedRecord("chrQ", 1000, 1100))), cached = cache, sorting = sorted)
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
      val records = loadRecords(inputVcf, regions)
      val compare = sampleCompare(records, header, regions).collectAsMap()("chrQ")
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
      val records = loadRecords(inputVcf, regions)
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
      val records = loadRecords(inputVcf, regions)
      val stats = sampleDistributions(records, regions).collectAsMap()("chrQ")
      stats.toMap(GenotypeStats.values.find(_.toString == "Total").get) shouldBe Map(3 -> 2L)
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
      val records = loadRecords(inputVcf, regions)
      val stats = genotypeStats(records, header, regions).collectAsMap()("chrQ")
      stats.samples.size shouldBe 3
      stats.toMap("Sample_101")(GenotypeStats.values.find(_.toString == "Total").get) shouldBe 2L
    } finally {
      sc.stop()
      vcfReader.close()
    }
  }

}
