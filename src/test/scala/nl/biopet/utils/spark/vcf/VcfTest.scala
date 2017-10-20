package nl.biopet.utils.spark.vcf

import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.test.BiopetTest
import nl.biopet.utils.ngs.intervals.BedRecord
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
      val rdd = loadRecords(inputVcf, List(BedRecord("chrQ", 1000, 1100)))
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
      val records = loadRecords(inputVcf, List(BedRecord("chrQ", 1, 16000)))
      val compare = sampleCompare(records, header).collectAsMap()("chrQ")
      compare.samples.size shouldBe 3
      compare.genotypesCount(0)(0) shouldBe 2
    } finally {
      sc.stop()
      vcfReader.close()
    }
  }

}
