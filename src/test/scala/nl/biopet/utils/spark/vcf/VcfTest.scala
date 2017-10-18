package nl.biopet.utils.spark.vcf

import nl.biopet.test.BiopetTest
import nl.biopet.utils.ngs.intervals.BedRecord
import org.apache.spark.SparkContext
import org.testng.annotations.Test
import nl.biopet.utils.spark

class VcfTest extends BiopetTest {
  @Test
  def testLoadRecords(): Unit = {
    val inputVcf = resourceFile("/chrQ.vcf.gz")
    implicit val sc: SparkContext = spark.loadSparkContext("test")

    try {
      val rdd = loadRecords(inputVcf, List(BedRecord("chrQ", 1000, 1100)))
      rdd.count() shouldBe 1L
    } finally {
      sc.stop()
    }
  }
}
