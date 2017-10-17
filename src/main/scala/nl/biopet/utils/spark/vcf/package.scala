package nl.biopet.utils.spark

import java.io.File

import htsjdk.variant.variantcontext.VariantContext
import nl.biopet.utils.ngs
import nl.biopet.utils.ngs.intervals.BedRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

package object vcf {
  def loadRecords(inputFile: File,
                  regions: Seq[BedRecord],
                  repartition: Boolean = true,
                  cached: Boolean = true)(implicit sc: SparkContext): RDD[VariantContext] = {
    val rdd = sc.parallelize(regions, regions.size).flatMap(ngs.vcf.loadRegion(inputFile, _))
    if (repartition && cached) rdd.repartition(regions.size).cache()
    else if (repartition) rdd.repartition(regions.size)
    else if (cached) rdd.cache()
    else rdd
  }
}
