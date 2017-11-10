package nl.biopet.utils.spark

import java.io.File

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFHeader
import nl.biopet.utils.ngs
import nl.biopet.utils.ngs.intervals.BedRecord
import nl.biopet.utils.ngs.vcf.{GeneralStats, GenotypeFieldCounts, GenotypeStats, InfoFieldCounts, SampleCompare, SampleDistributions, VcfField}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

package object vcf {
  def loadRecords(inputFile: File,
                  regions: Broadcast[List[BedRecord]],
                  sorting: Boolean = true,
                  cached: Boolean = true)(implicit sc: SparkContext): RDD[VariantContext] = {
    val rdd = sc.parallelize(regions.value, regions.value.size).mapPartitions(ngs.vcf.loadRegions(inputFile, _))
    if (sorting && cached) rdd.sortBy(x => (x.getContig, x.getStart)).cache()
    else if (sorting) rdd.repartition(regions.value.size)
    else if (cached) rdd.cache()
    else rdd
  }

  /**
    * This method will create a [[SampleCompare]] class for each contig
    *
    * @param vcfRecords Rdd for the vcf records, sorted RDD works best
    * @param header Header of the vcf file
    * @param sampleToSampleMinDepth Minimal depth to consider overlap
    * @return
    */
  def sampleCompare(vcfRecords: RDD[VariantContext],
                    header: Broadcast[VCFHeader],
                    regions: Broadcast[List[BedRecord]],
                    sampleToSampleMinDepth: Option[Int] = None): RDD[(String, SampleCompare)] = {
    vcfRecords.mapPartitions { it =>
      val contigCompare: mutable.Map[String, SampleCompare] = mutable.Map()
      it.foreach { record =>
        if (!contigCompare.contains(record.getContig))
          contigCompare += record.getContig -> new SampleCompare(header.value)
        contigCompare(record.getContig).addRecord(record, sampleToSampleMinDepth)
      }
      contigCompare.iterator
    }
      .union(vcfRecords.sparkContext.parallelize(regions.value.map(_.chr).distinct).map(_ -> new SampleCompare(header.value)))
      .reduceByKey(_ += _)
  }

  /**
    * This method will create a [[GeneralStats]] class for each contig
    *
    * @param vcfRecords Rdd for the vcf records, sorted RDD works best
    * @return
    */
  def generalStats(vcfRecords: RDD[VariantContext],
                   regions: Broadcast[List[BedRecord]]): RDD[(String, GeneralStats)] = {
    vcfRecords.mapPartitions { it =>
      val contigStats: mutable.Map[String, GeneralStats] = mutable.Map()
      it.foreach { record =>
        if (!contigStats.contains(record.getContig))
          contigStats += record.getContig -> new GeneralStats
        contigStats(record.getContig).addRecord(record)
      }
      contigStats.iterator
    }
      .union(vcfRecords.sparkContext.parallelize(regions.value.map(_.chr).distinct).map(_ -> new GeneralStats()))
      .reduceByKey(_ += _)
  }

  /**
    * This method will create a [[GeneralStats]] class for each contig
    *
    * @param vcfRecords Rdd for the vcf records, sorted RDD works best
    * @return
    */
  def sampleDistributions(vcfRecords: RDD[VariantContext],
                   regions: Broadcast[List[BedRecord]]): RDD[(String, SampleDistributions)] = {
    vcfRecords.mapPartitions { it =>
      val contigStats: mutable.Map[String, SampleDistributions] = mutable.Map()
      it.foreach { record =>
        if (!contigStats.contains(record.getContig))
          contigStats += record.getContig -> new SampleDistributions
        contigStats(record.getContig).addRecord(record)
      }
      contigStats.iterator
    }
      .union(vcfRecords.sparkContext.parallelize(regions.value.map(_.chr).distinct).map(_ -> new SampleDistributions()))
      .reduceByKey(_ += _)
  }

  /**
    * This method will create a [[GenotypeStats]] class for each contig
    *
    * @param vcfRecords Rdd for the vcf records, sorted RDD works best
    * @param header Header of the vcf file
    * @return
    */
  def genotypeStats(vcfRecords: RDD[VariantContext],
                    header: Broadcast[VCFHeader],
                    regions: Broadcast[List[BedRecord]]): RDD[(String, GenotypeStats)] = {
    vcfRecords.mapPartitions { it =>
      val contigStats: mutable.Map[String, GenotypeStats] = mutable.Map()
      it.foreach { record =>
        if (!contigStats.contains(record.getContig))
          contigStats += record.getContig -> new GenotypeStats(header.value)
        contigStats(record.getContig).addRecord(record)
      }
      contigStats.iterator
    }
      .union(vcfRecords.sparkContext.parallelize(regions.value.map(_.chr).distinct).map(_ -> new GenotypeStats(header.value)))
      .reduceByKey(_ += _)
  }

  /**
    * This method will create a [[InfoFieldCounts]] class for each contig
    *
    * @param vcfRecords Rdd for the vcf records, sorted RDD works best
    * @param header Header of the vcf file
    * @return
    */
  def infoFieldCounts(vcfRecords: RDD[VariantContext],
                      header: Broadcast[VCFHeader],
                      vcfField: Broadcast[VcfField],
                      regions: Broadcast[List[BedRecord]]): RDD[(String, InfoFieldCounts)] = {
    vcfRecords.mapPartitions { it =>
      val contigStats: mutable.Map[String, InfoFieldCounts] = mutable.Map()
      it.foreach { record =>
        if (!contigStats.contains(record.getContig))
          contigStats += record.getContig -> vcfField.value.newInfoCount(header.value)
        contigStats(record.getContig).addRecord(record)
      }
      contigStats.iterator
    }
      .union(vcfRecords.sparkContext.parallelize(regions.value.map(_.chr)
        .distinct)
        .map(_ -> vcfField.value.newInfoCount(header.value)))
      .reduceByKey(_ += _)
  }

  /**
    * This method will create a [[GenotypeFieldCounts]] class for each contig
    *
    * @param vcfRecords Rdd for the vcf records, sorted RDD works best
    * @param header Header of the vcf file
    * @return
    */
  def genotypeFieldCounts(vcfRecords: RDD[VariantContext],
                          header: Broadcast[VCFHeader],
                          vcfField: Broadcast[VcfField],
                          regions: Broadcast[List[BedRecord]]): RDD[(String, GenotypeFieldCounts)] = {
    vcfRecords.mapPartitions { it =>
      val contigStats: mutable.Map[String, GenotypeFieldCounts] = mutable.Map()
      it.foreach { record =>
        if (!contigStats.contains(record.getContig))
          contigStats += record.getContig -> vcfField.value.newGenotypeCount(header.value)
        contigStats(record.getContig).addRecord(record)
      }
      contigStats.iterator
    }
      .union(vcfRecords.sparkContext.parallelize(regions.value.map(_.chr)
        .distinct)
        .map(_ -> vcfField.value.newGenotypeCount(header.value)))
      .reduceByKey(_ += _)
  }
}
