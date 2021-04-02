package is.hail.io.database

import is.hail.HailContext
import is.hail.annotations._
import is.hail.backend.BroadcastValue
import is.hail.backend.spark.SparkBackend
import is.hail.expr.ir.{ExecuteContext, LowerMatrixIR, MatrixHybridReader, MatrixRead, MatrixReader, MatrixValue, TableRead, TableValue}
import is.hail.types.{MatrixType, TableType}
import is.hail.types.physical.{PStruct, PType}
import is.hail.types.virtual._
import is.hail.io.bgen.LoadBgen
import is.hail.io.vcf.LoadVCF
import is.hail.rvd.{RVD, RVDContext, RVDType}
import is.hail.sparkextras.ContextRDD
import is.hail.utils._
import is.hail.variant._
import is.hail.io.fs.FS
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.broadcast.Broadcast
import org.json4s.{DefaultFormats, Extraction, Formats, JObject, JValue}
import java.sql.{Connection,DriverManager, Statement, Types}
import org.postgresql.Driver
import is.hail.database.DatabaseConnector
import is.hail.snpEff.SnpEffectDatabasePredictor
import scala.collection.JavaConverters._
import org.snpeff.interval.BioType


case class GenResult(file: String, nSamples: Int, nVariants: Int, rdd: RDD[(Annotation, Iterable[Annotation])])


object MatrixDatabaseReader {
  def fromJValue(ctx: ExecuteContext, jv: JValue): MatrixDatabaseReader = {
    val fs = ctx.fs

    implicit val formats: Formats = DefaultFormats
    val params = jv.extract[MatrixDatabaseReaderParameters]

    val referenceGenome = params.rg.map(ReferenceGenome.getReference)

    referenceGenome.foreach(ref => ref.validateContigRemap(params.contigRecoding))

    val connection = DatabaseConnector.connectToDatabase(false)

    val samples = if (params.samples.length > 0) params.samples else DatabaseOperations.getAllPatients(connection)
    val nSamples = samples.length

    // FIXME: can't specify multiple chromosomes
    val variants = DatabaseOperations.loadVariants(connection, params.files, params.variants)

    val nVariants = variants.length

    info(s"Number of variants: $nVariants")
    info(s"Number of samples: $nSamples")

    connection.close()

    var rowEntries = Array(
        "locus" -> TLocus.schemaFromRG(referenceGenome),
        "alleles" -> TArray(TString),
        // "quality" -> TFloat32,
        "rsid" -> TString)

    if (params.annotations) {
      warn("Including annotations")
      rowEntries :+= "annotations" -> TArray(TDict(TString,TString))
    }

    var entryEntries = Array[(String,Type)]()
    if (params.entryFields.contains("GT")) {
      entryEntries :+= ("GT" -> TCall)
    }
    if (params.entryFields.contains("GQ")) {
      entryEntries :+= ("GQ" -> TInt32)
    }
    if (params.entryFields.contains("DP")) {
      entryEntries :+= ("DP" -> TInt32)
    }
    if (params.entryFields.contains("AD")) {
      entryEntries :+= ("AD" -> TArray(TInt32))
    }
    if (params.entryFields.contains("PL")) {
      entryEntries :+= ("PL" -> TArray(TInt32))
    }

    def fullMatrixType: MatrixType = MatrixType(
      globalType = TStruct.empty,
      colKey = Array("s"),
      colType = TStruct("s" -> TString),
      rowKey = Array("locus", "alleles"),
      rowType = TStruct(rowEntries:_*),
      entryType = TStruct(entryEntries:_*)
        )

    new MatrixDatabaseReader(params, fullMatrixType, samples, variants, referenceGenome)
  }
}

case class MatrixDatabaseReaderParameters(
  files: Array[String],
  samples: Array[String],
  variants: Array[String],
  annotations: Boolean,
  entryFields: Array[String],
  nPartitions: Option[Int],
  tolerance: Double,
  rg: Option[String],
  contigRecoding: Map[String, String],
  skipInvalidLoci: Boolean)

class MatrixDatabaseReader(
  val params: MatrixDatabaseReaderParameters,
  val fullMatrixType: MatrixType,
  samples: Array[String],
  variantsArray: Array[Variant],
  referenceGenome: Option[ReferenceGenome]
) extends MatrixHybridReader {
  def pathsUsed: Seq[String] = params.files

  def nSamples: Int = samples.length

  def columnCount: Option[Int] = Some(nSamples)

  def partitionCounts: Option[IndexedSeq[Long]] = None

  def rowAndGlobalPTypes(context: ExecuteContext, requestedType: TableType): (PStruct, PStruct) = {
    requestedType.canonicalRowPType -> PType.canonical(requestedType.globalType).asInstanceOf[PStruct]
  }

  def apply(tr: TableRead, ctx: ExecuteContext): TableValue = {
    val sc = SparkBackend.sparkContext("MatrixDatabaseReader.apply")

    val rdd = sc.parallelize(variantsArray)
    val rg = referenceGenome
    val localSamples = samples

    val requestedType = tr.typ
    val requestedRowType = requestedType.rowType
    val (requestedEntryType, dropCols) = requestedRowType.fieldOption(LowerMatrixIR.entriesFieldName) match {
      case Some(fd) => fd.typ.asInstanceOf[TArray].elementType.asInstanceOf[TStruct] -> false
      case None => TStruct.empty -> true
    }

    val localNSamples = nSamples
    val localAnnotations = params.annotations


    var rowTypes = Map(
      "locus" -> requestedRowType.fieldOption("locus").map(_.typ),
      "alleles" -> requestedRowType.fieldOption("alleles").map(_.typ),
      "rsid" -> requestedRowType.fieldOption("rsid").map(_.typ)
    )

    if (localAnnotations) {
      rowTypes += ("annotations" -> requestedRowType.fieldOption("annotations").map(_.typ))
    }

    val localEntryFields = params.entryFields


    var entryTypes : Map[String, Option[Type]] = Map()
    if (!dropCols) {
      if (params.entryFields.contains("GT")) {
        entryTypes += ("GT" -> requestedEntryType.fieldOption("GT").map(_.typ))
      }
      if (params.entryFields.contains("GQ")) {
        entryTypes += ("GQ" -> requestedEntryType.fieldOption("GQ").map(_.typ))
      }
      if (params.entryFields.contains("DP")) {
        entryTypes += ("DP" -> requestedEntryType.fieldOption("DP").map(_.typ))
      }
      if (params.entryFields.contains("AD")) {
        entryTypes += ("AD" -> requestedEntryType.fieldOption("AD").map(_.typ))
      }
      if (params.entryFields.contains("PL")) {
        entryTypes += ("PL" -> requestedEntryType.fieldOption("PL").map(_.typ))
      }
    }

    SnpEffectDatabasePredictor.createCacheIfNotExists()

    val localRVDType = tr.typ.canonicalRVDType
    val rvd = RVD.coerce(ctx,
      localRVDType,
      ContextRDD.weaken(rdd).cmapPartitions { (ctx, it) =>
        val rvb = ctx.rvb
        val connection = DatabaseConnector.connectToDatabase(false)

        val predictor = new SnpEffectDatabasePredictor()

        it.map { variant =>

          rvb.start(localRVDType.rowType)
          rvb.startStruct()

          val locus = Locus.annotation(variant.chromosome, variant.position, rg)
          var alleles = FastIndexedSeq.empty[String]
          variant.reference match {
            case Some(ref) => alleles :+= ref
            case _ =>
          }
          variant.alternative match {
            case Some(alt) => alleles :+= alt
            case _ =>
          }

          rowTypes("locus").foreach(rvb.addAnnotation(_, locus))
          rowTypes("alleles").foreach(rvb.addAnnotation(_, alleles))
          variant.rsId match {
            case Some(rsId) => rowTypes("rsid").foreach(rvb.addAnnotation(_, rsId))
            case None => rowTypes("rsid").foreach(rvb.addAnnotation(_, null))
          }


          if (localAnnotations) { // load annotation here
            val effects = predictor.annotateVariant(variant.chromosome, variant.position, variant.reference.getOrElse(null), variant.alternative.getOrElse(null))
            // warn("" + variant.chromosome + ":" + variant.position)
            var annotations = FastIndexedSeq.empty[Map[String,String]]
            effects.asScala.foreach { effect =>
              var annotation = Map(
                  "allele" -> effect.getVariant().getGenotype(),
                  "effect" -> effect.getEffectTypeString(true),
                  "impact" -> effect.getEffectImpact().toString()
              )
              // warn("  " + effect.toString())
              if (effect.getGene() != null) {
                annotation += ("gene_name" -> effect.getGene().getGeneName())
                annotation += ("gene_id" -> effect.getGene().getId())
              }
              if (effect.getTranscript() != null) {
                annotation += ("feature_type" -> "transcript")
                annotation += ("feature_id" -> effect.getTranscript().getId())
                if (effect.getTranscript().getBioType() != null) {
                  annotation += ("biotype" -> effect.getTranscript().getBioType().toString())
                } else {
                  annotation += ("biotype" -> BioType.coding(effect.getTranscript().isProteinCoding()).toString())
                }
              }

              annotations :+= annotation
            }
            rowTypes("annotations").foreach(rvb.addAnnotation(_, annotations))
          }

          if (!dropCols) {
            val variantOccurrences = DatabaseOperations.loadVariantOccurrences(connection, variant.id.get, localSamples, localEntryFields)
            rvb.startArray(localNSamples)
            variantOccurrences.foreach { variantOccurrence =>
              rvb.startStruct()
              if (entryTypes.contains("GT")) {
                variantOccurrence.genotype1 match {
                  case Some(genotype1) => {
                    val gt = Call2(genotype1, variantOccurrence.genotype2.get, phased = false)
                    entryTypes("GT").foreach(rvb.addAnnotation(_, gt))
                  }
                  case None => entryTypes("GT").foreach(rvb.addAnnotation(_, null))
                }
              }

              if (entryTypes.contains("GQ")) {
                variantOccurrence.genotypeQuality match {
                  case Some(quality) => entryTypes("GQ").foreach(rvb.addAnnotation(_, quality))
                  case None => entryTypes("GQ").foreach(rvb.addAnnotation(_, null))
                }
              }

              if (entryTypes.contains("DP")) {
                variantOccurrence.depthCoverage match {
                  case Some(coverage) => entryTypes("DP").foreach(rvb.addAnnotation(_, coverage))
                  case None => entryTypes("DP").foreach(rvb.addAnnotation(_, null))
                }
              }

              if (entryTypes.contains("AD")) {
                variantOccurrence.alleleDepthRef match {
                  case Some(alleleDepthRef) => {
                    val ad = IndexedSeq(alleleDepthRef, variantOccurrence.alleleDepthAlt.get)
                    entryTypes("AD").foreach(rvb.addAnnotation(_, ad))
                  }
                  case None => entryTypes("AD").foreach(rvb.addAnnotation(_, null))
                }
              }

              if (entryTypes.contains("PL")) {
                variantOccurrence.phredRef match {
                  case Some(phredRef) => {
                    val pl = IndexedSeq(phredRef, variantOccurrence.phredHetero.get, variantOccurrence.phredAlt.get)
                    entryTypes("PL").foreach(rvb.addAnnotation(_, pl))
                  }
                  case None => entryTypes("PL").foreach(rvb.addAnnotation(_, null))
                }
              }

              rvb.endStruct()
            }
            rvb.endArray()
          }
          rvb.endStruct()

          rvb.end()
        }
      })

    val globalValue = makeGlobalValue(ctx, requestedType.globalType, samples.map(Row(_)))

    TableValue(ctx, tr.typ, globalValue, rvd)
  }

  override def toJValue: JValue = {
    implicit val formats: Formats = DefaultFormats
    decomposeWithName(params, "MatrixDatabaseReader")
  }

  override def hashCode(): Int = params.hashCode()

  override def equals(that: Any): Boolean = that match {
    case that: MatrixDatabaseReader => params == that.params
    case _ => false
  }
}
