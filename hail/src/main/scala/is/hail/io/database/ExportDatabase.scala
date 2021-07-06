package is.hail.io.database

import is.hail
import is.hail.HailContext
import is.hail.backend.BroadcastValue
import is.hail.annotations.Region
import is.hail.expr.ir.{ExecuteContext, MatrixValue}
import is.hail.types.physical._
import is.hail.types.virtual._
import is.hail.io.{VCFAttributes, VCFFieldAttributes, VCFMetadata}
import is.hail.io.compress.{BGzipOutputStream, BGzipLineReader}
import is.hail.io.fs.FS
import is.hail.utils._
import is.hail.variant.{Call, RegionValueVariant}
import java.sql.{Connection,DriverManager, Statement, Types}
import org.postgresql.Driver
import is.hail.database.{DatabaseConnector, Datasource}

import htsjdk.samtools.util.FileExtensions
import htsjdk.tribble.SimpleFeature
import htsjdk.tribble.index.tabix.{TabixIndexCreator, TabixFormat}

import scala.io.Source
import scala.collection.mutable.HashMap

object ExportDatabase {

  def validFormatType(typ: Type): Boolean = {
    typ match {
      case TString => true
      case TFloat64 => true
      case TFloat32 => true
      case TInt32 => true
      case TInt64 => true
      case TCall => true
      case _ => false
    }
  }

  def checkFormatSignature(tg: TStruct) {
    tg.fields.foreach { fd =>
      val valid = fd.typ match {
        case it: TContainer => validFormatType(it.elementType)
        case t => validFormatType(t)
      }
      if (!valid)
        fatal(s"Invalid type for format field '${ fd.name }'. Found '${ fd.typ }'.")
    }
  }

  def readVariantOccurrence(formatFieldOrder: Array[Int], tg: PStruct, offset: Long, fieldDefined: Array[Boolean], patientId: Long, variantId: Long): VariantOccurrence = {

    var gt1: Option[Int] = None
    var gt2: Option[Int] = None
    var gq: Option[Int] = None
    var dp: Option[Int] = None
    var ad: Array[Int] = Array.empty[Int]
    var pl: Array[Int] = Array.empty[Int]

    var i = 0
    while (i < formatFieldOrder.length) {
      fieldDefined(i) = tg.isFieldDefined(offset, formatFieldOrder(i))
      i += 1
    }

    var end = i
    while (end > 0 && !fieldDefined(end - 1))
      end -= 1

    if (end != 0) {
      i = 0
      while (i < end) { // write the variant occurrence values

        val j = formatFieldOrder(i)
        val name = tg.fields(j).name
        val fIsDefined = fieldDefined(i)
        val fOffset = tg.loadField(offset, j)

        if (fIsDefined) {
          tg.fields(j).typ match {
          case it: PContainer =>
            val pt = it
            if ("AD".equals(name)) {
              val fLength = pt.loadLength(fOffset) //always 2? for diploid species
              for (a <- 0 to fLength) {
                val eOffset = pt.loadElement(fOffset, fLength, a)
                ad = ad :+ Region.loadInt(eOffset)
              }
            } else if ("PL".equals(name)) { //always 3? for diploid species
              val fLength = pt.loadLength(fOffset) //always 2? for diploid species
              for (a <- 0 to fLength) {
                val eOffset = pt.loadElement(fOffset, fLength, a)
                pl = pl :+ Region.loadInt(eOffset)
              }
            }
          case t =>
            if ("GT".equals(name)) {
              val c = Region.loadInt(fOffset)
              val p = Call.allelePair(c)
              gt1 = Some(p.j)
              gt2 = Some(p.k)
            } else if ("GQ".equals(name)) {
              gq = Some(Region.loadInt(fOffset))
            } else if ("DP".equals(name)) {
              dp = Some(Region.loadInt(fOffset))
            }
          }
        }
        i += 1
      }
    }

    val ad1 = if (ad.length > 0) Some(ad(0)) else None
    val ad2 = if (ad.length > 1) Some(ad(1)) else None

    val pl1 = if (pl.length > 0) Some(pl(0)) else None
    val pl2 = if (pl.length > 1) Some(pl(1)) else None
    val pl3 = if (pl.length > 2) Some(pl(2)) else None



    VariantOccurrence(patientId, "", gt1, gt2, gq, dp, ad1, ad2, pl1, pl2, pl3)
  }

  def apply(ctx: ExecuteContext, mv: MatrixValue, path: String, append: Option[String],
    exportType: String, metadata: Option[VCFMetadata], tabix: Boolean = false) {

    // val connection = DatabaseConnector.connectToDatabase(false)
    val connection = Datasource.datasource.getConnection()
    connection.setAutoCommit(false)

    // write datasource
    val datasourceId = DatabaseOperations.createDatasourceEntry(connection, path)

    val fs = ctx.fs

    mv.typ.requireColKeyString()
    mv.typ.requireRowKeyVariant()

    val typ = mv.typ

    val tg = mv.entryPType

    checkFormatSignature(tg.virtualType)

    val formatFieldOrder: Array[Int] = tg.fieldIdx.get("GT") match {
      case Some(i) => (i +: tg.fields.filter(fd => fd.name != "GT").map(_.index)).toArray
      case None => tg.fields.indices.toArray
    }
    val tinfo =
      if (typ.rowType.hasField("info")) {
        typ.rowType.field("info").typ match {
          case _: TStruct => mv.rvRowPType.field("info").typ.asInstanceOf[PStruct]
          case t =>
            warn(s"export_database found row field 'info' of type $t, but expected type 'Struct'. Emitting no INFO fields.")
            PCanonicalStruct.empty()
        }
      } else {
        warn(s"export_to_database found no row field 'info'. Emitting no INFO fields.")
        PCanonicalStruct.empty()
      }


    val localNSamples = mv.nCols
    val hasSamples = localNSamples > 0


    val patientsMap = DatabaseOperations.getOrCreatePatients(connection, mv.stringSampleIds)


    connection.commit()
    connection.close()

    val fieldIdx = typ.rowType.fieldIdx

    def lookupVAField(fieldName: String, vcfColName: String, expectedTypeOpt: Option[Type]): (Boolean, Int) = {
      fieldIdx.get(fieldName) match {
        case Some(idx) =>
          val t = typ.rowType.types(idx)
          if (expectedTypeOpt.forall(t == _)) // FIXME: make sure this is right
            (true, idx)
          else {
            warn(s"export_vcf found row field $fieldName with type '$t', but expected type ${ expectedTypeOpt.get }. " +
              s"Emitting missing $vcfColName.")
            (false, 0)
          }
        case None => (false, 0)
      }
    }

    val filtersType = TSet(TString)
    val filtersPType = if (typ.rowType.hasField("filters"))
      mv.rvRowPType.field("filters").typ.asInstanceOf[PSet]
    else null

    val (idExists, idIdx) = lookupVAField("rsid", "ID", Some(TString))
    val (qualExists, qualIdx) = lookupVAField("qual", "QUAL", Some(TFloat64))
    val (filtersExists, filtersIdx) = lookupVAField("filters", "FILTERS", Some(filtersType))
    val (infoExists, infoIdx) = lookupVAField("info", "INFO", None)

    val fullRowType = mv.rvRowPType
    val localEntriesIndex = mv.entriesIdx
    val localEntriesType = mv.entryArrayPType

    mv.rvd.mapPartitions { (_, it) =>

      val formatDefinedArray = new Array[Boolean](formatFieldOrder.length)

      val rvv = new RegionValueVariant(fullRowType)
      // val localConnection = DatabaseConnector.connectToDatabase(false)

      it.map { ptr =>
        val localConnection = Datasource.datasource.getConnection()
        localConnection.setAutoCommit(false)
        rvv.set(ptr)

        if (idExists && fullRowType.isFieldDefined(ptr, idIdx)) { // TODO add rsid here
          // val idOffset = fullRowType.loadField(ptr, idIdx)
          // sb.append(fullRowType.types(idIdx).asInstanceOf[PString].loadString(idOffset))
        }


        // if (rvv.alleles().length > 1) {
        //   rvv.alleles().tail.foreachBetween(aa => // TODO multi allelic here
        //     sb.append(aa))(sb += ',')
        // }
        val reference = if (rvv.alleles().length > 0) Option(rvv.alleles()(0)) else None
        val alternative = if (rvv.alleles().tail.length > 0) Option(rvv.alleles().tail(0)) else None

        val quality = if (qualExists && fullRowType.isFieldDefined(ptr, qualIdx)) Option(Region.loadDouble(fullRowType.loadField(ptr, qualIdx))) else None

        if (filtersExists && fullRowType.isFieldDefined(ptr, filtersIdx)) {
          val filtersOffset = fullRowType.loadField(ptr, filtersIdx)
          val filtersLength = filtersPType.loadLength(filtersOffset)
          // if (filtersLength == 0) {
          //   sb.append("PASS")
          //    variantsPrepared.setString(8, "PASS")
          // } else
            // variantsPrepared.setString(8, "FILTER") // set filters here
            // iterableVCF(sb, filtersPType, filtersLength, filtersOffset, ';')
        // } else {
        //    variantsPrepared.setString(8, ".")
        }

        val variant = Variant(None, rvv.contig(), rvv.position(), reference, alternative, quality, None)
        val variantId = DatabaseOperations.writeVariant(localConnection, variant, datasourceId)

        if (hasSamples) { // start variant_occurrences

          val gsOffset = fullRowType.loadField(ptr, localEntriesIndex)
          var i = 0
          var variantOccurrences = Array.empty[VariantOccurrence]
          while (i < localNSamples) {
            if (localEntriesType.isElementDefined(gsOffset, i)) {
              variantOccurrences :+= readVariantOccurrence(formatFieldOrder, tg, localEntriesType.loadElement(gsOffset, localNSamples, i), formatDefinedArray, patientsMap(i), variantId) // TODO use patient id
            }
            i += 1
          }
          DatabaseOperations.writeVariantOccurrences(localConnection, variantId, variantOccurrences)
        }
        localConnection.commit()
        localConnection.close()
      }
    }.collect()

  }
}
