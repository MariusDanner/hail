package is.hail.io.database

import scala.util.Properties

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
import java.sql.{Connection,DriverManager, Statement, Types, ResultSet}
import java.io.StringReader
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.postgresql.Driver

import htsjdk.samtools.util.FileExtensions
import htsjdk.tribble.SimpleFeature
import htsjdk.tribble.index.tabix.{TabixIndexCreator, TabixFormat}

import scala.io.Source
import scala.collection.mutable.HashMap

object DatabaseOperations {

  def copyVariantOccurrences(connection: Connection, variantId: Long, variantOccurrences: Array[VariantOccurrence]) {
    val stringBuilder = new StringBuilder;
    variantOccurrences.foreach { vo =>
      stringBuilder.append(variantId)
      stringBuilder.append(",")
      stringBuilder.append(vo.patientId)
      stringBuilder.append(",")
      vo.genotype1 match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append(",")
      vo.genotype2 match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append(",")
      vo.genotypeQuality match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append(",")
      vo.depthCoverage match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append(",")
      vo.alleleDepthRef match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append(",")
      vo.alleleDepthAlt match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append(",")
      vo.phredRef match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append(",")
      vo.phredHetero match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append(",")
      vo.phredAlt match {
        case Some(q) => {
          stringBuilder.append(q)
        }
        case None =>
      }
      stringBuilder.append("\n")
    }
    val reader = new StringReader(stringBuilder.toString)
    val pgconn = connection.asInstanceOf[BaseConnection]
    val copyManager = new CopyManager(pgconn);
    val copySql = "COPY variant_occurrences FROM STDIN CSV DELIMITER ','";
    copyManager.copyIn(copySql, reader)
  }

  def writeVariantOccurrences(connection: Connection, variantId: Long, variantOccurrences: Array[VariantOccurrence]) {
    val variantOccurrenceQuery = "INSERT INTO variant_occurrences (patient_id, variant_id, genotype1, genotype2, genotype_quality, depth_coverage, allele_depth_ref, allele_depth_alt, phred_scaled_likelihood_ref, phred_scaled_likelihood_hetero, phred_scaled_likelihood_alt) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    val variantOccurrencePrepared = connection.prepareStatement(variantOccurrenceQuery)

    variantOccurrences.foreach { vo =>
      variantOccurrencePrepared.setLong(1, vo.patientId)
      variantOccurrencePrepared.setLong(2, variantId)
      vo.genotype2 match {
        case Some(gt1) => variantOccurrencePrepared.setBoolean(3, gt1 == 1)
        case None => variantOccurrencePrepared.setNull(3, Types.BOOLEAN)
      }
      vo.genotype2 match {
        case Some(gt2) => variantOccurrencePrepared.setBoolean(4, gt2 == 1)
        case None => variantOccurrencePrepared.setNull(4, Types.BOOLEAN)
      }
      vo.genotypeQuality match {
        case Some(gq) => variantOccurrencePrepared.setInt(5, gq)
        case None => variantOccurrencePrepared.setNull(5, Types.INTEGER)
      }
      vo.depthCoverage match {
        case Some(dp) => variantOccurrencePrepared.setInt(6, dp)
        case None => variantOccurrencePrepared.setNull(6, Types.INTEGER)
      }
      vo.alleleDepthRef match {
        case Some(ad) => variantOccurrencePrepared.setInt(7, ad)
        case None => variantOccurrencePrepared.setNull(7, Types.INTEGER)
      }
      vo.alleleDepthAlt match {
        case Some(ad) => variantOccurrencePrepared.setInt(8, ad)
        case None => variantOccurrencePrepared.setNull(8, Types.INTEGER)
      }
      vo.phredRef match {
        case Some(pl) => variantOccurrencePrepared.setInt(9, pl)
        case None => variantOccurrencePrepared.setNull(9, Types.INTEGER)
      }
      vo.phredHetero match {
        case Some(pl) => variantOccurrencePrepared.setInt(10, pl)
        case None => variantOccurrencePrepared.setNull(10, Types.INTEGER)
      }
      vo.phredRef match {
        case Some(pl) => variantOccurrencePrepared.setInt(11, pl)
        case None => variantOccurrencePrepared.setNull(11, Types.INTEGER)
      }
      variantOccurrencePrepared.addBatch()
    }

    variantOccurrencePrepared.executeBatch()
  }

  def getOrCreatePatients(connection : Connection, patients : IndexedSeq[String]): Array[Long] = {
    var patientsMap = Map[String, Long]()

    val builder = new StringBuilder;
    patients.foreach { mrn =>
      builder.append("'")
      builder.append(mrn)
      builder.append("',")
    }

    val placeHolders =  builder.deleteCharAt( builder.length -1 ).toString();

    val getPatientsQuery = "select id, mrn from patients where mrn in (" + placeHolders + ")"
    val getPatientsStatement = connection.createStatement()
    val getPatientsRs = getPatientsStatement.executeQuery(getPatientsQuery)
    while (getPatientsRs.next()) {
      val id = getPatientsRs.getLong("id")
      val mrn = getPatientsRs.getString("mrn")
      patientsMap += (mrn -> id)
    }
    val patientsInsertQuery = "INSERT INTO patients (mrn) VALUES (?)"
    val usingHana = "com.sap.db.jdbc.Driver".equals(Properties.envOrElse("DB_DRIVER", ""))
    patients.foreach { mrn =>
      if (! patientsMap.contains(mrn)) {
        val insertStatement = if (usingHana) connection.prepareStatement(patientsInsertQuery) else connection.prepareStatement(patientsInsertQuery, Statement.RETURN_GENERATED_KEYS)
        insertStatement.setString(1, mrn)
        insertStatement.executeUpdate()
        if (usingHana) {
          val id = getHanaSequenceValue(connection, "PATIENTS")
          patientsMap += (mrn->id)
        } else {
          val insertRs = insertStatement.getGeneratedKeys()
          if (insertRs.next()) {
            val id = insertRs.getLong(1)
            patientsMap += (mrn->id)
          } else {
            warn("Could not insert mrn " + mrn)
          }
        }
      }
    }
    var patientsList = Array[Long]()
    patients.foreach { p => patientsList :+= patientsMap(p)}
    patientsList
  }

  def createDatasourceEntry(connection: Connection, path: String) : Long = {
    val datasourceQuery = "INSERT INTO datasources (filename) VALUES (?)"
    val usingHana = "com.sap.db.jdbc.Driver".equals(Properties.envOrElse("DB_DRIVER", ""))
    val datasourcePrepared = if (usingHana) connection.prepareStatement(datasourceQuery) else connection.prepareStatement(datasourceQuery, Statement.RETURN_GENERATED_KEYS)
    datasourcePrepared.setString(1, path)
    datasourcePrepared.executeUpdate()

    var datasourceId = 0L

    if (usingHana) {
      datasourceId = getHanaSequenceValue(connection, "DATASOURCES")
    } else {
      val rs = datasourcePrepared.getGeneratedKeys()
      if (rs.next())
        datasourceId = rs.getLong(1)
    }
    datasourceId
  }

  def getAllPatients(connection: Connection): Array[String] = {
    val query = "SELECT mrn from patients ORDER BY id ASC"
    val statement = connection.prepareStatement(query)
    val rs = statement.executeQuery()
    var patients = Array[String]()
    while (rs.next()) {
      patients :+= rs.getString("mrn")
    }
    patients
  }

  def getPatients(connection: Connection, mrns: Array[String]): Array[String] = {
    val builder = new StringBuilder;
    mrns.foreach { mrn =>
      builder.append("'")
      builder.append(mrn)
      builder.append("',")
    }

    val placeHolders =  builder.deleteCharAt( builder.length -1 ).toString();

    val query = "select mrn from patients where mrn in (" + placeHolders + ") ORDER BY id asc"
    val statement = connection.prepareStatement(query)
    val rs = statement.executeQuery()
    var patients = Array[String]()
    while (rs.next()) {
      patients :+= rs.getString("mrn")
    }
    patients
  }

  def getPatientIds(connection: Connection, mrns: Array[String]): Array[Int] = {
    val builder = new StringBuilder;
    mrns.foreach { mrn =>
      builder.append("'")
      builder.append(mrn)
      builder.append("',")
    }

    val placeHolders =  builder.deleteCharAt( builder.length -1 ).toString();

    val query = "select id from patients where mrn in (" + placeHolders + ") ORDER BY id asc"
    val statement = connection.prepareStatement(query)
    val rs = statement.executeQuery()
    var patients = Array[Int]()
    while (rs.next()) {
      patients :+= rs.getInt("id")
    }
    patients
  }

  def loadVariants(connection: Connection, files: Array[String], inputVariants: Array[String]) : Array[Variant] = {
    var selectQueryBuilder = new StringBuilder("SELECT v.id, chromosome, position, reference, alternative, rsid, quality, string_agg(db.id, ',') as dbsnpid from variants v JOIN datasources d ON v.datasource_id = d.id left outer join dbsnp db on v.chromosome = db.chrom and v.position = db.pos and v.reference = db.ref and db.alt like CONCAT(CONCAT('%',v.alternative),'%')")
    var useAnd = false
    if (files.length > 0) {
      useAnd = true
      selectQueryBuilder.append(" WHERE d.filename IN(")
      files.foreach { file =>
        selectQueryBuilder.append("'")
        selectQueryBuilder.append(file)
        selectQueryBuilder.append("',")
      }
      selectQueryBuilder.deleteCharAt(selectQueryBuilder.length -1)
      selectQueryBuilder.append(")")
    }

    if (inputVariants.length > 0) {
      if (useAnd) {
        selectQueryBuilder.append(" AND (")
      } else {
        selectQueryBuilder.append(" WHERE (")
        useAnd = true
      }
      var useOr = false

      inputVariants.foreach { inputRange =>
        if (useOr) {
          selectQueryBuilder.append(" OR ")
        } else {
          useOr = true
        }

        if (inputRange.startsWith("rs")) {
          selectQueryBuilder.append("(v.rsid = '" + inputRange + "' OR db.id = '" + inputRange + "')")
        } else {
          val rangeParams = inputRange.split(":")

          selectQueryBuilder.append("(v.chromosome = '")
          selectQueryBuilder.append(rangeParams(0))
          selectQueryBuilder.append("' AND position >= ")
          selectQueryBuilder.append(rangeParams(1))
          selectQueryBuilder.append(" AND position < ")
          selectQueryBuilder.append(rangeParams(2))
          selectQueryBuilder.append(")")
        }

      }
      selectQueryBuilder.append(")")
    }
    selectQueryBuilder.append(" GROUP BY v.id, chromosome, position, reference, alternative, rsid, quality ORDER BY chromosome, position, reference, alternative ASC")
    info(selectQueryBuilder.toString())

    val selectPrepared = connection.prepareStatement(selectQueryBuilder.toString())

    val rs = selectPrepared.executeQuery()
    var variants = Array[Variant]()
    while (rs.next()) {
        var rsid = loadString(rs, "rsid")
        rsid match {
          case None => rsid = loadString(rs, "dbsnpid")
        }
        var variant = Variant(Option(rs.getInt("id")), rs.getString("chromosome"), rs.getInt("position"), loadString(rs, "reference"), loadString(rs, "alternative"), loadDouble(rs, "quality"), rsid)
        variants :+= variant
    }
    variants
  }

  def loadString(rs: ResultSet, identifier: String): Option[String] = {
    try {
      val value = rs.getString(identifier)
      if (rs.wasNull())
        None
      else
        Some(value)
    } catch {
        case e: Exception => None
    }
  }

  def loadInt(rs: ResultSet, identifier: String): Option[Int] = {
    try {
      val value = rs.getInt(identifier)
      if (rs.wasNull())
        None
      else
        Some(value)
    } catch {
        case e: Exception => None
    }
  }

  def loadGenotype(rs: ResultSet, identifier: String): Option[Int] = {
    try {
      val value = rs.getBoolean(identifier)
      if (rs.wasNull())
        None
      else
        Some(if (value) 1 else 0)
    } catch {
        case e: Exception => None
    }
  }

  def loadDouble(rs: ResultSet, identifier: String): Option[Double] = {
    try {
      val value = rs.getDouble(identifier)
      if (rs.wasNull())
        None
      else
        Some(value)
    } catch {
        case e: Exception => None
    }
  }

  def loadVariantOccurrences(connection: Connection, variantId: Int, samples: Array[Int], entryFields: Array[String]) : Array[VariantOccurrence] = {

    val placeholderBuilder = new StringBuilder;
    samples.foreach { mrn =>
      placeholderBuilder.append("'")
      placeholderBuilder.append(mrn)
      placeholderBuilder.append("',")
    }

    val placeHolders =  placeholderBuilder.deleteCharAt( placeholderBuilder.length -1 ).toString();


    val selectQueryBuilder = new StringBuilder("SELECT patient_id,")
    if (entryFields.contains("GT")) {
      selectQueryBuilder.append("genotype1,genotype2,")
    }
    if (entryFields.contains("GQ")) {
      selectQueryBuilder.append("genotype_quality,")
    }
    if (entryFields.contains("DP")) {
      selectQueryBuilder.append("depth_coverage,")
    }
    if (entryFields.contains("AD")) {
      selectQueryBuilder.append("allele_depth_ref,allele_depth_alt,")
    }
    if (entryFields.contains("PL")) {
      selectQueryBuilder.append("phred_scaled_likelihood_ref,phred_scaled_likelihood_hetero,phred_scaled_likelihood_alt,")
    }
    selectQueryBuilder.deleteCharAt(selectQueryBuilder.length-1)
    selectQueryBuilder.append(" FROM variant_occurrences vo WHERE vo.variant_id = ? AND vo.patient_id in (" + placeHolders +  ") ORDER BY vo.patient_id ASC")

    val selectPrepared = connection.prepareStatement(selectQueryBuilder.toString())
    selectPrepared.setInt(1, variantId)

    val rs = selectPrepared.executeQuery()
    var variantOccurrences = Array[VariantOccurrence]()
    while (rs.next()) {
        val variantOccurrence = VariantOccurrence(rs.getLong("patient_id"), "", loadGenotype(rs, "genotype1"), loadGenotype(rs, "genotype2"), loadInt(rs, "genotype_quality"), loadInt(rs, "depth_coverage"), loadInt(rs, "allele_depth_ref"), loadInt(rs, "allele_depth_alt"), loadInt(rs, "phred_scaled_likelihood_ref"), loadInt(rs, "phred_scaled_likelihood_hetero"), loadInt(rs, "phred_scaled_likelihood_alt"))
        variantOccurrences :+= variantOccurrence
    }
    variantOccurrences
  }

  def writeVariant(connection: Connection, variant: Variant, datasourceId: Long) : Long = {
    val variantQuery = "INSERT INTO variants (datasource_id, chromosome, position, reference, alternative, rsid, quality, filters) VALUES (?,?,?,?,?,?,?,?)"
    val usingHana = "com.sap.db.jdbc.Driver".equals(Properties.envOrElse("DB_DRIVER", ""))
    val variantsPrepared = if (usingHana) connection.prepareStatement(variantQuery) else connection.prepareStatement(variantQuery, Statement.RETURN_GENERATED_KEYS)
    variantsPrepared.setLong(1, datasourceId)
    val chromosome = if (variant.chromosome.startsWith("chr")) variant.chromosome.substring(3) else variant.chromosome

    variantsPrepared.setString(2, chromosome.toInt.toString)
    variantsPrepared.setInt(3, variant.position)

    variant.reference match {
      case Some(reference) => variantsPrepared.setString(4, reference)
      case None => variantsPrepared.setNull(4, Types.VARCHAR)
    }

    variant.alternative match {
      case Some(alternative) => variantsPrepared.setString(5, alternative)
      case None => variantsPrepared.setNull(5, Types.VARCHAR)
    }

    variantsPrepared.setNull(6, Types.VARCHAR)

    variant.quality match {
      case Some(quality) => variantsPrepared.setDouble(7, quality)
      case None => variantsPrepared.setNull(7, Types.DOUBLE)
    }

    variantsPrepared.setNull(8, Types.ARRAY)


    variantsPrepared.executeUpdate()

    var variantId = 0L
    if (usingHana) {
      variantId = getHanaSequenceValue(connection, "VARIANTS")
    } else {
      val rs = variantsPrepared.getGeneratedKeys()
      if (rs.next())
        variantId = rs.getLong(1)
    }
    variantId
  }

  def getHanaSequenceValue(connection: Connection, tableName: String): Long = {
    val columnIdQuery = "SELECT column_id FROM table_columns WHERE table_name = ? AND column_name = 'ID'"
    val sequenceNameQuery = "select sequence_name from sequences where sequence_name like ?"

    val columnIdStatement = connection.prepareStatement(columnIdQuery)
    columnIdStatement.setString(1, tableName)
    val columnIdResultSet = columnIdStatement.executeQuery()
    var columnId = ""
    if (columnIdResultSet.next()) {
      columnId = columnIdResultSet.getString("column_id")
    }

    val sequenceNameStatement = connection.prepareStatement(sequenceNameQuery)
    sequenceNameStatement.setString(1, "%" + columnId + "%")
    val sequenceNameResultSet = sequenceNameStatement.executeQuery()
    var sequenceName = ""
    if (sequenceNameResultSet.next()) {
      sequenceName = sequenceNameResultSet.getString("sequence_name")
    }

    val sequenceValueQuery = "select " + sequenceName + ".currval as val from dummy"
    val sequenceValueStatement = connection.createStatement()
    val sequenceValueResultSet = sequenceValueStatement.executeQuery(sequenceValueQuery)
    var sequenceValue = -1L
    if (sequenceValueResultSet.next()) {
      sequenceValue = sequenceValueResultSet.getLong("val")
    }
    sequenceValue
  }
}

case class Variant(id: Option[Int], chromosome: String, position: Int, reference: Option[String], alternative: Option[String], quality: Option[Double], rsId: Option[String])

case class VariantOccurrence(patientId: Long, patientName: String, genotype1: Option[Int], genotype2: Option[Int], genotypeQuality: Option[Int], depthCoverage: Option[Int], alleleDepthRef: Option[Int], alleleDepthAlt: Option[Int], phredRef: Option[Int], phredHetero: Option[Int], phredAlt: Option[Int])