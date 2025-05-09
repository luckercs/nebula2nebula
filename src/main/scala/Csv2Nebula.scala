import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{NebulaConnectionConfig, WriteMode, WriteNebulaEdgeConfig, WriteNebulaVertexConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.File

object Csv2Nebula {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  private val log_flag = "======================="

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession.builder
      .appName("Csv2Nebula")
      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()

    val optionsProcessor = new OptionsProcessor()
    val commandLine = optionsProcessor.parse(args)
    val metad = commandLine.getOptionValue("metad", "127.0.0.1:9559")
    val graphd = commandLine.getOptionValue("graphd", "127.0.0.1:9669")
    val user = commandLine.getOptionValue("user", "root")
    val pass = commandLine.getOptionValue("pass", "nebula")
    val csvDir = commandLine.getOptionValue("csv", "nebula_dump")
    val delimiter = commandLine.getOptionValue("delimiter", ",")

    val nebulaConnectionConfig = NebulaConnectionConfig.builder()
      .withMetaAddress(metad)
      .withGraphAddress(graphd)
      .withConenctionRetry(2)
      .withExecuteRetry(2)
      .withTimeout(6000)
      .build()

    val csvFiles = findCSVPaths(csvDir)
    for (csvFile <- csvFiles) {
      LOG.info(log_flag + "Processing TAG CSV: " + csvFile + "...")
      val csvName = new File(csvFile).getName
      val csvNameSplits = csvName.split("__")
      val typeName = csvNameSplits(0)
      val spaceName = csvNameSplits(1)
      val tagName = csvNameSplits(2).replace(".csv", "")

      if (typeName.equals("tag")) {
        val df = spark.read
          .option("header", "true")
          .option("delimiter", delimiter)
          .csv(csvFile)
        val nebulaWriteVertexConfig = WriteNebulaVertexConfig
          .builder()
          .withSpace(spaceName)
          .withTag(tagName)
          .withVidField("_vertexId")
//          .withVidPolicy("hash")
          .withVidAsProp(false)
          .withUser(user)
          .withPasswd(pass)
          .withBatch(512)
          .withWriteMode(WriteMode.INSERT)
          .withOverwrite(true)
          .build()
        df.write.nebula(nebulaConnectionConfig, nebulaWriteVertexConfig).writeVertices()
        LOG.info(log_flag + "Wrote tag " + spaceName + "." + tagName + ": " + df.count() + " to nebulagraph")
      }
    }

    for (csvFile <- csvFiles) {
      LOG.info(log_flag + "Processing EDGE CSV: " + csvFile + "...")
      val csvName = new File(csvFile).getName
      val csvNameSplits = csvName.split("__")
      val typeName = csvNameSplits(0)
      val spaceName = csvNameSplits(1)
      val edgeName = csvNameSplits(2).replace(".csv", "")

      if (typeName.equals("edge")) {
        val df = spark.read
          .option("header", "true")
          .option("delimiter", delimiter)
          .csv(csvFile)
        val nebulaWriteEdgeConfig = WriteNebulaEdgeConfig
          .builder()
          .withSpace(spaceName)
          .withEdge(edgeName)
          .withSrcIdField("_srcId")
          .withSrcPolicy(null)
          .withDstIdField("_dstId")
          .withDstPolicy(null)
          .withRankField("_rank")
          .withSrcAsProperty(false)
          .withDstAsProperty(false)
          .withRankAsProperty(false)
          .withWriteMode(WriteMode.INSERT)
          .withOverwrite(true)
          .withUser(user)
          .withPasswd(pass)
          .withBatch(512)
          .build()
        df.write.nebula(nebulaConnectionConfig, nebulaWriteEdgeConfig).writeEdges()
        LOG.info(log_flag + "Wrote edge " + spaceName + "." + edgeName + ": " + df.count() + " to nebulagraph")
      }
    }
    LOG.info(log_flag + "Csv2Nebula done")
    spark.close()
  }

  def findCSVPaths(path: String): List[String] = {
    val file = new File(path)
    if (file.isFile && file.length() > 0 && file.getName.endsWith(".csv")) {
      List(path)
    } else if (file.isDirectory) {
      file.listFiles()
        .filter(_.isFile)
        .filter(_.length() > 0)
        .filter(_.getName.endsWith(".csv"))
        .map(_.getPath)
        .toList
    } else {
      List.empty
    }
  }
}
