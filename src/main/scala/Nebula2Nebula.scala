import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.{NebulaDataFrameReader, NebulaDataFrameWriter}
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig, WriteMode, WriteNebulaEdgeConfig, WriteNebulaVertexConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object Nebula2Nebula {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  private val log_flag = "======================="

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession.builder
      .appName("Nebula2Nebula")
      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()

    val optionsProcessor = new OptionsProcessor()
    val commandLine = optionsProcessor.parse(args)
    val metad = commandLine.getOptionValue("metad", "127.0.0.1:9559")
    val graphd = commandLine.getOptionValue("graphd", "127.0.0.1:9669")
    val user = commandLine.getOptionValue("user", "root")
    val pass = commandLine.getOptionValue("pass", "nebula")

    val t_metad = commandLine.getOptionValue("t_metad")
    val t_graphd = commandLine.getOptionValue("t_graphd")
    val t_user = commandLine.getOptionValue("t_user", "root")
    val t_pass = commandLine.getOptionValue("t_pass", "nebula")

    val nebulaConnectionConfig = NebulaConnectionConfig.builder()
      .withMetaAddress(metad)
      .withGraphAddress(graphd)
      .withConenctionRetry(2)
      .withExecuteRetry(2)
      .withTimeout(6000)
      .build()

    val t_nebulaConnectionConfig = NebulaConnectionConfig.builder()
      .withMetaAddress(t_metad)
      .withGraphAddress(t_graphd)
      .withConenctionRetry(2)
      .withExecuteRetry(2)
      .withTimeout(6000)
      .build()

    val nebulaPool = new GraphNebulaPool(graphd, user, pass)
    val session = nebulaPool.getSession()
    var resultSet = session.execute("SHOW SPACES")
    val spaces = resultSet.colValues(resultSet.getColumnNames.get(0))

    for (space <- spaces.asScala) {
      val spaceName = space.asString()
      LOG.info(log_flag + "Processing space: " + spaceName + "...")
      session.execute("USE " + spaceName)

      resultSet = session.execute("SHOW TAGS")
      val tags = resultSet.colValues(resultSet.getColumnNames.get(0))
      for (tag <- tags.asScala) {
        val tagName = tag.asString()
        val nebulaReadVertexConfig = ReadNebulaConfig
          .builder()
          .withUser(user)
          .withPasswd(pass)
          .withSpace(spaceName)
          .withLabel(tagName)
          .withPartitionNum(1)
          .build()
        val df = spark.read.nebula(nebulaConnectionConfig, nebulaReadVertexConfig).loadVerticesToDF()
        val count = df.count()

        val nebulaWriteVertexConfig = WriteNebulaVertexConfig
          .builder()
          .withSpace(spaceName)
          .withTag(tagName)
          .withVidField("_vertexId")
          //          .withVidPolicy("hash")
          .withVidAsProp(false)
          .withUser(t_user)
          .withPasswd(t_pass)
          .withBatch(512)
          .withWriteMode(WriteMode.INSERT)
          .withOverwrite(true)
          .build()
        df.write.nebula(t_nebulaConnectionConfig, nebulaWriteVertexConfig).writeVertices()
        LOG.info(log_flag + "Wrote tag " + spaceName + "." + tagName + ": " + df.count() + " to nebulagraph")
      }

      resultSet = session.execute("SHOW EDGES")
      val edges = resultSet.colValues(resultSet.getColumnNames.get(0))
      for (edge <- edges.asScala) {
        val edgeName = edge.asString()
        val nebulaReadEdgeConfig = ReadNebulaConfig
          .builder()
          .withUser(user)
          .withPasswd(pass)
          .withSpace(spaceName)
          .withLabel(edgeName)
          .withPartitionNum(1)
          .build()
        val df = spark.read.nebula(nebulaConnectionConfig, nebulaReadEdgeConfig).loadEdgesToDF()
        val count = df.count()
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
          .withUser(t_user)
          .withPasswd(t_pass)
          .withBatch(512)
          .build()
        df.write.nebula(t_nebulaConnectionConfig, nebulaWriteEdgeConfig).writeEdges()
        LOG.info(log_flag + "Wrote edge " + spaceName + "." + edgeName + ": " + df.count() + " to nebulagraph")
      }
    }
    LOG.info(log_flag + "Nebula2Nebula done")

    session.close()
    nebulaPool.close()
    spark.close()
  }
}


