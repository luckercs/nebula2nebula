import com.vesoft.nebula.client.graph.data.{ResultSet, ValueWrapper}
import com.vesoft.nebula.client.graph.net.Session
import org.slf4j.LoggerFactory

import java.io.{File, FileWriter}
import java.util
import scala.collection.JavaConverters._

object SchemaExport {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val optionsProcessor = new OptionsProcessor()
    val commandLine = optionsProcessor.parse(args)
    val graphd = commandLine.getOptionValue("graphd", "127.0.0.1:9669")
    val user = commandLine.getOptionValue("user", "root")
    val pass = commandLine.getOptionValue("pass", "nebula")

    val nebulaPool = new GraphNebulaPool(graphd, user, pass)
    val session = nebulaPool.getSession()

    var resultSet = execStatementWithRes(session, "SHOW SPACES", 5)
    val spaces: util.List[ValueWrapper] = resultSet.colValues(resultSet.getColumnNames.get(0))

    val schemafilename = "schemas.ngql"
    val file = new File(schemafilename)
    if (file.exists()) file.delete()
    var fileWriter = new FileWriter(schemafilename, true)

    for (space <- spaces.asScala) {
      val spaceName = space.asString()
      LOG.info("Processing space: " + spaceName + "...")
      execStatement(session, "USE " + spaceName, 5)

      resultSet = execStatementWithRes(session, "SHOW CREATE SPACE `" + spaceName + "`", 5)
      val spaceCreateStat = resultSet.rowValues(0).get(1).asString()
      fileWriter.write(spaceCreateStat + ";\n\n")
      fileWriter.write("USE " + spaceName + ";\n\n")

      resultSet = execStatementWithRes(session, "SHOW TAGS", 5)
      val tags = resultSet.colValues(resultSet.getColumnNames.get(0))

      for (tag <- tags.asScala) {
        val tagName = tag.asString()
        resultSet = execStatementWithRes(session, "SHOW CREATE TAG `" + tagName + "`", 5)
        val tagCreateStat = resultSet.rowValues(0).get(1).asString()
        fileWriter.write(tagCreateStat + ";\n\n")
      }

      resultSet = execStatementWithRes(session, "SHOW EDGES", 5)
      val edges = resultSet.colValues(resultSet.getColumnNames.get(0))

      for (edge <- edges.asScala) {
        val edgeName = edge.asString()
        resultSet = execStatementWithRes(session, "SHOW CREATE EDGE `" + edgeName + "`", 5)
        val tagCreateStat = resultSet.rowValues(0).get(1).asString()
        fileWriter.write(tagCreateStat + ";\n\n")
      }

      resultSet = execStatementWithRes(session, "SHOW TAG INDEXES", 5)
      val tagIndexes = resultSet.colValues(resultSet.getColumnNames.get(0))

      for (tagIndex <- tagIndexes.asScala) {
        val tagIndexName = tagIndex.asString()
        resultSet = execStatementWithRes(session, "SHOW CREATE TAG INDEX `" + tagIndexName + "`", 5)
        val tagIndexCreateStat = resultSet.rowValues(0).get(1).asString()
        fileWriter.write(tagIndexCreateStat + ";\n\n")
      }

      resultSet = execStatementWithRes(session, "SHOW EDGE INDEXES", 5)
      val edgeIndexes = resultSet.colValues(resultSet.getColumnNames.get(0))

      for (edgeIndex <- edgeIndexes.asScala) {
        val edgeIndexName = edgeIndex.asString()
        resultSet = execStatementWithRes(session, "SHOW CREATE EDGE INDEX `" + edgeIndexName + "`", 5)
        val tagIndexCreateStat = resultSet.rowValues(0).get(1).asString()
        fileWriter.write(tagIndexCreateStat + ";\n\n")
      }
      fileWriter.write("\n\n")
    }
    fileWriter.close()
    session.close()
    nebulaPool.close()
    LOG.info("Schema exported done, see " + schemafilename)
  }

  def execStatement(session: Session, statement: String, retryCount: Int): Unit = {
    LOG.info("Executing statement: " + statement)
    var currentRetry = 0
    var success = false
    while (currentRetry < retryCount && !success) {
      val resultSet = session.execute(statement)
      if (resultSet.isSucceeded) {
        success = true
      } else {
        if (currentRetry < retryCount - 1) {
          Thread.sleep(2000)
        }
      }
      currentRetry += 1
    }
    if (!success) {
      LOG.error("Failed to exec statement: " + statement)
      System.exit(1)
    }
  }

  def execStatementWithRes(session: Session, statement: String, retryCount: Int): ResultSet = {
    LOG.info("Executing statement: " + statement)
    var currentRetry = 0
    var success = false
    var resultSet: ResultSet = null
    while (currentRetry < retryCount && !success) {
      resultSet = session.execute(statement)
      if (resultSet.isSucceeded) {
        success = true
      } else {
        if (currentRetry < retryCount - 1) {
          Thread.sleep(2000)
        }
      }
      currentRetry += 1
    }
    if (!success) {
      LOG.error("Failed to exec statement: " + statement)
      System.exit(1)
    }
    resultSet
  }
}
