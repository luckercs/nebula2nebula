import com.vesoft.nebula.client.graph.data.ValueWrapper
import org.slf4j.LoggerFactory

import java.io.{File, FileWriter}
import java.util
import scala.collection.JavaConverters._

object NebulaSchema {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val optionsProcessor = new OptionsProcessor()
    val commandLine = optionsProcessor.parse(args)
    val graphd = commandLine.getOptionValue("graphd", "127.0.0.1:9669")
    val user = commandLine.getOptionValue("user", "root")
    val pass = commandLine.getOptionValue("pass", "nebula")

    val nebulaPool = new GraphNebulaPool(graphd, user, pass)
    val session = nebulaPool.getSession()
    var resultSet = session.execute("SHOW SPACES")
    if (!resultSet.isSucceeded) {
      LOG.error("Failed to exec statement")
      System.exit(1)
    }
    val spaces: util.List[ValueWrapper] = resultSet.colValues(resultSet.getColumnNames.get(0))

    val schemafilename = "schemas.ngql"
    val file = new File(schemafilename)
    if (file.exists()) file.delete()
    var fileWriter = new FileWriter(schemafilename, true)

    for (space <- spaces.asScala) {
      val spaceName = space.asString()
      LOG.info("Processing space: " + spaceName + "...")
      Thread.sleep(500)
      resultSet = session.execute("USE " + spaceName)
      if (!resultSet.isSucceeded) {
        LOG.error("Failed to exec statement")
        System.exit(1)
      }

      Thread.sleep(500)
      resultSet = session.execute("SHOW CREATE SPACE `" + spaceName + "`")
      if (!resultSet.isSucceeded) {
        LOG.error("Failed to exec statement")
        System.exit(1)
      }
      val spaceCreateStat = resultSet.rowValues(0).get(1).asString()
      fileWriter.write(spaceCreateStat + ";\n\n")
      fileWriter.write("USE " + spaceName + ";\n\n")

      Thread.sleep(500)
      resultSet = session.execute("SHOW TAGS")
      if (!resultSet.isSucceeded) {
        LOG.error("Failed to exec statement")
        System.exit(1)
      }
      val tags = resultSet.colValues(resultSet.getColumnNames.get(0))

      for (tag <- tags.asScala) {
        val tagName = tag.asString()
        Thread.sleep(500)
        resultSet = session.execute("SHOW CREATE TAG `" + tagName + "`")
        if (!resultSet.isSucceeded) {
          LOG.error("Failed to exec statement")
          System.exit(1)
        }
        val tagCreateStat = resultSet.rowValues(0).get(1).asString()
        fileWriter.write(tagCreateStat + ";\n\n")
      }

      Thread.sleep(500)
      resultSet = session.execute("SHOW EDGES")
      if (!resultSet.isSucceeded) {
        LOG.error("Failed to exec statement")
        System.exit(1)
      }
      val edges = resultSet.colValues(resultSet.getColumnNames.get(0))

      for (edge <- edges.asScala) {
        val edgeName = edge.asString()
        Thread.sleep(500)
        resultSet = session.execute("SHOW CREATE EDGE `" + edgeName + "`")
        if (!resultSet.isSucceeded) {
          LOG.error("Failed to exec statement")
          System.exit(1)
        }
        val tagCreateStat = resultSet.rowValues(0).get(1).asString()
        fileWriter.write(tagCreateStat + ";\n\n")
      }

      Thread.sleep(500)
      resultSet = session.execute("SHOW TAG INDEXES")
      if (!resultSet.isSucceeded) {
        LOG.error("Failed to exec statement")
        System.exit(1)
      }
      val tagIndexes = resultSet.colValues(resultSet.getColumnNames.get(0))

      for (tagIndex <- tagIndexes.asScala) {
        val tagIndexName = tagIndex.asString()
        Thread.sleep(500)
        resultSet = session.execute("SHOW CREATE TAG INDEX `" + tagIndexName + "`")
        if (!resultSet.isSucceeded) {
          LOG.error("Failed to exec statement")
          System.exit(1)
        }
        val tagIndexCreateStat = resultSet.rowValues(0).get(1).asString()
        fileWriter.write(tagIndexCreateStat + ";\n\n")
      }

      Thread.sleep(500)
      resultSet = session.execute("SHOW EDGE INDEXES")
      if (!resultSet.isSucceeded) {
        LOG.error("Failed to exec statement")
        System.exit(1)
      }
      val edgeIndexes = resultSet.colValues(resultSet.getColumnNames.get(0))

      for (edgeIndex <- edgeIndexes.asScala) {
        val edgeIndexName = edgeIndex.asString()
        Thread.sleep(500)
        resultSet = session.execute("SHOW CREATE EDGE INDEX `" + edgeIndexName + "`")
        if (!resultSet.isSucceeded) {
          LOG.error("Failed to exec statement")
          System.exit(1)
        }
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
}
