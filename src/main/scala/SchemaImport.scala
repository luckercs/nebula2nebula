import com.vesoft.nebula.client.graph.net.Session
import org.slf4j.LoggerFactory

import java.io.File
import scala.io.Source

object SchemaImport {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val optionsProcessor = new OptionsProcessor()
    val commandLine = optionsProcessor.parse(args)
    val graphd = commandLine.getOptionValue("graphd", "127.0.0.1:9669")
    val user = commandLine.getOptionValue("user", "root")
    val pass = commandLine.getOptionValue("pass", "nebula")

    val nebulaPool = new GraphNebulaPool(graphd, user, pass)
    val session: Session = nebulaPool.getSession()

    val schemafilename = "schemas.ngql"
    val file = new File(schemafilename)
    if (!file.exists()) {
      LOG.error("Schema file not found: " + schemafilename)
      System.exit(1)
    }
    val statements = splitByEmptyLines(schemafilename)
    for (statement <- statements) {
      execStatement(session, statement, 5)
    }

    session.close()
    nebulaPool.close()
    LOG.info("Schema import done")
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

  def splitByEmptyLines(filePath: String): Seq[String] = {
    val content = Source.fromFile(filePath).mkString
    content
      .split("\\n\\s*\\n")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList
  }
}
