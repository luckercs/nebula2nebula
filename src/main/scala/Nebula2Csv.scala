import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Nebula2Csv {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  private val log_flag = "======================="

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    sparkConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    val spark = SparkSession.builder
      .appName("Nebula2Csv")
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
    createOrReplaceDirectory(csvDir)

    val nebulaConnectionConfig = NebulaConnectionConfig.builder()
      .withMetaAddress(metad)
      .withGraphAddress(graphd)
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
        df.repartition(1)
          .write
          .format("csv")
          .option("header", "true")
          .option("delimiter", delimiter)
          .mode(SaveMode.Overwrite)
          .save(csvDir + "/tag__" + spaceName + "__" + tagName)
        LOG.info(log_flag + "Wrote " + count + " tag records to " + csvDir + "/tag__" + spaceName + "__" + tagName)
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
        df.repartition(1)
          .write
          .format("csv")
          .option("header", "true")
          .option("delimiter", delimiter)
          .mode(SaveMode.Overwrite)
          .save(csvDir + "/edge__" + spaceName + "__" + edgeName)
        LOG.info(log_flag + "Wrote " + count + " edge records to " + csvDir + "/edge__" + spaceName + "__" + edgeName)
      }
    }
    LOG.info(log_flag + "Nebula2Csv done")

    val rootDir = new File(csvDir)
    val csvFiles = collectCsvFiles(rootDir)
    moveAndRenameCsvFiles(csvFiles, rootDir)
    deleteSubdirectories(rootDir)

    session.close()
    nebulaPool.close()
    spark.close()
  }

  def createOrReplaceDirectory(path: String): Unit = {
    val dir = new File(path)

    if (dir.exists()) {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          Option(file.listFiles).foreach(_.foreach(deleteRecursively))
        }
        if (!file.delete()) {
          throw new RuntimeException(s"无法删除文件: ${file.getAbsolutePath}")
        }
      }

      deleteRecursively(dir)
    }

    if (!dir.mkdirs()) {
      throw new RuntimeException(s"无法创建目录: ${dir.getAbsolutePath}")
    }
  }


  def collectCsvFiles(dir: File): List[File] = {
    val result = ListBuffer[File]()

    if (dir.isDirectory) {
      Option(dir.listFiles()).getOrElse(Array.empty).foreach { file =>
        if (file.isDirectory) {
          result ++= collectCsvFiles(file)
        } else if (file.isFile && file.getName.toLowerCase.endsWith(".csv")) {
          result += file
        }
      }
    }

    result.toList
  }

  def moveAndRenameCsvFiles(csvFiles: List[File], targetDir: File): Unit = {
    csvFiles.foreach { csvFile =>
      try {
        val parentDirName = csvFile.getParentFile.getName
        val newFileName = s"${parentDirName}.csv"
        val targetFile = new File(targetDir, newFileName)
        val uniqueTargetFile = makeFileNameUnique(targetFile)

        if (csvFile.renameTo(uniqueTargetFile)) {
          LOG.info(s"成功: 将 '${csvFile.getAbsolutePath}' 重命名并移动为 '${uniqueTargetFile.getName}'")
        } else {
          throw new RuntimeException(s"失败: 无法将 '${csvFile.getAbsolutePath}' 移动到 '${uniqueTargetFile.getName}'")
        }
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"错误: 处理文件 '${csvFile.getAbsolutePath}' 时出错: ${e.getMessage}")
      }
    }
  }

  def makeFileNameUnique(file: File): File = {
    if (!file.exists()) return file

    var counter = 1
    val name = file.getName
    val extension = if (name.contains(".")) name.substring(name.lastIndexOf(".")) else ""
    val baseName = if (name.contains(".")) name.substring(0, name.lastIndexOf(".")) else name

    var uniqueFile = new File(file.getParent, s"${baseName}_${counter}${extension}")

    while (uniqueFile.exists()) {
      counter += 1
      uniqueFile = new File(file.getParent, s"${baseName}_${counter}${extension}")
    }

    uniqueFile
  }

  def deleteSubdirectories(rootDir: File): Unit = {
    Option(rootDir.listFiles()).getOrElse(Array.empty).foreach { file =>
      if (file.isDirectory) {
        deleteDirectoryRecursively(file)
      }
    }
  }

  def deleteDirectoryRecursively(dir: File): Boolean = {
    if (dir.isDirectory) {
      Option(dir.listFiles()).getOrElse(Array.empty).foreach { file =>
        if (!deleteDirectoryRecursively(file)) {
          return false
        }
      }
    }
    dir.delete()
  }
}


