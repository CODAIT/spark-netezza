package com.ibm.spark.netezza

import java.io._
import java.lang.String._
import java.sql._
import java.util._

import org.slf4j.LoggerFactory

/**
* @author Xiu Guo
*/
object NetezzaUtils {
  private val log = LoggerFactory.getLogger(getClass)

  def createPipe(): File = {

    var pExitCode: Int = 0

    val pipeDir = createTempDir("read_pipe" + java.util.UUID.randomUUID().toString(),
      String.valueOf(System.currentTimeMillis()))

    val pipeName: String = {
      new String("slice-pipe" + java.util.UUID.randomUUID ().toString ())
    }

    // created named pipe, check that the file exists
    val pipe: File = new File(pipeDir + "/" + pipeName)

    log.debug("Checking existence of pipe " + pipeName)
    var exists = pipe.exists()

    if (!exists) {
      log.debug("Creating pipe...")
      // attempt to create it
      val r: Runtime = Runtime.getRuntime()
      try {
        val createPipeProcess: Process = r.exec("mkfifo " + pipe)
        pExitCode = createPipeProcess.waitFor()
        log.debug("mkfifo exit code: " + Integer.toString(pExitCode));
      } catch {
        case e: IOException => log.warn("Unable to create pipe = '" + pipeName + "' "
          + e.getMessage())
        case e: InterruptedException => log.warn("Unable to create pipe = '" + pipeName + "' "
          + e.getMessage())
      }
    }

    exists = pipe.exists()

    if (!exists) {
      log.warn("Pipe does not exist and attempt to create it failed.")
    }
    return pipe
  }

  @throws(classOf[IOException])
  def createTempDir(prefix: String, suffix: String): File = {

    val f: File = File.createTempFile(prefix, suffix) // use default unix temp dir
    f.deleteOnExit()
    f.delete()
    if (!f.mkdir()) throw new IOException("Could not create local temp directory " + f)
    else f
  }

  /**
   * Get the name and type of all the columns in the table. The type string
   * has the precision and scale for the types that need it. For example:
   * VARCHAR(255)
   *
   * @param tablename
   * @param fieldNames
   * @param conn
   */
  @throws(classOf[SQLException])
  @throws(classOf[IOException])
  def getColumnTypes(tablename: String, fieldNames: Seq[String], conn: Connection): Map[String, String] = {

    val columns: StringBuilder = new StringBuilder()
    var i = 0
    for (i <- 0 to fieldNames.length) {
      columns ++= fieldNames(i)
      if (i != fieldNames.length - 1) {
        columns ++= ", "
      }
    }

    val query = "SELECT " + columns.toString() + " FROM " + tablename + " WHERE 1=0"
    log.info("getColumnTypes: " + query)
    var stmt: PreparedStatement = null
    var results: ResultSet = null
    var colTypes: Map[String, String] = new HashMap[String, String]()

    try {

      stmt = conn.prepareStatement(query)
      results = stmt.executeQuery()

      val cols: Int = results.getMetaData().getColumnCount()
      val metadata: ResultSetMetaData = results.getMetaData()
      var i: Int = 0
      for (i <- 0 to cols + 1) {
        var typeName: String = metadata.getColumnTypeName(i)
        val colName: String = metadata.getColumnName(i)
        val precision: Int = metadata.getPrecision(i)
        val scale: Int = metadata.getScale(i)
        if (precision > 0) {
          typeName ++= "(" + precision
          if (scale > 0) {
            typeName ++= "," + scale + ")"
          } else {
            typeName ++= ")"
          }
        }
        colTypes.put(new String(colName), new String(typeName))
      }
    } finally {
      try {
        if (results != null)
          results.close()
        if (stmt != null)
          stmt.close()
      } catch {
        case _: SQLException =>
      }
    }
    log.info("column names and types: " + colTypes.toString())
    colTypes
  }

  /**
   * Thread to run create external table
   */
  class StatementExecutorThread (conn: Connection, pstmt: PreparedStatement) extends Thread {

    var executionResult: Boolean = false

    var writePipe: File = null
    //var readPipe: File = null
    var receivedEarlyOut: Boolean = false
    var error: Boolean = false
    var e: Exception = null

    def getExecutionResult = executionResult

    def setWritePipe(writePipe: File): Unit = {
      this.writePipe = writePipe
    }

    //def setReadPipe(readPipe: File): Unit = {
    //  this.readPipe = readPipe
    //}

    def setEarlyOut(): Unit = {
      receivedEarlyOut = true
    }

    def setError(e1: Exception): Unit = {
      this.error = true
      this.e = e1
    }

    //def getError(): Exception = e

    //def isErrored(): Boolean = error

    override def run(): Unit = {
      log.info("running create table...")
      try {
        conn.setAutoCommit(false)
        executionResult = pstmt.execute()
        if (!conn.getAutoCommit()) conn.commit()
      }
      catch {
        case e: SQLException => {
          if (!receivedEarlyOut) log.error("Error creating external table pipe.")
          else
          {
            try conn.rollback()
            catch {
              case _: Throwable => log.error("Error rolling back create external table.")
            }
            setError(e)
          }
        }
        case e: RuntimeException => {
          if (!receivedEarlyOut) {
            // this is a work-around for a Netezza issue
            // INTERVAL is not supported with data load - the driver will
            // throw a NPE, which causes ou main thread in return to never
            // return from the FileOutputStream open since Netezza is not
            // opening the other end of the pipe. We do this here in a
            // desperate attempt
            // to get the main thread back
            if (writePipe != null) {
              try {
                new FileInputStream(writePipe).close()
              }
              catch {
                case e1: Throwable => e1.printStackTrace()
              }
            }
            //if (readPipe != null) try new FileOutputStream(readPipe).close()
            //catch {
            //  case e1: Throwable => e1.printStackTrace()
            //}
            throw e
          }
        }
      }
      finally {
        if (receivedEarlyOut) return
      }
    }
  }
}
