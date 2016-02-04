/**
 * (C) Copyright IBM Corp. 2010, 2015
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.spark.netezza

import java.io._
import java.nio.file.Files
import java.sql._
import java.util._

import org.slf4j.LoggerFactory

/**
 * Helper class to create names pipe, and Thread to execute external table statement.
 */
object NetezzaUtils {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Creates a named pipe on the local node used to transfer the data between
   * Netezza host , and the this spark data source.
   *
   * @return file handle to the named pipe.
   */
  @throws(classOf[IOException])
  def createPipe(): File = {

    val pipeName = "spark-netezza-" + java.util.UUID.randomUUID().toString()
    // Using the java default temporary directory to create the names pipes.
    val tempFile: File = Files.createTempFile(pipeName, "_pipe").toFile
    tempFile.delete()
    val pipe: File = new File(tempFile.getParent(), pipeName)

    // attempt to create it
    val r: Runtime = Runtime.getRuntime()
    try {
      val createPipeProcess: Process = r.exec("mkfifo " + pipe + " -m 0600")
      val pExitCode = createPipeProcess.waitFor()
      log.debug("mkfifo exit status code: " + Integer.toString(pExitCode));
    } catch {
      case e @ (_ : InterruptedException | _ : IOException) => {
        log.error("Unable to create named pipe:" + pipe , e)
        throw new IOException("Unable to create named pipe:" + pipe , e)
      }
    }
    if (!pipe.exists()) {
      throw new IOException("Unable to create named pipe:" + pipe)
    }
    pipe
  }

  /**
   * Thread to run create external table to make Netezza host write data in parallel
   * to the reading by the RDD,
   */
  class StatementExecutorThread(conn: Connection,
                                pstmt: PreparedStatement,
                                writePipe: File) extends Thread {

    var hasExceptionrOccured: Boolean = false
    var exception: Exception = null

    /*
     * Set to let the external table execution thread to know reader
     * has existed early. Ignore any exception thrown from Netezza
     * as result.
     */
    var receivedEarlyOut = false

    def setError(ex: Exception): Unit = {
      hasExceptionrOccured = true
      exception = ex
    }

    override def run(): Unit = {
      log.info("executing external table statement to transfer the data.")
      try {
        conn.setAutoCommit(false)
        pstmt.execute()
        conn.commit()
      }
      catch {
        case e: SQLException => {
          if (!receivedEarlyOut) {
            log.error("Error creating external table pipe: " + e.toString)
            setError(e)
          }
          else {
            try conn.rollback()
            catch {
              case _: Throwable => log.warn(
                "Error rolling back create external table: " + e.toString)
            }
          }
        }
        case e: RuntimeException => {
          if (!receivedEarlyOut) {
            setError(e)
          }
        }
      }
      finally {
        if (hasExceptionrOccured) {
          // if the Netezza host did not open the pipe due to errors, the reader will wait
          // forever. Open and close the pipe to avoid the reader blocking forever.
          if (writePipe != null) {
            try {
              val fos: FileOutputStream = new FileOutputStream(writePipe)
              fos.close();
            } catch {
              case ioe: IOException => log.warn("Failed to close the pipe:" + writePipe)
            }
          }
        }
      }
    }
  }
}
