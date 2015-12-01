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
import java.sql._
import java.util._

import org.slf4j.LoggerFactory

object NetezzaUtils {
  private val log = LoggerFactory.getLogger(getClass)

  def createPipe(): File = {

    var pExitCode: Int = 0

    val pipeDir = createTempDir("read_pipe" + java.util.UUID.randomUUID().toString(),
      String.valueOf(System.currentTimeMillis()))

    val pipeName: String = {
      new String("slice-pipe" + java.util.UUID.randomUUID().toString())
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
              val fos: FileOutputStream = new FileOutputStream(writePipe);
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
