package com.ibm.spark.netezza

import java.io.{FileInputStream, InputStreamReader, BufferedReader}
import java.sql.{PreparedStatement, Connection}


import org.apache.spark.Partition
import org.apache.spark.sql.sources.Filter
import org.slf4j.LoggerFactory

/**
 * Creates reader for the given partitions.
 *
 * @author suresh thalamati
 */
class NetezzaDataReader(conn: Connection,
                        table: String,
                        columns: Array[String],
                        filters: Array[Filter],
                        partition: NetezzaPartition) extends Iterator[NetezzaRecord] {

  private val log = LoggerFactory.getLogger(getClass)

  val pipe = NetezzaUtils.createPipe();

  // thread for creating table
  var execThread:NetezzaUtils.StatementExecutorThread =null
  var closed:Boolean = false

  var input:BufferedReader = null
  var fis:FileInputStream = null
  var isr:InputStreamReader = null
  var nextLine:String = null;
  var nextRecord:NetezzaRecord = null
  var recordParser:NetezzaRecordParser = null


  val escapeChar:Char = '\\';
  val delimiter:Char = '\001';

  val baseQuery = {
    val whereClause = NetezzaFilters.getWhereClause(filters, partition)
    val colStrBuilder = new StringBuilder()
    colStrBuilder.append(columns(0))
    columns.drop(1).foreach(col => colStrBuilder.append(",").append(col))
     s"SELECT $colStrBuilder FROM $table $whereClause"
  }
    // build external table initialized by base query
    var query:StringBuilder  = new StringBuilder()
    query.append("CREATE EXTERNAL TABLE '" + pipe + "'")
    query.append(" USING (delimiter '" + delimiter + "' ")
    query.append(" escapeChar '" + escapeChar + "' ")

    query.append(" REMOTESOURCE 'JDBC' NullValue 'null' BoolStyle 'T_F'")
    query.append(")")
    query.append(" AS " + baseQuery.toString() + " ")

    log.info("Query: " + query.toString())

    // start the thread that will populate the pipe
    val stmt:PreparedStatement  = conn.prepareStatement(query.toString())
    execThread = new NetezzaUtils.StatementExecutorThread(conn, stmt);
    execThread.setWritePipe(pipe);
    log.info("start thread to create table..");
    execThread.start();

    // set up the input stream
    fis = new FileInputStream(pipe)
    isr = new InputStreamReader(fis)
    input = new BufferedReader(isr)

    recordParser = new NetezzaRecordParser(delimiter, escapeChar)



  /**
   * Returns true if there are record in the pipe, otherwise false.
   */
  override def hasNext: Boolean = {
    val record: Option[NetezzaRecord] = getNextRecord()
    record match {
      case Some(_) =>
        nextRecord = record.get
        true
      case None => false
    }
  }

  override def next(): NetezzaRecord =  {
    nextRecord
  }


  private def getNextRecord():Option[NetezzaRecord] ={

    nextLine  = input.readLine()

    // the end of the data when row is null
    if (nextLine == null) {
      close()
      None
    } else {
      val row: Array[String] = recordParser.parse(nextLine)
      Some(new NetezzaRecord(row))
    }

  }


  def close() = {
    if (!closed) {
      execThread.setEarlyOut()
      stmt.close()
      closePipe()
      closeInputStream()
      execThread.join()
      closed = true
    }
  }

  def closeInputStream()  {
    log.info("close in stream ");
    if (fis != null) {
      fis.close();
      fis = null;
    }

    if (isr != null) {
      isr.close();
      isr = null;
    }

    if (input != null) {
      input.close();
      input = null;
    }
  }

  def closePipe() {
    log.info("close pipe ");
    if (pipe != null && pipe.exists()) {
      pipe.delete();
    }
  }

}
