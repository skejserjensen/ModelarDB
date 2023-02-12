/* Copyright 2021 The ModelarDB Contributors
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
 */
package dk.aau.modelardb.storage

import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Dimensions, SegmentGroup}
import dk.aau.modelardb.engines.spark.Spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type}

import org.h2.table.TableFilter

import java.io.FileNotFoundException
import java.{lang, util}

import scala.collection.mutable

class ParquetStorage(rootFolder: String) extends FileStorage(rootFolder) {

  /** Protected Methods **/
  //FileStorage
  override protected def getFileSuffix: String = ".parquet"

  override protected def getMaxID(columnName: String, timeSeriesFilePath: Path): Int = {
    val fieldIndex = columnName match {
      case "tid" => 0
      case "gid" => 3
      case _ => throw new IllegalArgumentException("ModelarDB: unable to get the maximum id for column " + columnName)
    }
    val reader = try {
      getReader(timeSeriesFilePath)
    } catch {
      case _: FileNotFoundException => return 0
    }
    var id = 0
    val schema = reader.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    var pages = reader.readNextRowGroup()
    while (pages != null) {
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        val simpleGroup = recordReader.read.asInstanceOf[SimpleGroup]
        id = Math.max(id, simpleGroup.getInteger(fieldIndex, 0))
      }
      pages = reader.readNextRowGroup()
    }
    reader.close()
    id
  }

  override protected def mergeFiles(outputFilePath: Path, inputFilesPaths: mutable.ArrayBuffer[Path]): Unit = {
    //NOTE: merge assumes all inputs share the same schema
    val inputPathsScala = inputFilesPaths
    val reader = getReader(inputFilesPaths(0))
    val schema = reader.getFooter.getFileMetaData.getSchema
    reader.close()

    //Write the new file
    val writer = getWriter(outputFilePath, schema)
    for (inputPath <- inputPathsScala) {
      val reader = getReader(inputPath)
      val columnIO = new ColumnIOFactory().getColumnIO(schema)
      var pages = reader.readNextRowGroup()

      while (pages != null) {
        val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
        for (_ <- 0L until pages.getRowCount) {
          writer.write(recordReader.read)
        }
        pages = reader.readNextRowGroup()
      }
      reader.close()
    }
    writer.close()
  }

  override protected def writeTimeSeriesFile(timeSeriesGroupRows: Array[Array[Object]], timeSeriesFilePath: Path): Unit = {
    val columns = new util.ArrayList[Type]()
    columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "tid"))
    columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "scaling_factor"))
    columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "sampling_interval"))
    columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "gid"))

    val dimensionTypes = this.dimensions.getTypes
    for (dimi <- this.dimensions.getColumns.zipWithIndex) {
      dimensionTypes(dimi._2) match {
        case Dimensions.Types.TEXT => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, dimi._1))
        case Dimensions.Types.INT => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, dimi._1))
        case Dimensions.Types.LONG => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, dimi._1))
        case Dimensions.Types.FLOAT => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, dimi._1))
        case Dimensions.Types.DOUBLE => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, dimi._1))
      }
    }
    val schema = new MessageType("time_series", columns)

    val writer = getWriter(timeSeriesFilePath, schema)
    for (row <- timeSeriesGroupRows) {
      val group = new SimpleGroup(schema)
      for (elementAndIndex <- row.zipWithIndex) {
        elementAndIndex._1 match {
          case elem: lang.String => group.add(elementAndIndex._2, elem)
          case elem: lang.Integer => group.add(elementAndIndex._2, elem)
          case elem: lang.Long => group.add(elementAndIndex._2, elem)
          case elem: lang.Float => group.add(elementAndIndex._2, elem)
          case elem: lang.Double => group.add(elementAndIndex._2, elem)
        }
      }
      writer.write(group)
    }
    writer.close()
  }

  override protected def readTimeSeriesFile(timeSeriesFilePath: Path): mutable.HashMap[Integer, Array[Object]] = {
    val columnsInNormalizedDimensions = dimensions.getColumns.length
    val timeSeriesInStorage = mutable.HashMap[Integer, Array[Object]]()
    val timeSeries = getReader(timeSeriesFilePath)
    var pages = timeSeries.readNextRowGroup()
    val schema = timeSeries.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    while (pages != null) {
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
        val group = recordReader.read()
        val metadata = mutable.ArrayBuffer[Object]()
        metadata += group.getFloat(1, 0).asInstanceOf[Object]
        metadata += group.getInteger(2, 0).asInstanceOf[Object]
        metadata += group.getInteger(3, 0).asInstanceOf[Object]

        //Dimensions
        var column = 4
        val dimensionTypes = dimensions.getTypes
        while (column < columnsInNormalizedDimensions + 4) {
          dimensionTypes(column - 4) match {
            case Dimensions.Types.TEXT => metadata += group.getString(column, 0)
            case Dimensions.Types.INT => metadata += group.getInteger(column, 0).asInstanceOf[Object]
            case Dimensions.Types.LONG => metadata += group.getLong(column, 0).asInstanceOf[Object]
            case Dimensions.Types.FLOAT => metadata += group.getFloat(column, 0).asInstanceOf[Object]
            case Dimensions.Types.DOUBLE => metadata += group.getDouble(column, 0).asInstanceOf[Object]
          }
          column += 1
        }
        timeSeriesInStorage.put(group.getInteger(0, 0), metadata.toArray)
      }
      pages = timeSeries.readNextRowGroup()
    }
    timeSeries.close()
    timeSeriesInStorage
  }

  override protected def writeModelTypeFile(modelsToInsert: mutable.HashMap[String,Integer],
                                            modelTypeFilePath: Path): Unit = {
    val schema = new MessageType("model_type",
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "mtid"),
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "name"))

    val writer = getWriter(modelTypeFilePath, schema)
    for ((k, v) <- modelsToInsert) {
      val group = new SimpleGroup(schema)
      group.add(0, v.intValue())
      group.add(1, k)
      writer.write(group)
    }
    writer.close()
  }

  override protected def readModelTypeFile(modelTypeFilePath: Path): mutable.HashMap[String, Integer] = {
    val modelsInStorage = new mutable.HashMap[String, Integer]()
    val modelTypes = getReader(modelTypeFilePath)
    var pages = modelTypes.readNextRowGroup()
    val schema = modelTypes.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    while (pages != null) {
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        val group = recordReader.read()
        val mtid = group.getInteger(0, 0)
        val name = group.getString(1, 0)
        modelsInStorage.put(name, mtid)
      }
      pages = modelTypes.readNextRowGroup()
    }
    modelTypes.close()
    modelsInStorage
  }

  //FileStorage - H2Storage
  override protected def writeSegmentGroupFile(segmentGroups: Array[SegmentGroup], segmentGroupFile: Path): Unit = {
    val writer = getWriter(segmentGroupFile, this.segmentGroupSchema)
    for (segmentGroup <- segmentGroups) {
      val group = new SimpleGroup(this.segmentGroupSchema)
      group.add(0, segmentGroup.gid)
      group.add(1, segmentGroup.startTime)
      group.add(2, segmentGroup.endTime)
      group.add(3, segmentGroup.mtid)
      group.add(4, Binary.fromConstantByteArray(segmentGroup.model))
      group.add(5, Binary.fromConstantByteArray(segmentGroup.offsets))
      writer.write(group)
    }
    writer.close()
  }

  override protected def readSegmentGroupsFiles(filter: TableFilter,
                                                segmentGroupFiles: mutable.ArrayBuffer[Path]): Iterator[SegmentGroup] = {
    Static.warn("ModelarDB: projection and predicate push-down is not yet implemented")
    new Iterator[SegmentGroup] {
      /** Instance Variables **/
      private val segmentFiles = segmentGroupFiles.iterator
      private var segmentFile: ParquetFileReader = _
      private var columnIO: MessageColumnIO = _
      private var pages: PageReadStore = _
      private var recordReader: RecordReader[Group] = _
      private var rowCount: Long = _
      private var rowIndex: Long = _
      nextFile()

      /** Public Methods **/
      override def hasNext: Boolean = {
        //The current batch contain additional rows
        if (this.rowIndex < this.rowCount) {
          return true
        }

        //The current file contain additional batches
        this.pages = this.segmentFile.readNextRowGroup()
        if (this.pages != null) {
          this.recordReader = this.columnIO.getRecordReader(this.pages, new GroupRecordConverter(segmentGroupSchema))
          this.rowCount = this.pages.getRowCount
          this.rowIndex = 0L
          return true
        }

        //There are more files to read
        this.segmentFile.close()
        if (this.segmentFiles.hasNext) {
          nextFile()
          return true
        }

        //All of the data in the file have been read
        false
      }

      override def next(): SegmentGroup = {
        val simpleGroup = this.recordReader.read
        val gid = simpleGroup.getInteger(0, 0)
        val startTime = simpleGroup.getLong(1, 0)
        val endTime = simpleGroup.getLong(2, 0)
        val mtid = simpleGroup.getInteger(3, 0)
        val model = simpleGroup.getBinary(4, 0).getBytesUnsafe
        val gaps = simpleGroup.getBinary(5, 0).getBytesUnsafe
        this.rowIndex += 1
        new SegmentGroup(gid, startTime, endTime, mtid, model, gaps)
      }

      /** Private Methods **/
      private def nextFile(): Unit = {
        this.segmentFile = getReader(segmentFiles.next())
        this.columnIO = new ColumnIOFactory().getColumnIO(segmentGroupSchema)
        this.pages = segmentFile.readNextRowGroup()
        this.recordReader = columnIO.getRecordReader(this.pages, new GroupRecordConverter(segmentGroupSchema))
        this.rowCount = this.pages.getRowCount
        this.rowIndex = 0L
      }
    }
  }

  //FileStorage - SparkStorage
  override protected def writeSegmentGroupsFolder(sparkSession: SparkSession, df: DataFrame, segmentGroupFolder: String): Unit = {
    df.write.mode(SaveMode.Append).parquet(segmentGroupFolder)
  }

  override protected def readSegmentGroupsFolders(sparkSession: SparkSession, filters: Array[Filter],
                                                  segmentGroupFolders: mutable.ArrayBuffer[String]): DataFrame = {
    val segmentGroupFoldersIterator = segmentGroupFolders.iterator
    var df = sparkSession.read.parquet(segmentGroupFoldersIterator.next())
    while (segmentGroupFoldersIterator.hasNext) {
      df = df.union(sparkSession.read.parquet(segmentGroupFoldersIterator.next()))
    }
    df = df.withColumn("start_time", (col("start_time") / 1000).cast("timestamp"))
    df = df.withColumn("end_time", (col("end_time") / 1000).cast("timestamp"))
    Spark.applyFiltersToDataFrame(df, filters)
  }

  /** Private Methods **/
  private def getReader(parquetFilePath: Path) = {
    ParquetFileReader.open(HadoopInputFile.fromPath(parquetFilePath, new Configuration()))
  }

  private def getWriter(parquetFilePath: Path, schema: MessageType): ParquetWriter[Group] = {
    new ParquetWriterBuilder(parquetFilePath)
      .withType(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()
  }

  /** Instance Variables **/
  private val segmentGroupSchema = new MessageType("segment",
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "gid" ),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "start_time"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "end_time"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "mtid" ),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "model"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "gaps"))
}

private class ParquetWriterBuilder(parquetFilePath: Path) extends ParquetWriter.Builder[Group, ParquetWriterBuilder](parquetFilePath) {
  /** Instance Variables * */
  private var messageType: MessageType = _

  /** Public Methods **/
  def withType(messageType: MessageType): ParquetWriterBuilder = {
    this.messageType = messageType
    this
  }
  override protected def self: ParquetWriterBuilder = this
  override protected def getWriteSupport(conf: Configuration): WriteSupport[Group] = {
    GroupWriteSupport.setSchema(this.messageType, conf)
    new GroupWriteSupport()
  }
}
