package org.geomesa.nifi.datastore.processor.records

import java.io.OutputStream
import java.util

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.schema.access.SchemaNameAsAttribute
import org.apache.nifi.serialization.record.{Record, RecordSchema}
import org.apache.nifi.serialization.{AbstractRecordSetWriter, RecordSetWriterFactory}
import org.geomesa.nifi.datastore.processor.records.GeoAvroRecordSetWriterFactory.WKT_COLUMNS
import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

@Tags(Array("avro", "geoavro", "result", "set", "recordset", "record", "writer", "serializer", "row"))
@CapabilityDescription("Writes the contents of a RecordSet as GeoAvro which AvroToPutGeoMesa* Processors can use.")
class GeoAvroRecordSetWriterFactory extends AbstractControllerService with RecordSetWriterFactory {
  // NB: This is the same as what the InheritSchemaFromRecord Strategy does
  override def getSchema(map: util.Map[String, String], recordSchema: RecordSchema): RecordSchema = {
    println(s"Getting the schema from $map and $recordSchema")
    recordSchema
  }

  override def createWriter(componentLog: ComponentLog, recordSchema: RecordSchema, outputStream: OutputStream, map: util.Map[String, String]): GeoAvroRecordSetWriter = {
    println(s"Creating writer for schema $recordSchema with map $map")
    componentLog.info("Starting to write Avro output!")
    new GeoAvroRecordSetWriter(componentLog, recordSchema, outputStream, getConfigurationContext.getProperties)
  }

  // TODO: Refactor
  // TODO: Add WKB_COLUMNS
  override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
//    val list = super.getSupportedPropertyDescriptors
//    list.add(WKT_COLUMNS)
    import scala.collection.JavaConverters._
    Seq(WKT_COLUMNS).asJava
  }
}

object GeoAvroRecordSetWriterFactory {
  val WKT_COLUMNS = new PropertyDescriptor.Builder()
    .name("WKT Columns")
    .description("Comma-separated list of columns with geometries in WKT format")
    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false).build
}

class GeoAvroRecordSetWriter(componentLog: ComponentLog, recordSchema: RecordSchema, outputStream: OutputStream, map: util.Map[PropertyDescriptor, String]) extends AbstractRecordSetWriter(outputStream) {
  private val schemaAccessWriter = new SchemaNameAsAttribute()
  val converter: SimpleFeatureRecordConverter = SimpleFeatureRecordConverter.fromRecordSchema(recordSchema, map, GeometryEncoding.Wkb)

  private val sft: SimpleFeatureType = converter.sft  // use recordSchema
  val writer = new AvroDataFileWriter(outputStream, sft)

  override def writeRecord(record: Record): util.Map[String, String] = {
    //val sf: SimpleFeature = converter.convert(record)  // use Record
    val sf = record match {
      case sfmr: SimpleFeatureMapRecord => sfmr.sf
      case other: Record => throw new Exception(s"Cannot converter records of type ${record.getClass} to a SimpleFeature")
    }
    writer.append(sf)
    schemaAccessWriter.getAttributes(recordSchema)
  }

  override def getMimeType: String = "application/avro-binary"

  override def close(): Unit = {
    writer.close()
    // Calling super.close will close the stream.  Check if this necessary.
    // super.close()
  }

  // Creating the AvroDataFileWriter may handle this completely.
  //override def onBeginRecordSet(): Unit = super.onBeginRecordSet()
}
