package org.geomesa.nifi.datastore.processor.records

import java.io.OutputStream
import java.util

import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.serialization.record.{Record, RecordSchema, RecordSet}
import org.apache.nifi.serialization.{RecordSetWriter, RecordSetWriterFactory, WriteResult}
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags

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
    new GeoAvroRecordSetWriter(componentLog, recordSchema, outputStream, map)
  }
}

class GeoAvroRecordSetWriter(componentLog: ComponentLog, recordSchema: RecordSchema, outputStream: OutputStream, map: util.Map[String, String]) extends RecordSetWriter {
  override def write(recordSet: RecordSet): WriteResult = ???

  override def beginRecordSet(): Unit = ???

  override def finishRecordSet(): WriteResult = ???

  override def write(record: Record): WriteResult = ???

  override def getMimeType: String = ???

  override def flush(): Unit = ???

  override def close(): Unit = ???
}
