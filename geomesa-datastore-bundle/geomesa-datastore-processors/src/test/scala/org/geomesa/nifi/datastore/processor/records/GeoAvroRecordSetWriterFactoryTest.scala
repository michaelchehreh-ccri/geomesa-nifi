/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.records

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util
import java.util.{HashMap, Map}

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.csv.{CSVReader, CSVRecordSetWriter, CSVUtils}
import org.apache.nifi.processors.standard.{ConvertRecord, ValidateRecord}
import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.geomesa.nifi.datastore.processor.records.GeoAvroRecordSetWriterFactory
import org.geomesa.nifi.datastore.processor.records.GeoAvroRecordSetWriterFactory.WKT_COLUMNS
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoAvroRecordSetWriterFactoryTest extends Specification with LazyLogging {
  sequential

  "GeoAvroRecordSetWriterFactory" should {
    "Write out GeoAvro from a simple Record producing Processor" in {
      val runner: TestRunner = buildRunner("position")

      val content: String =
        "id|username|role|position\n" +
          "123|Legend|actor|POINT(-118.3287 34.0928)\n" +
          "456|Lewis|leader|POINT(-86.9023 4.567)\n" +
          "789|Basie|pianist|POINT(-73.9465 40.8116)\n"

      enqueueAndRun(runner, content)

      val featuresRead: Seq[SimpleFeature] = getFeatures(runner)

      featuresRead.foreach { println(_) }
      featuresRead.size mustEqual(3)

      ok
    }
  }

  private def enqueueAndRun(runner: TestRunner, content: String) = {
    runner.enqueue(content)
    runner.run()

    runner.assertAllFlowFilesTransferred("success", 1)
  }

  private def getFeatures(runner: TestRunner) = {
    val result = runner.getContentAsByteArray(runner.getFlowFilesForRelationship("success").get(0))

    val bais = new ByteArrayInputStream(result)
    val avroReader = new AvroDataFileReader(bais)
    val featuresRead: Seq[SimpleFeature] = avroReader.toList
    featuresRead
  }

  private def buildRunner(geomtryColumns: String) = {
    val runner: TestRunner = TestRunners.newTestRunner(classOf[ConvertRecord])

    val csvReader: CSVReader = new CSVReader
    runner.addControllerService("csv-reader", csvReader)
    runner.setProperty(csvReader, CSVUtils.VALUE_SEPARATOR, "|")
    runner.enableControllerService(csvReader)

    val geoAvroWriter = new GeoAvroRecordSetWriterFactory()
    runner.addControllerService("geo-avro-record-set-writer", geoAvroWriter)
    runner.setProperty(geoAvroWriter, WKT_COLUMNS, geomtryColumns)
    runner.enableControllerService(geoAvroWriter)

    runner.setProperty("record-reader", "csv-reader")
    runner.setProperty("record-writer", "geo-avro-record-set-writer")
    runner
  }
}
