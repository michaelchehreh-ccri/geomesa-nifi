/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.records

import java.nio.charset.StandardCharsets
import java.util
import java.util.{HashMap, Map}

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.csv.{CSVReader, CSVRecordSetWriter, CSVUtils}
import org.apache.nifi.processors.standard.{ConvertRecord, ValidateRecord}
import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoAvroRecordSetWriterFactoryTest extends Specification with LazyLogging {
  sequential

  "GeoAvroRecordSetWriterFactory" should {
    "Write out GeoAvro from a simple Record producing Processor" in {
      val runner: TestRunner = TestRunners.newTestRunner(classOf[ConvertRecord])

      val csvReader: CSVReader = new CSVReader
      runner.addControllerService("csv-reader", csvReader)
      runner.enableControllerService(csvReader)

      val csvWriter: CSVRecordSetWriter = new CSVRecordSetWriter
      runner.addControllerService("csv-writer", csvWriter)
      runner.enableControllerService(csvWriter)

      runner.setProperty("record-reader", "csv-reader")
      runner.setProperty("record-writer", "csv-writer")

      val ffContent: String =
        "id,username,role,position\n" +
          "123,Legend,actor,POINT(-118.3287 34.0928)\n" +
          "456,Lewis,leader,POINT(-86.9023 4.567)\n" +
          "789,Basie,pianist,POINT(-73.9465 40.8116)\n"

      runner.enqueue(ffContent)
      runner.run()

      runner.assertAllFlowFilesTransferred("success", 1)

      val flowFile: MockFlowFile = runner.getFlowFilesForRelationship("success").get(0)
      //val expected = "id|username|password\n123|'John'|password\n"
      val expected = "id,username,role,position\n" +
        "123,Legend,actor,POINT(-118.3287 34.0928)\n" +
        "456,Lewis,leader,POINT(-86.9023 4.567)\n" +
        "789,Basie,pianist,POINT(-73.9465 40.8116)\n"
      assertEquals(expected, new String(flowFile.toByteArray))
      ok
    }
  }
}
