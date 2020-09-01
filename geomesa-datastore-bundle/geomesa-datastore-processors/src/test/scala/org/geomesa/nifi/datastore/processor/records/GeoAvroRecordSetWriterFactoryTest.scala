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
      runner.setProperty(csvReader, CSVUtils.VALUE_SEPARATOR, "${csv.in.delimiter}")
      runner.setProperty(csvReader, CSVUtils.QUOTE_CHAR, "${csv.in.quote}")
      runner.setProperty(csvReader, CSVUtils.ESCAPE_CHAR, "${csv.in.escape}")
      runner.setProperty(csvReader, CSVUtils.COMMENT_MARKER, "${csv.in.comment}")
      runner.enableControllerService(csvReader)

      val csvWriter: CSVRecordSetWriter = new CSVRecordSetWriter
      runner.addControllerService("csv-writer", csvWriter)
      runner.setProperty(csvWriter, CSVUtils.VALUE_SEPARATOR, "${csv.out.delimiter}")
      runner.setProperty(csvWriter, CSVUtils.QUOTE_CHAR, "${csv.out.quote}")
      runner.setProperty(csvWriter, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_ALL)
      runner.enableControllerService(csvWriter)

      runner.setProperty("record-reader", "csv-reader")
      runner.setProperty("record-writer", "csv-writer")

      val ffContent: String = "~ comment\n" + "id|username|password\n" + "123|'John'|^|^'^^\n"

      val ffAttributes: util.Map[String, String] = new util.HashMap[String, String]
      ffAttributes.put("csv.in.delimiter", "|")
      ffAttributes.put("csv.in.quote", "'")
      ffAttributes.put("csv.in.escape", "^")
      ffAttributes.put("csv.in.comment", "~")
      ffAttributes.put("csv.out.delimiter", "\t")
      ffAttributes.put("csv.out.quote", "`")

      runner.enqueue(ffContent, ffAttributes)
      runner.run()

      runner.assertAllFlowFilesTransferred("success", 1)

      val flowFile: MockFlowFile = runner.getFlowFilesForRelationship("success").get(0)

      val expected: String = "`id`\t`username`\t`password`\n" + "`123`\t`John`\t`|'^`\n"
      assertEquals(expected, new String(flowFile.toByteArray))
      ok
    }
  }
}
