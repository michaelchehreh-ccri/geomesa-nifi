package org.locationtech.geomesa.features.avro.backport

import java.io.{InputStream, OutputStream}

import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.opengis.feature.simple.SimpleFeature

trait LimitedSerialization extends SimpleFeatureSerializer {
  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    throw new NotImplementedError
  override def deserialize(in: InputStream): SimpleFeature =
    throw new NotImplementedError
  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    throw new NotImplementedError
}
