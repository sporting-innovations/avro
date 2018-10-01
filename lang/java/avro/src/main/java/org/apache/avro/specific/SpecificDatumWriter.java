/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.specific;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

/** {@link org.apache.avro.io.DatumWriter DatumWriter} for generated Java classes. */
public class SpecificDatumWriter<T> extends GenericDatumWriter<T> {
  public SpecificDatumWriter() {
    super(SpecificData.get());
  }

  public SpecificDatumWriter(Class<T> c) {
    super(SpecificData.get().getSchema(c), SpecificData.get());
  }

  public SpecificDatumWriter(Schema schema) {
    super(schema, SpecificData.get());
  }

  public SpecificDatumWriter(Schema root, SpecificData specificData) {
    super(root, specificData);
  }

  protected SpecificDatumWriter(SpecificData specificData) {
    super(specificData);
  }

  /** Returns the {@link SpecificData} implementation used by this writer. */
  public SpecificData getSpecificData() {
    return (SpecificData) getData();
  }

  private final ThreadLocal<LinkedList<Map<String, Conversion<?>>>> conversions
      = new ThreadLocal<LinkedList<Map<String, Conversion<?>>>>() {
    @Override
    protected LinkedList<Map<String, Conversion<?>>> initialValue() {
      return new LinkedList<Map<String, Conversion<?>>>();
    }
  };

  protected Conversion<?> getConversionByClass(Class<?> type,
                                               LogicalType logicalType) {
    if (logicalType == null) {
      return null;
    }
    // Check for logical types conversions used by the specific compiler. No
    // need to use the class because the compiler always produces code with the
    // same class if it used a conversion.
    Conversion<?> conversion = null;
    Map<String, Conversion<?>> conversionMap = conversions.get().peekLast();
    if (conversionMap != null) {
      conversion = conversionMap.get(logicalType.getName());
    }
    if (conversion == null) {
      conversion = super.getConversionByClass(type, logicalType);
    }
    return conversion;
  }

  @Override
  protected void writeEnum(Schema schema, Object datum, Encoder out)
    throws IOException {
    if (!(datum instanceof Enum))
      super.writeEnum(schema, datum, out);        // punt to generic
    else
      out.writeEnum(((Enum)datum).ordinal());
  }

  @Override
  protected void writeString(Schema schema, Object datum, Encoder out)
    throws IOException {
    if (!(datum instanceof CharSequence)
        && getSpecificData().isStringable(datum.getClass())) {
      datum = datum.toString();                   // convert to string
    }
    writeString(datum, out);
  }

  @Override
  protected void writeField(Object datum, Schema.Field f, Encoder out,
                            Object state) throws IOException {
    if (datum instanceof SpecificRecordBase) {
      Conversion<?> conversion = ((SpecificRecordBase) datum).getConversion(f.pos());
      Schema fieldSchema = f.schema();
      LogicalType logicalType = fieldSchema.getLogicalType();

      Object value = getData().getField(datum, f.name(), f.pos());
      if (conversion != null && logicalType != null) {
        value = convert(fieldSchema, logicalType, conversion, value);
      }

      writeWithoutConversion(fieldSchema, value, out);

    } else {
      super.writeField(datum, f, out, state);
    }
  }

  @Override
  protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
    // Update the current set of conversions for the record class
    Map<String, Conversion<?>> conversionMap =
            getSpecificData().getConversionMap(schema);
    conversions.get().addLast(conversionMap);
    try {
      super.writeRecord(schema, datum, out);
    } finally {
      conversions.get().removeLast();
    }
  }

  @Override
  protected void writeUnion(Schema schema, Object datum, Encoder out) throws IOException {
    Map<String, Conversion<?>> conversionMap =
            getSpecificData().getConversionMap(schema);
    conversions.get().addLast(conversionMap);
    try {
      super.writeUnion(schema, datum, out);
    } finally {
      conversions.get().removeLast();
    }
  }
}

