// Copyright 2015-present SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package swim.kafka.connector.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import swim.json.Json;
import swim.structure.Data;
import swim.structure.Form;
import swim.structure.Record;
import swim.structure.Value;
import static org.testng.Assert.assertEquals;

public class SinkMessageConvertorSpec {

  private final SinkMessageConvertor messageConvertor = new SinkMessageConvertor();

  // Data for Tests
  private SchemaBuilder baseSchema;
  private Struct baseStruct;
  private Value baseValue;

  @BeforeTest
  public void initTestData() {
    baseSchema = makeBaseSchema();
    baseStruct = makeBaseStruct(baseSchema);
    baseValue = makeBaseValue();
  }

  @Test
  public void convertSchemalessPrimitiveKey() {
    assertValueEquals(messageConvertor.convertKey(makeSchemalessSinkRecord("a", null)), Record.fromObject("a"));
    assertValueEquals(messageConvertor.convertKey(makeSchemalessSinkRecord(1, null)), Record.fromObject(1));
    assertValueEquals(messageConvertor.convertKey(makeSchemalessSinkRecord(2.0f, null)), Record.fromObject(2.0));
    assertValueEquals(messageConvertor.convertKey(makeSchemalessSinkRecord(true, null)), Record.fromObject(true));
    assertValueEquals(messageConvertor.convertKey(makeSchemalessSinkRecord("abc".getBytes(), null)), Record.fromObject("abc".getBytes()));
  }

  @Test
  public void convertSchemalessPrimitiveValue() {
    assertValueEquals(messageConvertor.convertValue(makeSchemalessSinkRecord(null, "a")), Record.fromObject("a"));
    assertValueEquals(messageConvertor.convertValue(makeSchemalessSinkRecord(null, 1)), Record.fromObject(1));
    assertValueEquals(messageConvertor.convertValue(makeSchemalessSinkRecord(null, 2.0f)), Record.fromObject(2.0));
    assertValueEquals(messageConvertor.convertValue(makeSchemalessSinkRecord(null, true)), Record.fromObject(true));
    assertValueEquals(messageConvertor.convertValue(makeSchemalessSinkRecord(null, "abc".getBytes())), Record.fromObject("abc".getBytes()));
  }

  @Test
  public void convertSchemalessCollection() {
    final List<Object> list = makeList("a", 1, 2.0f, true, "abc".getBytes());
    final SinkRecord sinkRecord = makeSchemalessSinkRecord(null, list);
    final Value value = makeValue("a", 1, 2.0f, true, "abc".getBytes());
    assertValueEquals(messageConvertor.convertValue(sinkRecord), value);
  }

  @Test
  public void convertSchemalessMap() {
    final Map<String, Object> map = makeMap("a", 1, 2.0f, true, "abc".getBytes());
    final SinkRecord sinkRecord = makeSchemalessSinkRecord(null, map);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());
    assertValueEquals(messageConvertor.convertValue(sinkRecord), value);
  }

  @Test
  public void convertSchemalessNestedMap() {
    final Map<String, Object> map = makeMap("a", 1, 2.0f, true, "abc".getBytes());
    final Map<String, Object> map1 = makeMap("a", 1, 2.0f, true, "abc".getBytes(), map);
    final SinkRecord sinkRecord = makeSchemalessSinkRecord(null, map1);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());
    final Value value1 = makeMapValue("a", 1, 2.0f, true, "abc".getBytes(), value);
    assertValueEquals(messageConvertor.convertValue(sinkRecord), value1);
  }

  @Test
  public void convertSchemalessJson() {
    final String json = "{\"key1\": \"a\", \"key2\": 1, \"key3\": 2.0, \"key4\": true}";
    final SinkRecord sinkRecord = makeSchemalessSinkRecord(null, json);
    final Value value = Json.parse(json);
    assertValueEquals(messageConvertor.convertValue(sinkRecord), value);
  }

  @Test
  public void convertSchemaFlatStruct() {
    final SinkRecord sinkRecord = makeSchemaSinkRecord(null, null, baseStruct, baseSchema);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());
    assertValueEquals(messageConvertor.convertValue(sinkRecord), value);
  }

  @Test
  public void convertSchemaNestedStruct() {
    final Schema nestedSchema = makeBaseSchema().field("5", baseSchema);
    final Struct struct = makeBaseStruct(nestedSchema)
          .put("5", baseStruct);
    final SinkRecord sinkRecord = makeSchemaSinkRecord(null, null, struct, nestedSchema);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());
    final Value value1 = makeMapValue("a", 1, 2.0f, true, "abc".getBytes(), value);
    assertValueEquals(messageConvertor.convertValue(sinkRecord), value1);
  }

  @Test
  public void convertSchemaStructWithMap() {
    final Schema mapSchema = SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.STRING_SCHEMA);
    final Schema nestedSchema = makeBaseSchema().field("5", mapSchema);
    final HashMap<String, String> map = new HashMap<>();
    map.put("0", "x");
    map.put("1", "y");
    map.put("2", "z");
    final Struct struct = makeBaseStruct(nestedSchema)
          .put("5", map);
    final SinkRecord sinkRecord = makeSchemaSinkRecord(null, null, struct, nestedSchema);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());
    final Value nestedValue = Record.fromObject(value).updatedSlot("5", makeMapValue("x", "y", "z"));
    assertValueEquals(messageConvertor.convertValue(sinkRecord), nestedValue);
  }

  @Test
  public void convertSchemaStructWithArray() {
    final Schema arraySchema = SchemaBuilder.array(SchemaBuilder.STRING_SCHEMA);
    final Schema nestedSchema = makeBaseSchema().field("5", arraySchema);
    final List<Object> list = makeList("0", "1", "2");
    final Struct struct = makeBaseStruct(nestedSchema)
          .put("5", list);
    final SinkRecord sinkRecord = makeSchemaSinkRecord(null, null, struct, nestedSchema);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());
    final Value nestedValue = Record.fromObject(value).updatedSlot("5", Form.forCollection(List.class, Form.forString()).mold(list).toValue());
    assertValueEquals(messageConvertor.convertValue(sinkRecord), nestedValue);
  }

  @Test
  public void convertSchemaStructWithMapAndArray() {
    final Schema mapSchema = SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.STRING_SCHEMA);
    final HashMap<String, String> map = new HashMap<>();
    map.put("0", "x");
    map.put("1", "y");
    map.put("2", "z");

    final Schema arraySchema = SchemaBuilder.array(SchemaBuilder.STRING_SCHEMA);
    final List<Object> list = makeList("0", "1", "2");

    final Schema nestedSchema = makeBaseSchema().field("5", mapSchema).field("6", arraySchema);
    final Struct struct = makeBaseStruct(nestedSchema)
          .put("5", map)
          .put("6", list);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());
    final Value nestedValue = Record.fromObject(value)
          .updatedSlot("5", makeMapValue("x", "y", "z"))
          .updatedSlot("6", Form.forCollection(List.class, Form.forString())
           .mold(list).toValue());

    final SinkRecord sinkRecord = makeSchemaSinkRecord(null, null, struct, nestedSchema);
    assertValueEquals(messageConvertor.convertValue(sinkRecord), nestedValue);
  }

  @Test
  public void convertSchemaStructWithMapStruct() {
    final Schema mapSchema = SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, baseSchema);
    final HashMap<String, Struct> map = new HashMap<>();
    map.put("0", baseStruct);
    map.put("1", baseStruct);
    map.put("2", baseStruct);

    final Schema nestedSchema = makeBaseSchema().field("5", mapSchema);
    final Struct struct = makeBaseStruct(nestedSchema)
          .put("5", map);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());
    final Value nestedValue = Record.fromObject(value)
          .updatedSlot("5", makeMapValue(baseValue, baseValue, baseValue));

    final SinkRecord sinkRecord = makeSchemaSinkRecord(null, null, struct, nestedSchema);
    assertValueEquals(messageConvertor.convertValue(sinkRecord), nestedValue);
  }

  @Test
  public void convertSchemaStructWithArrayStruct() {
    final Schema arraySchema = SchemaBuilder.array(baseSchema);
    final Schema nestedSchema = makeBaseSchema().field("5", arraySchema);
    final List<Object> list = makeList(baseStruct, baseStruct, baseStruct);
    final Struct struct = makeBaseStruct(nestedSchema)
          .put("5", list);
    final SinkRecord sinkRecord = makeSchemaSinkRecord(null, null, struct, nestedSchema);
    final Value value = makeMapValue("a", 1, 2.0f, true, "abc".getBytes());

    final List<Value> expectedList =  new ArrayList<>(Arrays.asList(baseValue, baseValue, baseValue));
    final Value nestedValue = Record.fromObject(value).updatedSlot("5", Form.forCollection(List.class, Form.forValue()).mold(expectedList).toValue());
    assertValueEquals(messageConvertor.convertValue(sinkRecord), nestedValue);
  }

  private SchemaBuilder makeBaseSchema() {
    return SchemaBuilder.struct()
          .field("0", Schema.STRING_SCHEMA)
          .field("1", Schema.INT32_SCHEMA)
          .field("2", Schema.FLOAT32_SCHEMA)
          .field("3", Schema.BOOLEAN_SCHEMA)
          .field("4", Schema.BYTES_SCHEMA);
  }

  private Struct makeBaseStruct(Schema baseSchema) {
    return new Struct(baseSchema)
          .put("0", "a")
          .put("1", 1)
          .put("2", 2.0f)
          .put("3", true)
          .put("4", "abc".getBytes());
  }

  private Value makeBaseValue() {
    return Record.create(5)
          .slot("0", "a")
          .slot("1", 1)
          .slot("2", 2.0f)
          .slot("3", true)
          .slot("4", Data.wrap("abc".getBytes()));
  }

  private Value makeMapValue(Object... objects) {
    final Value value = Record.create();
    int index = 0;
    for (Object object: objects) {
      value.updatedSlot(Integer.toString(index), Record.fromObject(object));
      index += 1;
    }
    return value;
  }

  private Map<String, Object> makeMap(Object... objects) {
    Map<String, Object> map = new HashMap<>();
    int index = 0;
    for (Object object: objects) {
      map.put(Integer.toString(index), object);
      index += 1;
    }
    return map;
  }


  private Value makeValue(Object... objects) {
    final Value value = Record.create();
    for (Object object: objects) {
      value.appended(object);
    }
    return value;
  }

  private List<Object> makeList(Object... objects) {
    return new ArrayList<>(Arrays.asList(objects));
  }

  private SinkRecord makeSchemalessSinkRecord(Object key, Object value) {
    return new SinkRecord("test-topic", 1, null, key, null, value, 0L);
  }

  private SinkRecord makeSchemaSinkRecord(Object key, Schema keySchema, Object value, Schema valueSchema) {
    return new SinkRecord("test-topic", 1, keySchema, key, valueSchema, value, 0L);
  }

  private void assertValueEquals(Value actual , Value expected) {
    assertEquals(actual, expected, "Expected: " + expected + ". Got Actual: " + actual);
  }

}
