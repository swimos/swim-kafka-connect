// Copyright 2015-present SWIM Inc.
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

package swim.kafka.connector.convertor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import swim.json.Json;
import swim.structure.Record;
import swim.structure.Value;
import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.apache.kafka.connect.data.Schema.Type.MAP;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;

public class MessageConvertor {

  public MessageConvertor() {

  }

  public Value toValue(Object object, Schema schema) {
    if (schema != null && object instanceof Struct) {
      return schemaToValue((Struct) object, schema, Record.create());
    } else {
      return schemalessToValue(object, Record.create());
    }
  }

  private Value schemalessToValue(Object object, Record record) {
    if (object instanceof Collection) {
      final Collection<?> collection = (Collection<?>) object;
      final Record collectionRecord = Record.create();
      for (Object element : collection) {
        collectionRecord.appended(schemalessToValue(element, Record.create()));
      }
      return record.isEmpty() ? collectionRecord : record.appended(collectionRecord);
    } else if (object instanceof Map) {
      final Map<?, ?> map = (Map<?, ?>) object;
      final Set<?> keys = map.keySet();
      final Record mapRecord = Record.create();
      for (Object key : keys) {
        // only take keys of String type, ignore others
        if (key instanceof String) {
          final String keyStr = (String) key;
          final Value valueRecord = schemalessToValue(map.get(key), Record.create());
          mapRecord.updatedSlot(keyStr, valueRecord);
        }
      }
      return record.isEmpty() ? mapRecord : record.appended(mapRecord);
    } else if (object instanceof String) {
      // use JSON parser for String
      return Json.parse((String) object);
    } else {
      // this works for primitive types and byte arrays
      return record.isEmpty() ? Record.fromObject(object) : record.appended(Record.fromObject(object));
    }
  }

  private Value schemaToValue(Struct struct, Schema schema, Record record) {
    final List<Field> fields = schema.fields();
    for (Field field : fields) {
      record.updatedSlot(field.name(), fieldToValue(field.schema(), struct.get(field)));
    }
    return record;
  }

  private Value fieldToValue(Schema fieldSchema, Object fieldValue) {
    final Schema.Type fieldSchemaType = fieldSchema.type();
    if (fieldSchemaType.isPrimitive()) {
      return Record.fromObject(fieldValue);
    } else if (fieldSchemaType == STRUCT) {
      return schemaToValue((Struct) fieldValue, fieldSchema, Record.create());
    } else if (fieldSchemaType == ARRAY) {
      return arrayToValue(fieldSchema, (List<?>) fieldValue);
    } else if (fieldSchemaType == MAP) {
      return mapToValue(fieldSchema, (Map<?, ?>) fieldValue);
    } else {
      return Value.absent();
    }
  }

  private Record mapToValue(Schema fieldSchema, Map<?, ?> map) {
    final Schema mapKeySchema = fieldSchema.keySchema();
    final Schema mapValueSchema = fieldSchema.valueSchema();
    final Set<?> keys = map.keySet();
    Record mapRecord = Record.create();
    for (Object key : keys) {
      final Value mapKey = fieldToValue(mapKeySchema, key);
      final Value mapValue = fieldToValue(mapValueSchema, map.get(key));
      mapRecord.updatedSlot(mapKey, mapValue);
    }
    return mapRecord;
  }

  private Value arrayToValue(Schema fieldSchema, List<?> fieldValue) {
    final Schema arrayItemSchema = fieldSchema.valueSchema();
    final Schema.Type arrayItemSchemaType = arrayItemSchema.type();
    Record arrayRecord = Record.create();
    for (Object listItem : fieldValue) {
      if (arrayItemSchemaType.isPrimitive()) {
        arrayRecord.appended(Record.fromObject(listItem));
      } else if (arrayItemSchemaType == STRUCT) {
        arrayRecord.appended(schemaToValue((Struct) listItem, arrayItemSchema, Record.create()));
      } else if (arrayItemSchemaType == ARRAY) {
        arrayRecord.appended(arrayToValue(arrayItemSchema, (List<?>) listItem));
      } else if (arrayItemSchemaType == MAP) {
        arrayRecord.appended(mapToValue(arrayItemSchema, (Map<?, ?>) listItem));
      }
    }
    return arrayRecord;
  }
}
