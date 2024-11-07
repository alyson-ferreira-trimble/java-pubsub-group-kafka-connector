package com.google.pubsub.kafka.sink;
// https://github.com/a0x8o/kafka/blob/e2035664d45a52f1ebefd122522787d8e8075ca0/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java
/*
 * Steps to patch the code above only for serialization without config:
 * 
 * - Implement the ObjectMapper usage from JsonSerializer (instantiate in constructor, use in fromConnectData).
 * - Remove all occurrences of JsonConverterConfig, JsonSerializer, and JsonDeserializer.
 * - For parts of the code using JsonConverterConfig, hard-code options.
 * - Address deprecation of JsonNodeFactory.withExactBigDecimals(true) by using JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES.
 * - Remove implements <SomeClass> from JsonConverter
 * - Keep only parts relevant for fromConnectData method: LOGICAL_CONVERTERS, JSON_NODE_FACTORY, convertToJson, and LogicalTypeConverter.
 * 
 * Notable removed features:
 * 
 * - Json Schema Envelope support.
 * - BASE64 Decimal format. Only numeric available.
 */

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Implementation of Converter that uses JSON to store schemas and objects. By default this converter will serialize Connect keys, values,
 * and headers with schemas, although this can be disabled with {@link JsonConverterConfig#SCHEMAS_ENABLE_CONFIG schemas.enable}
 * configuration option.
 *
 * This implementation currently does nothing with the topic names or header names.
 */
public class JsonConverter {
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Convert values in Kafka Connect form into/from their logical types. These logical converters are discovered by logical type
    // names specified in the field
    private static final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

    static {
        LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof BigDecimal))
                    throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());

                final BigDecimal decimal = (BigDecimal) value;
                return JSON_NODE_FACTORY.numberNode(decimal);
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (value.isNumber()) return value.decimalValue();
                if (value.isBinary() || value.isTextual()) {
                    try {
                        return Decimal.toLogical(schema, value.binaryValue());
                    } catch (Exception e) {
                        throw new DataException("Invalid bytes for Decimal field", e);
                    }
                }

                throw new DataException("Invalid type for Decimal, underlying representation should be numeric or bytes but was " + value.getNodeType());
            }
        });

        LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                return JSON_NODE_FACTORY.numberNode(Date.fromLogical(schema, (java.util.Date) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!(value.isInt()))
                    throw new DataException("Invalid type for Date, underlying representation should be integer but was " + value.getNodeType());
                return Date.toLogical(schema, value.intValue());
            }
        });

        LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, (java.util.Date) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!(value.isInt()))
                    throw new DataException("Invalid type for Time, underlying representation should be integer but was " + value.getNodeType());
                return Time.toLogical(schema, value.intValue());
            }
        });

        LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                return JSON_NODE_FACTORY.numberNode(Timestamp.fromLogical(schema, (java.util.Date) value));
            }

            @Override
            public Object toConnect(final Schema schema, final JsonNode value) {
                if (!(value.isIntegralNumber()))
                    throw new DataException("Invalid type for Timestamp, underlying representation should be integral but was " + value.getNodeType());
                return Timestamp.toLogical(schema, value.longValue());
            }
        });
    }

    public JsonConverter() {
        objectMapper.setNodeFactory(JSON_NODE_FACTORY);
        objectMapper.configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, true);
    }

    public byte[] fromConnectData(Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }

        JsonNode jsonValue = convertToJson(schema, value);

        try {
            return objectMapper.writeValueAsBytes(jsonValue);
        } catch (Exception e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning both the schema
     * and the converted object.
     */
    private JsonNode convertToJson(Schema schema, Object value) {
        if (value == null) {
            if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            if (schema.defaultValue() != null)
                return convertToJson(schema, schema.defaultValue());
            if (schema.isOptional())
                return JSON_NODE_FACTORY.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                return logicalConverter.toJson(schema, value);
        }

        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null)
                    throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8:
                    return JSON_NODE_FACTORY.numberNode((Byte) value);
                case INT16:
                    return JSON_NODE_FACTORY.numberNode((Short) value);
                case INT32:
                    return JSON_NODE_FACTORY.numberNode((Integer) value);
                case INT64:
                    return JSON_NODE_FACTORY.numberNode((Long) value);
                case FLOAT32:
                    return JSON_NODE_FACTORY.numberNode((Float) value);
                case FLOAT64:
                    return JSON_NODE_FACTORY.numberNode((Double) value);
                case BOOLEAN:
                    return JSON_NODE_FACTORY.booleanNode((Boolean) value);
                case STRING:
                    CharSequence charSeq = (CharSequence) value;
                    return JSON_NODE_FACTORY.textNode(charSeq.toString());
                case BYTES:
                    if (value instanceof byte[])
                        return JSON_NODE_FACTORY.binaryNode((byte[]) value);
                    else if (value instanceof ByteBuffer)
                        return JSON_NODE_FACTORY.binaryNode(((ByteBuffer) value).array());
                    else
                        throw new DataException("Invalid type for bytes type: " + value.getClass());
                case ARRAY: {
                    Collection<?> collection = (Collection<?>) value;
                    ArrayNode list = JSON_NODE_FACTORY.arrayNode();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode fieldValue = convertToJson(valueSchema, elem);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;
                    // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                    boolean objectMode;
                    if (schema == null) {
                        objectMode = true;
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            if (!(entry.getKey() instanceof String)) {
                                objectMode = false;
                                break;
                            }
                        }
                    } else {
                        objectMode = schema.keySchema().type() == Schema.Type.STRING;
                    }
                    ObjectNode obj = null;
                    ArrayNode list = null;
                    if (objectMode)
                        obj = JSON_NODE_FACTORY.objectNode();
                    else
                        list = JSON_NODE_FACTORY.arrayNode();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode mapKey = convertToJson(keySchema, entry.getKey());
                        JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

                        if (objectMode)
                            obj.set(mapKey.asText(), mapValue);
                        else
                            list.add(JSON_NODE_FACTORY.arrayNode().add(mapKey).add(mapValue));
                    }
                    return objectMode ? obj : list;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");
                    ObjectNode obj = JSON_NODE_FACTORY.objectNode();
                    for (Field field : schema.fields()) {
                        obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
                    }
                    return obj;
                }
            }

            throw new DataException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }

    private interface LogicalTypeConverter {
        JsonNode toJson(Schema schema, Object value);
        Object toConnect(Schema schema, JsonNode value);
    }
}
