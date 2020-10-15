package org.hypertrace.traceenricher.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Test;

public class EntityAvroConverterTest {
  @Test
  public void testConvertToAvroEntity() {
    org.hypertrace.entity.data.service.v1.Entity entity = org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setEntityId("entity-id")
        .setEntityName("entity-name")
        .setEntityType("entity-type")
        .setTenantId("entity-tenant-id")
        .putAllAttributes(Map.of(
            "attr1", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("v1")).build(),
            "attr2", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setBoolean(true)).build(),
            "attr3", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setInt(23)).build(),
            "attr4", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValueList(
                AttributeValueList.newBuilder()
                    .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l1")))
                    .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l2")))
                    .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l3")))
            ).build(),
            "attr5", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setBytes(ByteString.copyFrom("test-bytes".getBytes()))).build()
        ))
        .build();

    Entity avroEntity1 = EntityAvroConverter.convertToAvroEntity(entity, false);
    assertEquals(
        Entity.newBuilder()
            .setEntityId("entity-id")
            .setEntityName("entity-name")
            .setEntityType("entity-type")
            .setCustomerId("entity-tenant-id")
            .build(),
        avroEntity1);

    Entity avroEntity2 = EntityAvroConverter.convertToAvroEntity(entity, true);
    assertEquals(
        Entity.newBuilder()
            .setEntityId("entity-id")
            .setEntityName("entity-name")
            .setEntityType("entity-type")
            .setCustomerId("entity-tenant-id")
            .setAttributesBuilder(
                Attributes.newBuilder()
                    .setAttributeMap(Map.of(
                        "attr1", AttributeValue.newBuilder().setValue("v1").build(),
                        "attr2", AttributeValue.newBuilder().setValue("true").build(),
                        "attr3", AttributeValue.newBuilder().setValue("23").build(),
                        "attr4", AttributeValue.newBuilder().setValueList(List.of("l1", "l2", "l3")).build(),
                        "attr5", AttributeValue.newBuilder().setBinaryValue(ByteBuffer.wrap("test-bytes".getBytes())).build()
                    ))
            )
            .build(),
        avroEntity2);
  }
}