package org.hypertrace.trace.client;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

public interface ValueCoercer {

  Optional<?> fromLiteral(LiteralValue value, AttributeKind attributeKind);

  Optional<LiteralValue> toLiteral(Object value);

  Optional<LiteralValue> toLiteral(String stringValue, AttributeKind attributeKind);

  Optional<LiteralValue> toLiteral(Double doubleValue, AttributeKind attributeKind);
}
