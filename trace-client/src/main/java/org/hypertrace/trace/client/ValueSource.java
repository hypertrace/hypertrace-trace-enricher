package org.hypertrace.trace.client;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

public interface ValueSource {
  Optional<LiteralValue> getAttribute(String key, AttributeKind attributeKind);

  Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind);

  static ValueSource forSpan(Event span) {
    return new SpanValueSource(span, DefaultValueCoercer.INSTANCE);
  }

  static ValueSource forTrace(StructuredTrace trace) {
    return new TraceValueSource(trace, DefaultValueCoercer.INSTANCE);
  }
}
