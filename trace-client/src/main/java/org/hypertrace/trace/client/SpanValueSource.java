package org.hypertrace.trace.client;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

class SpanValueSource extends AvroBackedValueSource {

  private final StructuredTrace trace;
  private final Event span;
  private final ValueCoercer valueCoercer;

  SpanValueSource(StructuredTrace trace, Event span, ValueCoercer valueCoercer) {
    this.trace = trace;
    this.span = span;
    this.valueCoercer = valueCoercer;
  }

  @Override
  public Optional<LiteralValue> getAttribute(String key, AttributeKind attributeKind) {
    return this.getAttributeString(this.span.getEnrichedAttributes(), key)
        .or(() -> this.getAttributeString(this.span.getAttributes(), key))
        .flatMap(stringValue -> this.valueCoercer.toLiteral(stringValue, attributeKind));
  }

  @Override
  public Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind) {
    return this.getMetricDouble(this.span.getMetrics(), key)
        .flatMap(doubleValue -> this.valueCoercer.toLiteral(doubleValue, attributeKind));
  }

  @Override
  public Optional<ValueSource> sourceForScope(String scope) {
    return AttributeScope.TRACE.name().equals(scope)
        ? Optional.of(ValueSource.forTrace(this.trace))
        : Optional.of(this);
  }
}
