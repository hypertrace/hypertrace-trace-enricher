package org.hypertrace.trace.client;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.StructuredTrace;

class TraceValueSource extends AvroBackedValueSource {

  private final StructuredTrace trace;
  private final ValueCoercer valueCoercer;

  TraceValueSource(StructuredTrace trace, ValueCoercer valueCoercer) {
    this.trace = trace;
    this.valueCoercer = valueCoercer;
  }

  @Override
  public Optional<LiteralValue> getAttribute(String key, AttributeKind attributeKind) {
    return this.getAttributeString(this.trace.getAttributes(), key)
        .flatMap(stringValue -> this.valueCoercer.toLiteral(stringValue, attributeKind));
  }

  @Override
  public Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind) {
    return this.getMetricDouble(this.trace.getMetrics(), key)
        .flatMap(doubleValue -> this.valueCoercer.toLiteral(doubleValue, attributeKind));
  }

  @Override
  public Optional<ValueSource> sourceForScope(String scope) {
    return AttributeScope.TRACE.name().equals(scope) ? Optional.of(this) : Optional.empty();
  }
}
