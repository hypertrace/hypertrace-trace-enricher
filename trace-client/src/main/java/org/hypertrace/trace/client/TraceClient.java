package org.hypertrace.trace.client;

import io.reactivex.rxjava3.core.Maybe;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

public interface TraceClient {
  Maybe<LiteralValue> getSpanValue(Event span, String attributeKey);

  Maybe<LiteralValue> getTraceValue(StructuredTrace trace, String attributeKey);
}
