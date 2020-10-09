package org.hypertrace.trace.reader;

import io.grpc.ManagedChannel;
import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

class DefaultTraceReader implements TraceReader {

  private final CachingAttributeClient attributeClient;
  private final ValueResolver valueResolver;
  private final AttributeProjectionRegistry projectionRegistry;

  DefaultTraceReader(ManagedChannel attributeServiceChannel) {
    this.attributeClient =
        CachingAttributeClient.builder(attributeServiceChannel)
            .withAttributeFilter(
                AttributeMetadataFilter.newBuilder()
                    .addScope(AttributeScope.EVENT)
                    .addScope(AttributeScope.TRACE)
                    .build())
            .build();
    this.projectionRegistry = new AttributeProjectionRegistry();
    this.valueResolver = ValueResolver.build(this.attributeClient, this.projectionRegistry);
  }

  @Override
  public Single<LiteralValue> getSpanValue(
      StructuredTrace trace, Event span, String attributeScope, String attributeKey) {
    return this.getAttribute(attributeScope, attributeKey)
        .flatMap(
            definition -> this.valueResolver.resolve(ValueSource.forSpan(trace, span), definition));
  }

  @Override
  public Single<LiteralValue> getTraceValue(StructuredTrace trace, String attributeKey) {
    return this.getAttribute(AttributeScope.TRACE.name(), attributeKey)
        .flatMap(definition -> this.valueResolver.resolve(ValueSource.forTrace(trace), definition));
  }

  private Single<AttributeMetadata> getAttribute(String attributeScope, String attributeKey) {
    return this.attributeClient.get(attributeScope, attributeKey);
  }
}
