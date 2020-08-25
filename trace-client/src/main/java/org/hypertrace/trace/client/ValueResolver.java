package org.hypertrace.trace.client;

import io.reactivex.rxjava3.core.Maybe;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

public interface ValueResolver {

  Maybe<LiteralValue> resolve(ValueSource valueSource, AttributeMetadata attributeMetadata);

  static ValueResolver build(
      CachingAttributeClient attributeClient, AttributeProjectionRegistry projectionRegistry) {
    return new DefaultValueResolver(
        attributeClient, DefaultValueCoercer.INSTANCE, projectionRegistry);
  }
}
