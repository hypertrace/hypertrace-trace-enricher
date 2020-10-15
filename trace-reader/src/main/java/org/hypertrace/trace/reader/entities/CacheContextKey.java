package org.hypertrace.trace.reader.entities;


import java.util.Objects;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;
import org.hypertrace.core.grpcutils.context.RequestContext;

class CacheContextKey {
  static CacheContextKey forCurrentContext() {
    return forContext(RequestContext.CURRENT.get());
  }

  static CacheContextKey forContext(RequestContext context) {
    return new CacheContextKey(Objects.requireNonNull(context));
  }

  private static final String DEFAULT_IDENTITY = "default";

  private final GrpcRxExecutionContext executionContext;
  private final String identity;

  private CacheContextKey(@Nonnull RequestContext requestContext) {
    this.executionContext = GrpcRxExecutionContext.forContext(requestContext);
    this.identity = requestContext.getTenantId().orElse(DEFAULT_IDENTITY);
  }

  public GrpcRxExecutionContext getExecutionContext() {
    return executionContext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CacheContextKey that = (CacheContextKey) o;
    return identity.equals(that.identity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(identity);
  }
}
