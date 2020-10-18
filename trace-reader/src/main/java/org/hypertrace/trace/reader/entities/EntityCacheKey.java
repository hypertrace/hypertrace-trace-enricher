package org.hypertrace.trace.reader.entities;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;

class EntityCacheKey extends TenantBasedCacheKey {

  static EntityCacheKey entityInCurrentContext(Entity inputEntity) {
    return new EntityCacheKey(
        requireNonNull(RequestContext.CURRENT.get()), requireNonNull(inputEntity));
  }

  private final Entity inputEntity;

  protected EntityCacheKey(RequestContext requestContext, Entity inputEntity) {
    super(requestContext);

    this.inputEntity = inputEntity;
  }

  public Entity getInputEntity() {
    return inputEntity;
  }

  protected String getEntityType() {
    return this.getInputEntity().getEntityType();
  }

  protected String getEntityId() {
    return this.getInputEntity().getEntityId();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    EntityCacheKey that = (EntityCacheKey) o;
    return getEntityId().equals(that.getEntityId()) && getEntityType().equals(that.getEntityType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getEntityId(), getEntityType());
  }
}
