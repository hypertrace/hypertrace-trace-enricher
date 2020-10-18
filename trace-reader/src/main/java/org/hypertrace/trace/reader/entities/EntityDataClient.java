package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.entity.data.service.v1.Entity;

public interface EntityDataClient {

  Single<Entity> getOrCreateEntity(Entity entity);
}
