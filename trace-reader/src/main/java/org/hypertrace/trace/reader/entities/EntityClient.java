package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.entity.data.service.v2.Entity;

public interface EntityClient {

  Single<Entity> getOrCreateEntity(String entityType, String id);
}
