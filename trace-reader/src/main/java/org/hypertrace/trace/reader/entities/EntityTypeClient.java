package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import org.hypertrace.entity.type.service.v2.EntityType;

public interface EntityTypeClient {

  Observable<EntityType> getAll();

  Single<EntityType> get(String name);
}
