package org.hypertrace.trace.reader.entities;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityTypeFilter;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc.EntityTypeServiceStub;

class EntityTypeCachingClient implements EntityTypeClient {

  private final EntityTypeServiceStub entityTypeClient;
  private final LoadingCache<CacheContextKey, Single<Map<String, EntityType>>> cache;

  EntityTypeCachingClient(
      @Nonnull Channel channel,
      @Nonnull CallCredentials credentials,
      int maxCacheContexts,
      @Nonnull Duration cacheExpiration) {
    this.entityTypeClient = EntityTypeServiceGrpc.newStub(channel).withCallCredentials(credentials);
    this.cache =
        CacheBuilder.newBuilder()
            .maximumSize(maxCacheContexts)
            .expireAfterWrite(cacheExpiration)
            .build(CacheLoader.from(this::fetchTypes));
  }

  @Override
  public Observable<EntityType> getAll() {
    return this.getOrInvalidate(CacheContextKey.forCurrentContext())
        .flattenAsObservable(Map::values);
  }

  @Override
  public Single<EntityType> get(String name) {
    return this.getOrInvalidate(CacheContextKey.forCurrentContext())
        .mapOptional(map -> Optional.ofNullable(map.get(name)))
        .switchIfEmpty(Single.error(this.buildErrorForMissingType(name)));
  }

  private Single<Map<String, EntityType>> fetchTypes(CacheContextKey key) {
    return key.getExecutionContext().<EntityType>stream(
            streamObserver ->
                this.entityTypeClient.queryEntityTypes(
                    EntityTypeFilter.getDefaultInstance(), streamObserver))
        .toList()
        .map(this::buildMap)
        .cache();
  }

  private Map<String, EntityType> buildMap(List<EntityType> entityTypes) {
    return entityTypes.stream()
        .collect(Collectors.toUnmodifiableMap(EntityType::getName, Function.identity()));
  }

  private Single<Map<String, EntityType>> getOrInvalidate(CacheContextKey key) {
    return this.cache.getUnchecked(key).doOnError(x -> this.cache.invalidate(key));
  }

  private NoSuchElementException buildErrorForMissingType(String name) {
    return new NoSuchElementException(
        String.format("No entity type available for name '%s'", name));
  }
}
