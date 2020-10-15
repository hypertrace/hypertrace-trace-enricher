package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.attribute.service.v1.LiteralValue.ValueCase;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.trace.reader.TraceReader;

public class DefaultTraceEntityReader implements TraceEntityReader {

  private final EntityTypeClient entityTypeClient;
  private final EntityClient entityClient;
  private final TraceReader traceReader;
  private final AvroEntityConverter avroEntityConverter;

  DefaultTraceEntityReader(
      EntityTypeClient entityTypeClient,
      EntityClient entityClient,
      TraceReader traceReader,
      AvroEntityConverter avroEntityConverter) {
    this.entityTypeClient = entityTypeClient;
    this.entityClient = entityClient;
    this.traceReader = traceReader;
    this.avroEntityConverter = avroEntityConverter;
  }

  @Override
  public Maybe<Entity> getAssociatedEntityForSpan(
      String entityType, StructuredTrace trace, Event span) {
    return this.getAllAssociatedEntitiesForSpan(trace, span)
        .mapOptional(entityMap -> Optional.ofNullable(entityMap.get(entityType)));
  }

  @Override
  public Single<Map<String, Entity>> getAllAssociatedEntitiesForSpan(
      StructuredTrace trace, Event span) {
    return this.entityTypeClient
        .getAll()
        .flatMapMaybe(entityType -> this.tryBuildEntityEntry(entityType, trace, span))
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
  }

  private Maybe<Entry<String, Entity>> tryBuildEntityEntry(
      EntityType entityType, StructuredTrace trace, Event span) {
    String entityTypeScope = entityType.getAttributeScope();
    return this.traceReader
        .getSpanValue(trace, span, entityTypeScope, entityType.getIdAttributeKey())
        .onErrorComplete()
        .filter(value -> value.getValueCase().equals(ValueCase.STRING_VALUE))
        .map(LiteralValue::getStringValue)
        .flatMapSingle(id -> this.entityClient.getOrCreateEntity(entityTypeScope, id)) // This needs some thought
        .flatMapSingle(
            entity -> this.avroEntityConverter.convertToAvroEntity(span.getCustomerId(), entity))
        .map(entity -> Map.entry(entityTypeScope, entity));
  }
}
