package org.hypertrace.trace.reader.entities;

import static io.reactivex.rxjava3.core.Maybe.zip;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.v2.AttributeValue;
import org.hypertrace.entity.data.service.v2.AttributeValue.TypeCase;
import org.hypertrace.entity.data.service.v2.Value;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.trace.reader.TraceReader;

public class DefaultTraceEntityReader implements TraceEntityReader {

  private final EntityTypeClient entityTypeClient;
  private final EntityClient entityClient;
  private final CachingAttributeClient attributeClient;
  private final TraceReader traceReader;
  private final AvroEntityConverter avroEntityConverter;
  private final AttributeValueConverter attributeValueConverter;

  DefaultTraceEntityReader(
      EntityTypeClient entityTypeClient,
      EntityClient entityClient,
      CachingAttributeClient attributeClient,
      TraceReader traceReader,
      AvroEntityConverter avroEntityConverter,
      AttributeValueConverter attributeValueConverter) {
    this.entityTypeClient = entityTypeClient;
    this.entityClient = entityClient;
    this.attributeClient = attributeClient;
    this.traceReader = traceReader;
    this.avroEntityConverter = avroEntityConverter;
    this.attributeValueConverter = attributeValueConverter;
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

    // Somewhere in here we should upsert the entity (if needed) - do we wait on it to return?
    return this.entityTypeClient
        .getAll()
        .flatMapMaybe(entityType -> this.buildEntity(entityType, trace, span))
        .flatMapSingle(
            entity -> this.avroEntityConverter.convertToAvroEntity(span.getCustomerId(), entity))
        .toMap(Entity::getEntityType)
        .map(Collections::unmodifiableMap);
  }

  private Maybe<org.hypertrace.entity.data.service.v2.Entity> buildEntity(
      EntityType entityType, StructuredTrace trace, Event span) {
    Maybe<Map<String, AttributeValue>> attributes =
        this.resolveAllAttributes(entityType.getAttributeScope(), trace, span).cache();

    Maybe<String> id =
        attributes.mapOptional(map -> this.extractString(map, entityType.getIdAttributeKey()));

    Maybe<String> name =
        attributes.mapOptional(map -> this.extractString(map, entityType.getNameAttributeKey()));

    return zip(
        id,
        name,
        attributes,
        (resolvedId, resolvedName, resolvedAttributeMap) ->
            org.hypertrace.entity.data.service.v2.Entity.newBuilder()
                .setEntityId(resolvedId)
                .setEntityType(entityType.getAttributeScope())
                .setEntityName(resolvedName)
                .putAllAttributes(resolvedAttributeMap)
                .build());
  }

  private Maybe<Map<String, AttributeValue>> resolveAllAttributes(
      String scope, StructuredTrace trace, Event span) {
    return this.attributeClient
        .getAllInScope(scope)
        .flattenAsObservable(list -> list)
        .filter(attributeMetadata -> attributeMetadata.getType().equals(AttributeType.ATTRIBUTE))
        .flatMapMaybe(attributeMetadata -> this.resolveAttribute(attributeMetadata, trace, span))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue))
        .toMaybe();
  }

  private Maybe<Entry<String, AttributeValue>> resolveAttribute(
      AttributeMetadata attributeMetadata, StructuredTrace trace, Event span) {
    return this.traceReader
        .getSpanValue(
            trace, span, attributeMetadata.getScope().toString(), attributeMetadata.getKey())
        .onErrorComplete()
        .flatMapSingle(this.attributeValueConverter::convert)
        .map(value -> Map.entry(attributeMetadata.getKey(), value));
  }

  private Optional<String> extractString(
      Map<String, AttributeValue> attributeValueMap, String key) {
    return Optional.ofNullable(attributeValueMap.get(key))
        .filter(value -> value.getTypeCase().equals(TypeCase.VALUE))
        .map(AttributeValue::getValue)
        .filter(value -> value.getTypeCase().equals(Value.TypeCase.STRING))
        .map(Value::getString);
  }
}
