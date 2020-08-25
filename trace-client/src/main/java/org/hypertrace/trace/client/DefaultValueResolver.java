package org.hypertrace.trace.client;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjection;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.attribute.service.v1.Projection;
import org.hypertrace.core.attribute.service.v1.ProjectionExpression;

class DefaultValueResolver implements ValueResolver {
  private final CachingAttributeClient attributeClient;
  private final ValueCoercer valueCoercer;
  private final AttributeProjectionRegistry attributeProjectionRegistry;

  DefaultValueResolver(
      CachingAttributeClient attributeClient,
      ValueCoercer valueCoercer,
      AttributeProjectionRegistry attributeProjectionRegistry) {
    this.attributeClient = attributeClient;
    this.valueCoercer = valueCoercer;
    this.attributeProjectionRegistry = attributeProjectionRegistry;
  }

  @Override
  public Maybe<LiteralValue> resolve(ValueSource valueSource, AttributeMetadata attributeMetadata) {
    if (!attributeMetadata.hasDefinition()) {
      return Maybe.error(new NoSuchElementException("Attribute definition not set"));
    }

    switch (attributeMetadata.getDefinition().getValueCase()) {
      case SPAN_PATH:
        return this.resolveValue(
            valueSource,
            attributeMetadata.getType(),
            attributeMetadata.getValueKind(),
            attributeMetadata.getDefinition().getSpanPath());
      case PROJECTION:
        return this.resolveProjection(
            valueSource,
            attributeMetadata.getDefinition().getProjection(),
            attributeMetadata.getScope());
      case VALUE_NOT_SET:
      default:
        return Maybe.error(new NoSuchElementException("Unrecognized attribute definition"));
    }
  }

  private Maybe<LiteralValue> resolveValue(
      ValueSource valueSource,
      AttributeType attributeType,
      AttributeKind attributeKind,
      String path) {
    switch (attributeType) {
      case ATTRIBUTE:
        return Maybe.fromOptional(valueSource.getAttribute(path, attributeKind));
      case METRIC:
        return Maybe.fromOptional(valueSource.getMetric(path, attributeKind));
      case UNRECOGNIZED:
      case TYPE_UNDEFINED:
      default:
        return Maybe.error(new NoSuchElementException("Unrecognized projection type"));
    }
  }

  private Maybe<LiteralValue> resolveProjection(
      ValueSource valueSource, Projection projection, AttributeScope scope) {
    switch (projection.getValueCase()) {
      case ATTRIBUTE_KEY:
        return this.attributeClient
            .get(scope.name(), projection.getAttributeKey())
            .flatMapMaybe(projectedAttribute -> this.resolve(valueSource, projectedAttribute));
      case LITERAL:
        return Maybe.just(projection.getLiteral());
      case EXPRESSION:
        return this.resolveExpression(valueSource, projection.getExpression(), scope);
      case VALUE_NOT_SET:
      default:
        return Maybe.error(new NoSuchElementException("Unrecognized projection type"));
    }
  }

  private Maybe<LiteralValue> resolveExpression(
      ValueSource valueSource, ProjectionExpression expression, AttributeScope scope) {
    return Maybe.fromOptional(
            this.attributeProjectionRegistry.getProjection(expression.getOperator()))
        .flatMap(
            projectionFunction ->
                this.resolveArgumentList(
                        valueSource, projectionFunction, expression.getArgumentsList(), scope)
                    .map(projectionFunction::project))
        .mapOptional(this.valueCoercer::toLiteral);
  }

  private Maybe<List<Object>> resolveArgumentList(
      ValueSource valueSource,
      AttributeProjection projectionFunction,
      List<Projection> arguments,
      AttributeScope attributeScope) {
    return Observable.range(0, arguments.size())
        .flatMapMaybe(
            index ->
                this.resolveArgument(
                    valueSource,
                    arguments.get(index),
                    projectionFunction.getArgumentKindAtIndex(index),
                    attributeScope))
        .collect(Collectors.toList())
        .toMaybe();
  }

  private Maybe<Object> resolveArgument(
      ValueSource valueSource,
      Projection projection,
      AttributeKind attributeKind,
      AttributeScope attributeScope) {
    return this.resolveProjection(valueSource, projection, attributeScope)
        .mapOptional(x -> this.valueCoercer.fromLiteral(x, attributeKind));
  }
}
