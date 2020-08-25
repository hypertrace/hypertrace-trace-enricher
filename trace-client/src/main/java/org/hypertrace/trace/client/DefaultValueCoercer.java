package org.hypertrace.trace.client;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

class DefaultValueCoercer implements ValueCoercer {
  static final DefaultValueCoercer INSTANCE = new DefaultValueCoercer();

  private DefaultValueCoercer() {}

  @Override
  public Optional<?> fromLiteral(LiteralValue value, AttributeKind attributeKind) {
    switch (attributeKind) {
      case TYPE_DOUBLE:
        return this.extractDoubleValue(value);
      case TYPE_INT64:
        return this.extractLongValue(value);
      case TYPE_TIMESTAMP:
        return this.extractTimestamp(value);
      case TYPE_BOOL:
        return this.extractBooleanValue(value);
      case TYPE_STRING:
      case TYPE_BYTES: // Treating bytes as equivalent to string
        return this.extractStringValue(value);
      case KIND_UNDEFINED:
        return this.extractAnyValue(value);
      default:
        return Optional.empty();
    }
  }

  @Override
  public Optional<LiteralValue> toLiteral(Object value) {
    if (isNull(value)) {
      return Optional.empty();
    }
    if (this.isAssignableToAnyOfClasses(value.getClass(), CharSequence.class)) {
      return Optional.of(stringLiteral(value.toString()));
    }
    if (this.isAssignableToAnyOfClasses(value.getClass(), Boolean.class)) {
      return Optional.of(booleanLiteral((Boolean) value));
    }
    if (this.isAssignableToAnyOfClasses(
        value.getClass(), Long.class, Integer.class, BigInteger.class)) {
      return Optional.of(longLiteral((Number) value));
    }
    if (this.isAssignableToAnyOfClasses(value.getClass(), TemporalAccessor.class)) {
      return Optional.of(longLiteral(Instant.from((TemporalAccessor) value).toEpochMilli()));
    }
    if (this.isAssignableToAnyOfClasses(
        value.getClass(), Double.class, Float.class, BigDecimal.class)) {
      return Optional.of(doubleLiteral((Number) value));
    }
    return Optional.empty();
  }

  @Override
  public Optional<LiteralValue> toLiteral(String stringValue, AttributeKind attributeKind) {

    switch (attributeKind) {
      case TYPE_DOUBLE:
        return tryParseDouble(stringValue).map(this::doubleLiteral);
      case TYPE_INT64:
        return tryParseLong(stringValue).map(this::longLiteral);
      case TYPE_BOOL:
        return tryParseBoolean(stringValue).map(this::booleanLiteral);
      case TYPE_STRING:
      case TYPE_BYTES: // Treat bytes and string the same
        return Optional.of(stringLiteral(stringValue));
      case TYPE_TIMESTAMP:
        return tryParseLong(stringValue)
            .or(() -> tryParseTimestamp(stringValue))
            .map(this::longLiteral);
      default:
        return Optional.empty();
    }
  }

  @Override
  public Optional<LiteralValue> toLiteral(Double doubleValue, AttributeKind attributeKind) {
    switch (attributeKind) {
      case TYPE_DOUBLE:
        return Optional.of(doubleLiteral(doubleValue));
      case TYPE_TIMESTAMP:
      case TYPE_INT64: // Timestamp and long both convert the same
        return Optional.of(longLiteral(doubleValue));
      case TYPE_STRING:
      case TYPE_BYTES: // Treat bytes and string the same
        return Optional.of(stringLiteral(doubleValue.toString()));

      default:
        return Optional.empty();
    }
  }

  private Optional<Boolean> extractBooleanValue(LiteralValue value) {
    switch (value.getValueCase()) {
      case BOOLEAN_VALUE:
        return Optional.of(value.getBooleanValue());
      case STRING_VALUE:
        return tryParseBoolean(value.getStringValue());
      default:
        return Optional.empty();
    }
  }

  private Optional<String> extractStringValue(LiteralValue value) {
    switch (value.getValueCase()) {
      case BOOLEAN_VALUE:
        return Optional.of(String.valueOf(value.getBooleanValue()));
      case INT_VALUE:
        return Optional.of(String.valueOf(value.getIntValue()));
      case FLOAT_VALUE:
        return Optional.of(String.valueOf(value.getFloatValue()));
      case STRING_VALUE:
        return Optional.of(value.getStringValue());
      default:
        return Optional.empty();
    }
  }

  private Optional<Long> extractLongValue(LiteralValue value) {
    switch (value.getValueCase()) {
      case FLOAT_VALUE:
        return Optional.of(Double.valueOf(value.getFloatValue()).longValue());
      case INT_VALUE:
        return Optional.of(value.getIntValue());
      case STRING_VALUE:
        return tryParseLong(value.getStringValue());
      default:
        return Optional.empty();
    }
  }

  private Optional<Double> extractDoubleValue(LiteralValue value) {
    switch (value.getValueCase()) {
      case FLOAT_VALUE:
        return Optional.of(value.getFloatValue());
      case INT_VALUE:
        return Optional.of(Long.valueOf(value.getIntValue()).doubleValue());
      case STRING_VALUE:
        return tryParseDouble(value.getStringValue());
      default:
        return Optional.empty();
    }
  }

  private Optional<Long> extractTimestamp(LiteralValue value) {
    return this.extractLongValue(value).or(() -> this.tryParseTimestamp(value.getStringValue()));
  }

  private Optional<?> extractAnyValue(LiteralValue value) {
    switch (value.getValueCase()) {
      case BOOLEAN_VALUE:
        return Optional.of(value.getBooleanValue());
      case INT_VALUE:
        return Optional.of(value.getIntValue());
      case FLOAT_VALUE:
        return Optional.of(value.getFloatValue());
      case STRING_VALUE:
        return Optional.of(value.getStringValue());
      default:
        return Optional.empty();
    }
  }

  private boolean isAssignableToAnyOfClasses(Class<?> classToCheck, Class<?>... classesAllowed) {
    for (Class<?> allowedClass : classesAllowed) {
      if (allowedClass.isAssignableFrom(classToCheck)) return true;
    }
    return false;
  }

  private LiteralValue stringLiteral(@Nonnull String stringValue) {
    return LiteralValue.newBuilder().setStringValue(stringValue).build();
  }

  private LiteralValue longLiteral(@Nonnull Number number) {
    return LiteralValue.newBuilder().setIntValue(number.longValue()).build();
  }

  private LiteralValue doubleLiteral(@Nonnull Number number) {
    return LiteralValue.newBuilder().setFloatValue(number.doubleValue()).build();
  }

  private LiteralValue booleanLiteral(boolean booleanValue) {
    return LiteralValue.newBuilder().setBooleanValue(booleanValue).build();
  }

  private Optional<Long> tryParseLong(@Nullable String intString) {
    try {
      return Optional.of(Long.valueOf(requireNonNull(intString)));
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }

  private Optional<Double> tryParseDouble(@Nullable String doubleString) {
    try {
      return Optional.of(Double.valueOf(requireNonNull(doubleString)));
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }

  private Optional<Boolean> tryParseBoolean(@Nullable String booleanString) {
    if ("true".equalsIgnoreCase(booleanString)) {
      return Optional.of(Boolean.TRUE);
    }
    if ("false".equalsIgnoreCase(booleanString)) {
      return Optional.of(Boolean.FALSE);
    }
    return Optional.empty();
  }

  private Optional<Long> tryParseTimestamp(@Nullable String dateString) {
    try {
      return Optional.of(Instant.parse(requireNonNull(dateString))).map(Instant::toEpochMilli);
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }
}
