package org.hypertrace.traceenricher.enrichment.enrichers;

import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Envoy;
import org.hypertrace.core.span.constants.v1.OCAttribute;
import org.hypertrace.core.span.constants.v1.OCSpanKind;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.core.span.constants.v1.SpanNamePrefix;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.util.Constants;
import org.hypertrace.traceenricher.util.EnricherUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Enricher that figures out if an event is an entry event and by adding EVENT_TYPE attribute.
 * "EXIT" means calls going out. "ENTRY" means calls coming in or code executing in the server.
 * "UNKNOWN" means this enricher cannot conclusively figure out if this is exit or entry.
 */
public class SpanTypeAttributeEnricher extends AbstractTraceEnricher {

  // Spans that are generated by OpenCensus Java library have this attribute.
  static final String SPAN_KIND_KEY =
      Constants.getRawSpanConstant(OCAttribute.OC_ATTRIBUTE_SPAN_KIND);
  // Value for span.kind if it's server span.
  static final String SERVER_VALUE =
      Constants.getRawSpanConstant(OCSpanKind.OC_SPAN_KIND_SERVER);

  // The OpenCensus constants below are not shared with anyone, and should only be known
  // by this enricher only.
  // Also, proto enums can't have dot in the names as well.
  // Value for span.kind if it's client span.
  static final String CLIENT_VALUE = Constants.getRawSpanConstant(OCSpanKind.OC_SPAN_KIND_CLIENT);
  // Spans that are generated by Go Opencensus grpc/http clients have this attribute.
  static final String CLIENT_KEY =
      Constants.getRawSpanConstant(OCAttribute.OC_ATTRIBUTE_CLIENT_KEY);
  private static final Logger LOGGER = LoggerFactory.getLogger(SpanTypeAttributeEnricher.class);
  private static final String PROTOCOL_ATTR =
      EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL);
  private static final String GRPC_PROTOCOL_VALUE =
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_GRPC);
  private static final String HTTP_PROTOCOL_VALUE =
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_HTTP);
  private static final Map<String, Protocol> NAME_TO_PROTOCOL_MAP = Map.of(
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_GRPC), Protocol.PROTOCOL_GRPC,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_HTTP), Protocol.PROTOCOL_HTTP,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_HTTPS), Protocol.PROTOCOL_HTTPS,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_REDIS), Protocol.PROTOCOL_REDIS,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_MONGO), Protocol.PROTOCOL_MONGO,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_JDBC), Protocol.PROTOCOL_JDBC
  );

  private final String spanTypeAttrName =
      EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE);
  private final String envoyOperationNameAttr = RawSpanConstants
      .getValue(Envoy.ENVOY_OPERATION_NAME);
  private final String envoyIngressSpanValue = RawSpanConstants.getValue(Envoy.ENVOY_INGRESS_SPAN);
  private final String envoyEgressSpanValue = RawSpanConstants.getValue(Envoy.ENVOY_EGRESS_SPAN);

  @Nonnull
  public static Protocol getProtocolName(Event event) {
    Protocol protocol = getGrpcProtocol(event);

    if (protocol == Protocol.PROTOCOL_GRPC) {
      return protocol;
    } else if (protocol == Protocol.PROTOCOL_UNSPECIFIED || protocol == Protocol.UNRECOGNIZED) {
      return getHttpProtocol(event);
    }

    return Protocol.PROTOCOL_UNSPECIFIED;
  }

  @Nonnull
  public static Protocol getGrpcProtocol(Event event) {
    Map<String, AttributeValue> attributeMap = event.getAttributes().getAttributeMap();

    // check Open Tracing grpc component value first
    AttributeValue componentAttrValue = attributeMap.get(
        RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_COMPONENT));
    if (componentAttrValue != null) {
      if (GRPC_PROTOCOL_VALUE.equalsIgnoreCase(componentAttrValue.getValue())) {
        return Protocol.PROTOCOL_GRPC;
      }
    }

    if (event.getRpc() != null && event.getRpc().getSystem() != null) {
      String rpcSystem = event.getRpc().getSystem();
      if (GRPC_PROTOCOL_VALUE.equalsIgnoreCase(rpcSystem)) {
        return Protocol.PROTOCOL_GRPC;
      }
    }

    /* This logic is the brute force checking if there's any attribute starts with grpc.
     * Unfortunately, depends on the language, instrumented vs non, we can't count on a set
     * of attributes that can identify the protocol.
     */
    for (String attrKey : attributeMap.keySet()) {
      String upperCaseKey = attrKey.toUpperCase();
      if (upperCaseKey.startsWith(GRPC_PROTOCOL_VALUE.toUpperCase())) {
        return Protocol.PROTOCOL_GRPC;
      }
    }

    if (EnricherUtil.isSentGrpcEvent(event) || EnricherUtil.isReceivedGrpcEvent(event)) {
      return Protocol.PROTOCOL_GRPC;
    }

    // this means, there's no grpc prefix protocol
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Couldn't map the event to any protocol; eventId: {}", event.getEventId());
    }

    return Protocol.PROTOCOL_UNSPECIFIED;
  }

  @Nonnull
  public static Protocol getHttpProtocol(Event event) {
    Map<String, AttributeValue> attributeMap = event.getAttributes().getAttributeMap();

    // Try to check whether HTTP or HTTPS based on full URL.
    String fullUrl = EnrichedSpanUtils.getFullHttpUrl(event).orElse(null);
    if (fullUrl != null) {
      try {
        URI uri = new URI(fullUrl);
        Protocol protocol = NAME_TO_PROTOCOL_MAP.get(uri.getScheme().toUpperCase());
        if (protocol != null) {
          return protocol;
        }
      } catch (URISyntaxException ignore) {
        // Ignore these exceptions.
      }
    }

    /* This logic is the brute force checking if there's all attribute starts with http.
     * As in, there shouldn't be any grpc attribute
     * Unfortunately, depends on the language, instrumented vs non, we can't count on a set
     * of attributes that can identify the protocol.
     */
    boolean hasHttpPrefix = false;
    // Go through all attributes check if there's GRPC attribute. If there are any grpc attribute,
    // then it's not a HTTP protocol
    for (String attrKey : attributeMap.keySet()) {
      String upperCaseKey = attrKey.toUpperCase();
      if (upperCaseKey.startsWith(HTTP_PROTOCOL_VALUE.toUpperCase())) {
        // just marking if http exists but do not decide on the protocol.
        // It needs to complete checking grpc before deciding
        hasHttpPrefix = true;
      } else if (upperCaseKey.startsWith(GRPC_PROTOCOL_VALUE.toUpperCase())) {
        // if any of the attribute starts with GRPC, it's not HTTP protocol
        return Protocol.PROTOCOL_UNSPECIFIED;
      }
    }

    // this means, there's no grpc prefix protocol, then check if there were HTTP
    if (hasHttpPrefix) {
      return Protocol.PROTOCOL_HTTP;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Couldn't map the event to any protocol; eventId: {}", event.getEventId());
    }

    return Protocol.PROTOCOL_UNSPECIFIED;
  }

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    if (event.getAttributes() == null) {
      return;
    }

    Map<String, AttributeValue> attributeMap = event.getAttributes().getAttributeMap();
    if (attributeMap == null) {
      return;
    }

    // Figure out if event is entry or exit based on other span attributes
    Boolean isEntry = null;
    if (attributeMap.containsKey(SPAN_KIND_KEY)) {
      String spanKindValue = attributeMap.get(SPAN_KIND_KEY).getValue();
      if (spanKindValue.equalsIgnoreCase(SERVER_VALUE)) {
        isEntry = true;
      } else if (spanKindValue.equalsIgnoreCase(CLIENT_VALUE)) {
        isEntry = false;
      } else {
        LOGGER.debug("Unrecognized span.kind value: {}. Event: {}.", spanKindValue, event);
      }
    } else if (attributeMap.containsKey(CLIENT_KEY)) {
      String clientValue = attributeMap.get(CLIENT_KEY).getValue();
      if (clientValue.equalsIgnoreCase("false")) {
        isEntry = true;
      } else if (clientValue.equalsIgnoreCase("true")) {
        isEntry = false;
      } else {
        LOGGER.debug("Unrecognized Client value: {}. Event: {}.", clientValue, event);
      }
    } else if (attributeMap.containsKey(envoyOperationNameAttr)) {
      String spanType = attributeMap.get(envoyOperationNameAttr).getValue();
      if (StringUtils.equalsIgnoreCase(envoyIngressSpanValue, spanType)) {
        isEntry = true;
      } else if (StringUtils.equalsIgnoreCase(envoyEgressSpanValue, spanType)) {
        isEntry = false;
      } else {
        LOGGER.debug("Unrecognized envoyOperationNameAttr value: {}. Event: {}.", spanType, event);
      }
    } else if (StringUtils.startsWith(event.getEventName(),
        RawSpanConstants.getValue(SpanNamePrefix.SPAN_NAME_PREFIX_SENT))) {
      // Go Opencensus instrumentation seems to have this convention Sent. prefix
      // meaning client/exit/backend call
      isEntry = false;
    } else if (StringUtils.startsWith(event.getEventName(),
        RawSpanConstants.getValue(SpanNamePrefix.SPAN_NAME_PREFIX_RECV))) {
      isEntry = true;
    }

    // Add the new information as an enriched attribute, not raw attribute.
    if (isEntry == null) {
      addEnrichedAttribute(event, spanTypeAttrName, AttributeValueCreator.create(
          Constants.getEnrichedSpanConstant(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_UNSPECIFIED)));
    } else if (isEntry) {
      addEnrichedAttribute(event, spanTypeAttrName, AttributeValueCreator.create(
          Constants.getEnrichedSpanConstant(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY)));
    } else {
      addEnrichedAttribute(event, spanTypeAttrName, AttributeValueCreator.create(
          Constants.getEnrichedSpanConstant(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT)));
    }

    // Get the protocol and name and create API entity based on the protocol.
    Protocol protocol = getProtocolName(event);
    addEnrichedAttribute(event, PROTOCOL_ATTR,
        AttributeValueCreator.create(Constants.getEnrichedSpanConstant(protocol)));
  }
}
