package org.hypertrace.traceenricher.enrichedspan.constants.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.reflect.Nullable;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.eventfields.http.RequestHeaders;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Docker;
import org.hypertrace.core.span.constants.v1.TracerAttribute;
import org.hypertrace.entity.constants.v1.ApiAttribute;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.constants.v1.K8sEntityAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Http;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;

import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;

/**
 * Utility class to easily read named attributes from an enriched span. This is equivalent of
 * an enriched span POJO.
 */
public class EnrichedSpanUtils {
  private static final String SERVICE_ID_ATTR =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_ID);
  private static final String SERVICE_NAME_ATTR =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_NAME);

  private static final String API_ID_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_ID);
  private static final String API_URL_PATTERN_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_URL_PATTERN);
  private static final String API_NAME_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME);
  private static final String API_DISCOVERY_STATE_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_DISCOVERY_STATE);

  private static final String NAMESPACE_NAME_ATTR =
      EntityConstants.getValue(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME);
  private static final String CLUSTER_NAME_ATTR =
      EntityConstants.getValue(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME);

  private static final String BACKEND_ID_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_ID);
  private static final String BACKEND_NAME_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_NAME);
  private static final String BACKEND_HOST_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST);
  private static final String BACKEND_PORT_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT);
  private static final String BACKEND_PROTOCOL_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL);
  private static final String BACKEND_PATH_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH);

  private static final String SPAN_TYPE_ATTR =
      EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE);
  private static final String API_BOUNDARY_TYPE_ATTR =
      EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE);
  private static final String TRACER_TYPE_ATTR =
      RawSpanConstants.getValue(TracerAttribute.TRACER_ATTRIBUTE_TRACER_TYPE);
  private static final String PROTOCOL_ATTR =
      EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL);
  private static final String HOST_HEADER_ATTR = EnrichedSpanConstants.getValue(Http.HTTP_HOST);
  private static final String HTTP_USER_AGENT =
      RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT);

  private static final String CONTAINER_ID_ATTR =
      RawSpanConstants.getValue(Docker.DOCKER_CONTAINER_ID);
  private static final String USER_AGENT =
      RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT);
  private static final String USER_AGENT_UNDERSCORE =
      RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE);
  private static final String USER_AGENT_DASH =
      RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH);
  private static final String USER_AGENT_REQUEST_HEADER =
      RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER);

  private static final String HTTP_PREFIX = "http";
  private static final List<String> FULL_URL_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL),
                  RawSpanConstants.getValue(HTTP_REQUEST_URL),
                  RawSpanConstants.getValue(HTTP_URL));

  @VisibleForTesting
  static final List<String> USER_AGENT_ATTRIBUTES =
      ImmutableList.of(USER_AGENT, USER_AGENT_UNDERSCORE, USER_AGENT_DASH, USER_AGENT_REQUEST_HEADER, HTTP_USER_AGENT);

  @Nullable
  private static String getStringAttribute(Event event, String attributeKey) {
    AttributeValue value = SpanAttributeUtils.getAttributeValue(event, attributeKey);
    return value == null ? null : value.getValue();
  }

  public static Protocol getProtocol(Event event) {
    String protocol = getStringAttribute(event, PROTOCOL_ATTR);
    if (protocol != null) {
      for (Protocol p : Protocol.values()) {
        if (p != Protocol.UNRECOGNIZED && EnrichedSpanConstants.getValue(p).equals(protocol)) {
          return p;
        }
      }
    }
    return null;
  }

  public static Protocol getProtocol(Event.Builder eventBuilder) {
    String protocol = SpanAttributeUtils.getStringAttribute(eventBuilder, PROTOCOL_ATTR);
    if (protocol != null) {
      for (Protocol p : Protocol.values()) {
        if (p != Protocol.UNRECOGNIZED && EnrichedSpanConstants.getValue(p).equals(protocol)) {
          return p;
        }
      }
    }
    return null;
  }

  public static String getDockerContainerId(Event event) {
    return getStringAttribute(event, CONTAINER_ID_ATTR);
  }

  public static String getServiceId(Event event) {
    return getStringAttribute(event, SERVICE_ID_ATTR);
  }

  public static String getServiceName(Event event) {
    return getStringAttribute(event, SERVICE_NAME_ATTR);
  }

  public static String getBackendId(Event event) {
    return getStringAttribute(event, BACKEND_ID_ATTR);
  }

  public static String getBackendName(Event event) {
    return getStringAttribute(event, BACKEND_NAME_ATTR);
  }

  public static String getBackendHost(Event event) {
    return getStringAttribute(event, BACKEND_HOST_ATTR);
  }

  public static String getBackendPort(Event event) {
    return getStringAttribute(event, BACKEND_PORT_ATTR);
  }

  public static String getBackendPath(Event event) {
    return getStringAttribute(event, BACKEND_PATH_ATTR);
  }

  public static String getBackendProtocol(Event event) {
    return getStringAttribute(event, BACKEND_PROTOCOL_ATTR);
  }

  public static String getNamespaceName(Event event) {
    return getStringAttribute(event, NAMESPACE_NAME_ATTR);
  }

  public static String getApiId(Event event) {
    return getStringAttribute(event, API_ID_ATTR);
  }

  public static String getApiPattern(Event event) {
    return getStringAttribute(event, API_URL_PATTERN_ATTR);
  }

  public static String getApiName(Event event) {
    return getStringAttribute(event, API_NAME_ATTR);
  }

  public static String getApiDiscoveryState(Event event) {
    return getStringAttribute(event, API_DISCOVERY_STATE_ATTR);
  }

  public static boolean isExternalApi(Event e) {
    return SpanAttributeUtils.getBooleanAttribute(e,
        EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_IS_EXTERNAL_API));
  }

  public static String getSpanType(Event event) {
    return getStringAttribute(event, SPAN_TYPE_ATTR);
  }

  public static String getTracerType(Event event) {
    return getStringAttribute(event, TRACER_TYPE_ATTR);
  }

  @Nullable
  public static String getApiBoundaryType(Event event) {
    return getStringAttribute(event, API_BOUNDARY_TYPE_ATTR);
  }


  /**
   * Find the First Span (Entrance Span) of the Api Trace and return its id
   */
  @Nullable
  public static ByteBuffer getApiEntrySpanId(
      Event event,
      Map<ByteBuffer, Event> idToEvent,
      Map<ByteBuffer, ByteBuffer> childToParent) {
    Event entryApiEvent = getApiEntrySpan(event, idToEvent, childToParent);
    if (entryApiEvent != null) {
      return entryApiEvent.getEventId();
    }
    return null;
  }

  /**
   * Helper method to find and entryApiEvent by iterate parent-child chain.
   */
  @Nullable
  public static Event getApiEntrySpan(Event event,
                                      Map<ByteBuffer, Event> idToEvent,
                                      Map<ByteBuffer, ByteBuffer> childToParent) {
    String apiBoundary = getApiBoundaryType(event);
    if (EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY).equals(apiBoundary)) {
      // if current span itself is an api entry span, return same.
      return event;
    } else {
      // current span is not an api entry span, find an ancestor who is an api entry span
      Event parentEvent = idToEvent.get(childToParent.get(event.getEventId()));
      while (parentEvent != null) {
        apiBoundary = getApiBoundaryType(parentEvent);
        if (EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY).equals(apiBoundary)) {
          return parentEvent;
        }
        parentEvent = idToEvent.get(childToParent.get(parentEvent.getEventId()));
      }
    }
    // oops, we didn't find the any api entry span in the parent-child chain
    return null;
  }

  public static boolean isEntryApiBoundary(Event event) {
    return EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY)
        .equalsIgnoreCase(getApiBoundaryType(event));
  }

  public static boolean isExitSpan(Event event) {
    if (event == null) {
      return false;
    }
    return EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT)
        .equalsIgnoreCase(getSpanType(event));
  }

  public static boolean isEntrySpan(Event event) {
    if (event == null) {
      return false;
    }
    return EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY)
        .equalsIgnoreCase(getSpanType(event));
  }

  public static boolean isExitApiBoundary(Event event) {
    return EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT)
        .equalsIgnoreCase(getApiBoundaryType(event));
  }

  public static String getClusterName(Event span) {
    return getStringAttribute(span, CLUSTER_NAME_ATTR);
  }

  public static String getHostHeader(Event span) {
    return getStringAttribute(span, HOST_HEADER_ATTR);
  }

  public static boolean containsServiceId(Event span) {
    return SpanAttributeUtils.containsAttributeKey(span, SERVICE_ID_ATTR);
  }

  @Nullable
  public static String getStatus(Event event) {
    return getStringAttribute(event, EnrichedSpanConstants.getValue(Api.API_STATUS));
  }

  @Nullable
  public static String getStatusCode(Event event) {
    return getStringAttribute(event, EnrichedSpanConstants.getValue(Api.API_STATUS_CODE));
  }

  @Nullable
  public static String getStatusMessage(Event event) {
    return getStringAttribute(event, EnrichedSpanConstants.getValue(Api.API_STATUS_MESSAGE));
  }

  @Nullable
  public static String getUserAgent(Event event) {
    return SpanAttributeUtils.getFirstAvailableStringAttribute(event, USER_AGENT_ATTRIBUTES);
  }

  public static Optional<String> getHttpMethod(Event event) {
    if (event.getHttp() != null && event.getHttp().getRequest() != null) {
      return Optional.ofNullable(event.getHttp().getRequest().getMethod());
    }

    return Optional.empty();
  }

  /**
   * This method returns an Optional which, if present, contains the full URL, including the protocol and host name.
   * If the tags in the raw span do not contain the full URL, it tries to construct a full URL using the protocol
   * ("http") and host name.
   */
  public static Optional<String> getFullHttpUrl(Event event) {
    Optional<String> optionalUrl = Optional.ofNullable(event.getHttp())
            .map(org.hypertrace.core.datamodel.eventfields.http.Http::getRequest)
            .map(Request::getUrl);

    if (optionalUrl.isEmpty() && event.getAttributes() != null && event.getAttributes().getAttributeMap() != null) {
      // iterate on all URL related attribute keys and find / create one with protocol, host header and path
      Map<String, AttributeValue> attributesMap = event.getAttributes().getAttributeMap();
      for (String urlAttribute : FULL_URL_ATTRIBUTES) {
        if (attributesMap.containsKey(urlAttribute)) {
          String url = attributesMap.get(urlAttribute).getValue();
          if (url != null && url.toLowerCase().startsWith(HTTP_PREFIX)) {
            optionalUrl = Optional.of(url);
          } else {
            optionalUrl = constructHttpUrlFromHostName(event);
          }
        }
      }
    }
    return optionalUrl;
  }

  private static Optional<String> constructHttpUrlFromHostName(Event event) {
    Optional<String> optionalUrl = Optional.empty();
    Optional<RequestHeaders> requestHeaders = Optional.ofNullable(event.getHttp())
            .map(org.hypertrace.core.datamodel.eventfields.http.Http::getRequest)
            .map(Request::getHeaders);
    Optional<String> hostName = requestHeaders.map(RequestHeaders::getHost);
    Optional<String> path = requestHeaders.map(RequestHeaders::getPath);
    if (hostName.isPresent() && path.isPresent()) {
      try {
        URL fullUrl = new URL(HTTP_PREFIX, hostName.get(), path.get());
        optionalUrl = Optional.of(fullUrl.toString());
      } catch (MalformedURLException e) {
        // ignore. optionalUrl will be empty.
      }
    }
    return optionalUrl;
  }

  /**
   * This method returns an Optional which, if present, contains the URL as captured in the tags. It may or may not
   * be a full URL. For instance, it may only contain the path and not the protocol and hostname like
   * "/customer?customer=392".
   */
  public static Optional<String> getHttpUrl(Event event) {
    Optional<String> optionalUrl = Optional.ofNullable(event.getHttp())
            .map(org.hypertrace.core.datamodel.eventfields.http.Http::getRequest)
            .map(Request::getUrl);

    if (optionalUrl.isEmpty() && event.getAttributes() != null && event.getAttributes().getAttributeMap() != null) {
      // iterate on all URL related attribute keys and find the first non-empty one
      Map<String, AttributeValue> attributesMap = event.getAttributes().getAttributeMap();
      for (String urlAttribute : FULL_URL_ATTRIBUTES) {
        if (attributesMap.containsKey(urlAttribute)) {
          String url = attributesMap.get(urlAttribute).getValue();
          if (url != null && !url.isEmpty()) {
            optionalUrl = Optional.of(url);
          }
        }
      }
    }
    return optionalUrl;
  }

  public static Optional<Integer> getRequestSize(Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    if (protocol == null) {
      return Optional.empty();
    }

    switch (protocol) {
      case PROTOCOL_HTTP:
      case PROTOCOL_HTTPS:
        if (event.getHttp() != null && event.getHttp().getRequest() != null) {
          return Optional.of(event.getHttp().getRequest().getSize());
        }
        break;
      case PROTOCOL_GRPC:
        if (event.getGrpc() != null && event.getGrpc().getRequest() != null) {
          return Optional.of(event.getGrpc().getRequest().getSize());
        }
        break;
    }

    return Optional.empty();
  }

  public static Optional<Integer> getResponseSize(Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    if (protocol == null) {
      return Optional.empty();
    }

    switch (protocol) {
      case PROTOCOL_HTTP:
      case PROTOCOL_HTTPS:
        if (event.getHttp() != null && event.getHttp().getResponse() != null) {
          return Optional.of(event.getHttp().getResponse().getSize());
        }
        break;
      case PROTOCOL_GRPC:
        if (event.getGrpc() != null && event.getGrpc().getResponse() != null) {
          return Optional.of(event.getGrpc().getResponse().getSize());
        }
        break;
    }

    return Optional.empty();
  }
}
