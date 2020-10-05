package org.hypertrace.traceenricher.enrichment.enrichers;

<<<<<<< HEAD
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Map;
=======
import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.OperatingSystemFamily;
import net.sf.uadetector.ReadableDeviceCategory;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgent;
import net.sf.uadetector.VersionNumber;
import net.sf.uadetector.service.UADetectorServiceFactory;
>>>>>>> Use named fields for enriching grpc info
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.eventfields.http.RequestHeaders;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserAgentSpanEnricherTest extends AbstractAttributeEnricherTest {

  private static final String USER_AGENT_HEADER =
      UserAgentSpanEnricher.HTTP_REQUEST_HEADER_PREFIX + UserAgentSpanEnricher.USER_AGENT_HEADER;

  private static final String RPC_USER_AGENT_HEADER =
      UserAgentSpanEnricher.RPC_REQUEST_METADATA_PREFIX + UserAgentSpanEnricher.USER_AGENT_HEADER;

  private UserAgentSpanEnricher enricher;

  @BeforeEach
  public void setUp() {
    enricher = new UserAgentSpanEnricher();
  }

  @Test
  public void noUserAgent() {
    // no http present
    {
      Event e = createMockEvent();
      when(e.getHttp()).thenReturn(null);
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no request present for http
    {
      Event e = createMockEvent();
      when(e.getHttp()).thenReturn(Http.newBuilder().build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no request headers and user agent present for request
    {
      Event e = createMockEvent();
      when(e.getHttp())
          .thenReturn(Http.newBuilder().setRequest(Request.newBuilder().build()).build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no user agent present
    {
      Event e = createMockEvent();
      when(e.getHttp())
          .thenReturn(
              Http.newBuilder()
                  .setRequest(
                      Request.newBuilder().setHeaders(RequestHeaders.newBuilder().build()).build())
                  .build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // empty user agent on headers
    {
      Event e = createMockEvent();
      when(e.getHttp())
          .thenReturn(
              Http.newBuilder()
                  .setRequest(
                      Request.newBuilder()
                          .setHeaders(RequestHeaders.newBuilder().setUserAgent("").build())
                          .build())
                  .build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // empty user agent on request
    {
      Event e = createMockEvent();
      when(e.getHttp())
          .thenReturn(
              Http.newBuilder().setRequest(Request.newBuilder().setUserAgent("").build()).build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }
  }

  @Test
  public void enrichFromUserAgent() {
    String userAgent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

<<<<<<< HEAD
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(userAgent).build())
                        .setUserAgent(userAgent)
                        .build())
                .build());
=======
    Map<String, AttributeValue> attributeValueMap = e.getAttributes().getAttributeMap();
    attributeValueMap.put(RPC_USER_AGENT_HEADER, AttributeValueCreator.create(header));

    enricher.enrichEvent(null, e);

    ReadableUserAgent expected = UADetectorServiceFactory.getResourceModuleParser().parse(header);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(map.get(Constants.getEnrichedSpanConstant(
        org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_NAME)).getValue(),
        expected.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_TYPE)).getValue(),
        expected.getType().getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_DEVICE_CATEGORY)).getValue(),
        expected.getDeviceCategory().getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_NAME)).getValue(),
        expected.getOperatingSystem().getName());
  }

  @Test
  public void enrichFromRpcUserAgent() {
    Event e = createMockEvent();

    String header = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)" +
        " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Map<String, AttributeValue> attributeValueMap = e.getAttributes().getAttributeMap();
    attributeValueMap.put(USER_AGENT_HEADER, AttributeValueCreator.create(header));
>>>>>>> Use named fields for enriching grpc info

    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(6, map.size());
    assertEquals(
        "Chrome", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "Browser",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_TYPE)).getValue());
    assertEquals(
        "Personal computer",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "OS X",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.14.3",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "73.0.3683.103",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void badOS() {
    String userAgent =
        "Mozilla/5.0"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(userAgent).build())
                        .setUserAgent(userAgent)
                        .build())
                .build());

    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(6, map.size());
    assertEquals(
        "Chrome", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "Browser",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_TYPE)).getValue());
    assertEquals(
        "Personal computer",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "73.0.3683.103",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void badBrowser() {
    String userAgent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chroma/73.0.3683.103";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(userAgent).build())
                        .setUserAgent(userAgent)
                        .build())
                .build());
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(6, map.size());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "OS X",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.14.3",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void badUserAgent() {
    String badUserAgent = "I am not a User Agent!";
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(badUserAgent).build())
                        .setUserAgent(badUserAgent)
                        .build())
                .build());
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(6, map.size());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void should_preferUserAgent_fromHeaders() {
    String userAgent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(userAgent).build())
                        .setUserAgent("I am not a User Agent!")
                        .build())
                .build());
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(6, map.size());
    assertEquals(6, map.size());
    assertEquals(
        "Chrome", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "Browser",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_TYPE)).getValue());
    assertEquals(
        "Personal computer",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "OS X",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.14.3",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "73.0.3683.103",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void should_fallbackToUserAgent_fromRequest() {
    String userAgent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setUserAgent(userAgent)
                        .build())
                .build());
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(6, map.size());
    assertEquals(6, map.size());
    assertEquals(
        "Chrome", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "Browser",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_TYPE)).getValue());
    assertEquals(
        "Personal computer",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "OS X",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.14.3",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "73.0.3683.103",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }
}
