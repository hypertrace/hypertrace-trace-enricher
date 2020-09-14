package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UserAgentSpanEnricherTest extends AbstractAttributeEnricherTest {

  private UserAgentSpanEnricher enricher;

  @BeforeEach
  public void setUp() {
    enricher = new UserAgentSpanEnricher();
  }

  @Test
  public void noUserAgent() {
    {
      Event e = createMockEvent();
      when(e.getHttp()).thenReturn(null);
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }

    {
      Event e = createMockEvent();
      when(e.getHttp()).thenReturn(Http.newBuilder().build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }

    {
      Event e = createMockEvent();
      when(e.getHttp())
          .thenReturn(Http.newBuilder().setRequest(Request.newBuilder().build()).build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
    }

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

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(Request.newBuilder().setUserAgent(userAgent).build())
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
                .setRequest(Request.newBuilder().setUserAgent(userAgent).build())
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
                .setRequest(Request.newBuilder().setUserAgent(userAgent).build())
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
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(Request.newBuilder().setUserAgent("I am not a User Agent!").build())
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
}
