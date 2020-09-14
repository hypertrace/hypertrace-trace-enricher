package org.hypertrace.traceenricher.trace.enricher;

import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_CONFIGS_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_CONFIG_TEMPLATE;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_NAMES_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.JOB_CONFIG;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY;

import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceEnricher extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(TraceEnricher.class);

  public TraceEnricher(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> properties, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    String inputTopic = getAppConfig().getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = getAppConfig().getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<String, StructuredTrace> inputStream = (KStream<String, StructuredTrace>) inputStreams
        .get(inputTopic);
    if (inputStream == null) {
      inputStream = streamsBuilder
          .stream(inputTopic, Consumed.with(Serdes.String(), null));
      inputStreams.put(inputTopic, inputStream);
    }

    inputStream
        .transform(StructuredTraceEnrichProcessor::new)
        .to(outputTopic, Produced.keySerde(Serdes.String()));

    return streamsBuilder;
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config config) {
    Map<String, Object> properties = new HashMap<>();

    properties.putAll(ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));

    List<String> enrichers = config.getStringList(ENRICHER_NAMES_CONFIG_KEY);

    Map<String, Config> enricherConfigs = new LinkedHashMap<>();
    for (String enricher : enrichers) {
      Config enricherConfig = config.getConfig(getEnricherConfigPath(enricher));
      enricherConfigs.put(enricher, enricherConfig);
    }

    properties.put(ENRICHER_CONFIGS_KEY, enricherConfigs);
    properties.put(JOB_CONFIG, config);

    return properties;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public List<String> getInputTopics() {
    return Arrays.asList(getAppConfig().getString(INPUT_TOPIC_CONFIG_KEY));
  }

  @Override
  public List<String> getOutputTopics() {
    return Arrays.asList(getAppConfig().getString(OUTPUT_TOPIC_CONFIG_KEY));
  }

  private String getEnricherConfigPath(String enricher) {
    return String.format(ENRICHER_CONFIG_TEMPLATE, enricher);
  }
}
