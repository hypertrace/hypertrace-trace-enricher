package org.hypertrace.traceenricher.trace.enricher;

import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_CONFIGS_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_CONFIG_TEMPLATE;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_NAMES_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.JOB_CONFIG;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.SCHEMA_REGISTRY_CONFIG_KEY;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.serdes.SchemaRegistryBasedAvroSerde;
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceEnricher extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(TraceEnricher.class);
  private Map<String, String> schemaRegistryConfig;
  private String inputTopic;
  private String outputTopic;

  public TraceEnricher(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> streamsConfig, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    SchemaRegistryBasedAvroSerde<StructuredTrace> traceSerde = new SchemaRegistryBasedAvroSerde<>(
        StructuredTrace.class);
    traceSerde.configure(schemaRegistryConfig, false);

    inputTopic = (String) streamsConfig.get(INPUT_TOPIC_CONFIG_KEY);
    outputTopic = (String) streamsConfig.get(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<String, StructuredTrace> inputStream = (KStream<String, StructuredTrace>) inputStreams
        .get(inputTopic);
    if (inputStream == null) {
      inputStream = streamsBuilder
          .stream(inputTopic,
              Consumed.with(Serdes.String(), Serdes.serdeFrom(traceSerde, traceSerde)));
      inputStreams.put(inputTopic, inputStream);
    }

    inputStream
        .transform(StructuredTraceEnrichProcessor::new)
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.serdeFrom(traceSerde, traceSerde)));

    return streamsBuilder;
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config config) {
    Map<String, Object> streamsConfig = new HashMap<>();

    schemaRegistryConfig = ConfigUtils.getFlatMapConfig(config, SCHEMA_REGISTRY_CONFIG_KEY);
    streamsConfig.putAll(schemaRegistryConfig);

    streamsConfig.put(INPUT_TOPIC_CONFIG_KEY, config.getString(INPUT_TOPIC_CONFIG_KEY));
    streamsConfig.put(OUTPUT_TOPIC_CONFIG_KEY, config.getString(OUTPUT_TOPIC_CONFIG_KEY));
    streamsConfig.putAll(ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));

    List<String> enrichers = config.getStringList(ENRICHER_NAMES_CONFIG_KEY);

    Map<String, Config> enricherConfigs = new LinkedHashMap<>();
    for (String enricher : enrichers) {
      Config enricherConfig = config.getConfig(getEnricherConfigPath(enricher));
      enricherConfigs.put(enricher, enricherConfig);
    }

    streamsConfig.put(ENRICHER_CONFIGS_KEY, enricherConfigs);

    streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UseWallclockTimeOnInvalidTimestamp.class);
    streamsConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

    streamsConfig.put(JOB_CONFIG, config);

    return streamsConfig;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public List<String> getInputTopics() {
    return List.of(inputTopic);
  }

  @Override
  public List<String> getOutputTopics() {
    return List.of(outputTopic);
  }

  private String getEnricherConfigPath(String enricher) {
    return String.format(ENRICHER_CONFIG_TEMPLATE, enricher);
  }
}
