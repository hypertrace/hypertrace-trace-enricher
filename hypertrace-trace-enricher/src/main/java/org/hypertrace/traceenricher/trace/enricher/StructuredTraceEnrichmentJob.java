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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.serdes.SchemaRegistryBasedAvroSerde;
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredTraceEnrichmentJob extends KafkaStreamsApp {
  private static final Logger logger = LoggerFactory.getLogger(StructuredTraceEnrichmentJob.class);
  private Map<String, String> schemaRegistryConfig;

  protected StructuredTraceEnrichmentJob(Config jobConfig) {
    super(jobConfig);
  }

  @Override
  protected StreamsBuilder buildTopology(Properties properties, StreamsBuilder streamsBuilder) {
    SchemaRegistryBasedAvroSerde<StructuredTrace> traceSerde = new SchemaRegistryBasedAvroSerde<>(StructuredTrace.class);
    traceSerde.configure(schemaRegistryConfig, false);

    String inputTopic = properties.getProperty(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = properties.getProperty(OUTPUT_TOPIC_CONFIG_KEY);

    streamsBuilder
        .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.serdeFrom(traceSerde, traceSerde)))
        .transform(StructuredTraceEnrichProcessor::new)
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.serdeFrom(traceSerde, traceSerde)));

    return streamsBuilder;
  }

  @Override
  protected Properties getStreamsConfig(Config config) {
    Properties properties = new Properties();

    schemaRegistryConfig = ConfigUtils.getFlatMapConfig(config, SCHEMA_REGISTRY_CONFIG_KEY);
    properties.putAll(schemaRegistryConfig);

    properties.put(INPUT_TOPIC_CONFIG_KEY, config.getString(INPUT_TOPIC_CONFIG_KEY));
    properties.put(OUTPUT_TOPIC_CONFIG_KEY, config.getString(OUTPUT_TOPIC_CONFIG_KEY));
    properties.putAll(ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));

    List<String> enrichers = config.getStringList(ENRICHER_NAMES_CONFIG_KEY);

    Map<String, Config> enricherConfigs = new LinkedHashMap<>();
    for (String enricher : enrichers) {
      Config enricherConfig = config.getConfig(getEnricherConfigPath(enricher));
      enricherConfigs.put(enricher, enricherConfig);
    }

    properties.put(ENRICHER_CONFIGS_KEY, enricherConfigs);

    properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UseWallclockTimeOnInvalidTimestamp.class);
    properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

    properties.put(JOB_CONFIG, config);

    return properties;
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  private String getEnricherConfigPath(String enricher) {
    return String.format(ENRICHER_CONFIG_TEMPLATE, enricher);
  }
}
