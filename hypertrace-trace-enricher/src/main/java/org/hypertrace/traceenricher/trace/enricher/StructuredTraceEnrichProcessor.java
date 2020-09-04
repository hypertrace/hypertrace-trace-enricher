package org.hypertrace.traceenricher.trace.enricher;

import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_CONFIGS_KEY;

import com.typesafe.config.Config;
import java.util.Map;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.client.DefaultEdsClientProvider;
import org.hypertrace.traceenricher.enrichment.EnrichmentProcessor;
import org.hypertrace.traceenricher.enrichment.EnrichmentRegistry;

public class StructuredTraceEnrichProcessor implements
    Transformer<String, StructuredTrace, KeyValue<String, StructuredTrace>> {

  private EnrichmentProcessor processor;

  @Override
  public void init(ProcessorContext context) {
    Map<String, Config> enricherConfigs = (Map<String, Config>) context.appConfigs()
        .get(ENRICHER_CONFIGS_KEY);
    EnrichmentRegistry enrichmentRegistry = new EnrichmentRegistry();
    enrichmentRegistry.registerEnrichers(enricherConfigs);

    processor = new EnrichmentProcessor(enrichmentRegistry.getOrderedRegisteredEnrichers(),
        new DefaultEdsClientProvider());
  }

  @Override
  public KeyValue<String, StructuredTrace> transform(String key, StructuredTrace value) {
    processor.process(value);
    return new KeyValue<>(null, value);
  }

  @Override
  public void close() {
  }
}
