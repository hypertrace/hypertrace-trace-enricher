plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.4")
  implementation("org.hypertrace.core.attribute.service:attribute-service-api:0.4.3")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.4.3")
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.4.3")
  implementation("org.hypertrace.entity.service:entity-service-api:0.1.26-SNAPSHOT")
  implementation("org.hypertrace.core.grpcutils:grpc-client-rx-utils:0.2.0")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.0")
  implementation("io.reactivex.rxjava3:rxjava:3.0.6")
  implementation("com.google.guava:guava:29.0-jre")
}