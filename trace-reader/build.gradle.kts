plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.4")
  implementation("org.hypertrace.core.attribute.service:attribute-service-api:0.4.3")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.4.3")
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.4.3")
  implementation("io.reactivex.rxjava3:rxjava:3.0.6")
}