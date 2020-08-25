plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.4")
  implementation("org.hypertrace.core.attribute.service:attribute-service-api:0.3.0-SNAPSHOT")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.3.0-SNAPSHOT")
  implementation("org.hypertrace.core.attribute.service:attribute-projections:0.3.0-SNAPSHOT")
  implementation("io.reactivex.rxjava3:rxjava:3.0.5")
}