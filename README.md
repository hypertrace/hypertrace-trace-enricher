# Hypertrace Trace Enricher
###### org.hypertrace.traceenricher

[![CircleCI](https://circleci.com/gh/hypertrace/hypertrace-trace-enricher.svg?style=svg)](https://circleci.com/gh/hypertrace/hypertrace-trace-enricher)

A streaming job that enriches the incoming spans with identified entities like Endpoint, Service, Backend, etc.

## trace-enrichers

We have different enrichers in the pipeline and all those enrichers are executed as a DAG. The list of enrichers Hypertrace uses is as follows:
- `SpanTypeAttributeEnricher`
- `ApiStatusEnricher`
- `EndpointEnricher`
- `TransactionNameEnricher`
- `ApiBoundaryTypeAttributeEnricher`
- `ErrorsAndExceptionsEnricher`
- `BackendEntityEnricher`
- `HttpAttributeEnricher`
- `DefaultServiceEntityEnricher` 
- `UserAgentSpanEnricher`

## Description

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/ingestion-pipeline.png) | 
|:--:| 
| *Hypertrace Ingestion Pipeline* |

trace-enrichers are being used to enrich spans/traces with entity information. `hypertrace-trace-enricher` service talks to `entity-service` which fetches entity information from Mongo as required. 

For example, Let's say we got span which has http method related attribute `method: /api/v1/user?name`. So, in this case, if we already have Endpoint entity which refers to `/api/v1/user`, we fetch the id of that entity and add it to span. Now, span will have one more attribute like this `method:/api/v1/user?name, api_id:1234`.

## Building locally
`hypertrace-trace-enricher` uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build `hypertrace-trace-enricher`, run:

```
./gradlew dockerBuildImages
```

## Testing

### Running unit tests
Run `./gradlew test` to execute unit tests. 


### Testing image

You can test the image you built after modification by running docker-compose or helm setup. 

#### docker-compose
Change the tag for `hypertrace-trace-enricher ` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  hypertrace-trace-enricher:
    image: hypertrace/hypertrace-trace-enricher:test
    container_name: hypertrace-trace-enricher
    ...
```
and then run `docker-compose up` to test the setup.

## Docker Image Source:
- [DockerHub > Hypertrace trace enricher](https://hub.docker.com/r/hypertrace/hypertrace-trace-enricher)
