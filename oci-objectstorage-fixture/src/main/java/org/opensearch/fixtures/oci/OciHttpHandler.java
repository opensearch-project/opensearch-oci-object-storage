package org.opensearch.fixtures.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.base.Preconditions;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.model.Bucket;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.model.ListObjects;
import com.oracle.bmc.objectstorage.model.ObjectSummary;
import com.oracle.bmc.util.internal.StringUtils;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import lombok.extern.log4j.Log4j2;
import org.opensearch.common.bytes.BytesReference;

import org.opensearch.common.io.Streams;
import org.opensearch.common.regex.Regex;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.RestUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

@Log4j2
public class OciHttpHandler implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        // Prevent exceptions from being thrown for unknown properties
        MAPPER.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(FAIL_ON_IGNORED_PROPERTIES, false);
        MAPPER.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));
    }

    private final ConcurrentHashMap<String, LocalBucket> buckets = new ConcurrentHashMap<>();

    public OciHttpHandler() {
        log.info("Initializing OciHttpHandler");
    }

    @Override
    public void handle(HttpExchange exchange) {
        final String path = exchange.getRequestURI().getPath();
        final String opcRequestId = exchange.getRequestHeaders().getFirst("Opc-request-id");
        final String requestMethod = exchange.getRequestMethod();
        log.debug("request received in fixture with method: {}, path: {}, opcRequestId: {}", requestMethod, path, opcRequestId);
        final String[] pathParams = path.split("/");
        try {
            if (path.equals("/n/testResource") && exchange.getRequestMethod().equals("GET")) {
                // Test resource
                final byte[] response = getIt().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
            } else if (Regex.simpleMatch("/n/*/b", path)
                    && exchange.getRequestMethod().equals("POST")) {
                // create bucket
                final String namespace = pathParams[2];
                final CreateBucketDetails createBucketDetails =
                        MAPPER.readValue(exchange.getRequestBody(), CreateBucketDetails.class);
                createBucket(namespace, createBucketDetails, exchange);
            } else if (Regex.simpleMatch("/n/*/b/*/o/*", path)
                    && exchange.getRequestMethod().equals("PUT")) {
                // PUT object
                final String namespace = pathParams[2];
                final String bucket = pathParams[4];
                final String objectName =
                        String.join("/", Arrays.copyOfRange(pathParams, 6, pathParams.length));
                final int contentLength =
                        Integer.parseInt(exchange.getRequestHeaders().getFirst("Content-Length"));
                putObject(namespace, bucket, objectName, contentLength, exchange);
            } else if (Regex.simpleMatch("/n/*/b/*/o/*", path)
                    && exchange.getRequestMethod().equals("GET")) {
                // GET object
                final String namespace = pathParams[2];
                final String bucket = pathParams[4];
                final String objectName =
                        String.join("/", Arrays.copyOfRange(pathParams, 6, pathParams.length));
                final String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                final Range range;
                if (rangeHeader != null) {
                    range = parseRangeValue(rangeHeader);
                } else {
                    range = null;
                }

                getObject(namespace, bucket, objectName, range, exchange);
            } else if (Regex.simpleMatch("/n/*/b/*/o/*", path)
                    && exchange.getRequestMethod().equals("HEAD")) {
                // HEAD object
                final String namespace = pathParams[2];
                final String bucket = pathParams[4];
                final String objectName =
                        String.join("/", Arrays.copyOfRange(pathParams, 6, pathParams.length));
                headObject(namespace, bucket, objectName, exchange);
            } else if (Regex.simpleMatch("/n/*/b/*/o/*", path)
                    && exchange.getRequestMethod().equals("DELETE")) {
                // DELETE object
                final String namespace = pathParams[2];
                final String bucket = pathParams[4];
                final String objectName =
                        String.join("/", Arrays.copyOfRange(pathParams, 6, pathParams.length));
                deleteObject(namespace, bucket, objectName, exchange);
            } else if (Regex.simpleMatch("/n/*/b/*/o", path)
                    && exchange.getRequestMethod().equals("GET")) {
                //  List objects
                final String namespace = pathParams[2];
                final String bucket = pathParams[4];
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                listObject(namespace, bucket, params.get("prefix"), exchange);
            } else if (Regex.simpleMatch("/n/*/b/*", path)
                    && exchange.getRequestMethod().equals("GET")) {
                // GET bucket
                final String namespace = pathParams[2];
                final String bucket = pathParams[4];
                getBucket(namespace, bucket, exchange);
            } else {
                sendError(
                        exchange, RestStatus.METHOD_NOT_ALLOWED, "400", "Method not allowed/found");
            }
        } catch (Exception e) {
            log.error("Exception found when processing request", e);
        }
    }

    public static void sendError(
            final HttpExchange exchange,
            final RestStatus status,
            final String errorCode,
            final String message)
            throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/xml");

        final String requestId = exchange.getRequestHeaders().getFirst("x-amz-request-id");
        if (requestId != null) {
            headers.add("x-amz-request-id", requestId);
        }

        if (errorCode == null || "HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status.getStatus(), -1L);
            exchange.close();
        } else {
            final byte[] response =
                    ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>"
                                    + "<Code>"
                                    + errorCode
                                    + "</Code>"
                                    + "<Message>"
                                    + message
                                    + "</Message>"
                                    + "<RequestId>"
                                    + requestId
                                    + "</RequestId>"
                                    + "</Error>")
                            .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.getStatus(), response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        }
    }

    private String getIt() {
        return "Got it!";
    }

    private void createBucket(
            String namespaceName, CreateBucketDetails createBucketDetails, HttpExchange exchange)
            throws IOException {
        log.debug(
                "CreateBucket request with namespaceName: {}, createBucketDetails: {}",
                namespaceName,
                createBucketDetails);
        final String bucketName = createBucketDetails.getName();
        if (buckets.containsKey(bucketName)) {
            throw new RuntimeException("Bucket already exists");
        }
        final LocalBucket localBucket = new LocalBucket(bucketName);
        buckets.put(bucketName, localBucket);

        final Bucket bucket = Bucket.builder().name(bucketName).build();
        final String bucketStr = MAPPER.writeValueAsString(bucket);
        final byte[] response = bucketStr.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private void getBucket(String namespace, String bucketName, HttpExchange exchange)
            throws IOException {
        log.debug("getBucket with namespace: {}, bucketName: {}", namespace, bucketName);
        final LocalBucket localBucket = buckets.get(bucketName);
        if (localBucket != null) {
            final Bucket bucket = Bucket.builder().name(bucketName).build();
            final String bucketStr = MAPPER.writeValueAsString(bucket);
            final byte[] response = bucketStr.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
            exchange.getResponseBody().write(response);
        } else {
            sendError(exchange, RestStatus.NOT_FOUND, "404", "Bucket not found");
        }

        exchange.close();
    }

    private void putObject(
            String namespaceName,
            String bucketName,
            String objectName,
            int contentLength,
            HttpExchange exchange)
            throws IOException {
        final BytesReference requestBody = Streams.readFully(exchange.getRequestBody());
        Preconditions.checkArgument(buckets.containsKey(bucketName), "Bucket doesn't exist");
        final LocalBucket bucket = buckets.get(bucketName);
        bucket.putObject(
                objectName,
                new ByteArrayInputStream(requestBody.toBytesRef().bytes),
                contentLength);

        log.info(
                "Put object with namespaceName:{}, bucketName: {}, objectName: {}",
                namespaceName,
                bucketName,
                objectName);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
        exchange.close();
    }

    private void getObject(
            String namespaceName, String bucketName, String objectName, Range range, HttpExchange exchange)
            throws IOException {
        log.info(
                "Get object with namespaceName:{}, bucketName: {}, objectName: {}",
                namespaceName,
                bucketName,
                objectName);
        Preconditions.checkArgument(buckets.containsKey(bucketName), "Bucket doesn't exist");
        final LocalBucket bucket = buckets.get(bucketName);
        final OSObject object = bucket.getObject(objectName);
        if (object != null) {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
            if (range != null) {
                exchange.getResponseBody()
                        .write(Arrays.copyOfRange(object.getBytes(),
                                range.getStartByte().intValue(), range.getEndByte().intValue() + 1));
            } else {
                exchange.getResponseBody().write(object.getBytes());
            }

            exchange.close();
        } else {
            sendError(exchange, RestStatus.NOT_FOUND, "404", "Object not found");
        }
    }

    private void headObject(
            String namespaceName, String bucketName, String objectName, HttpExchange exchange)
            throws IOException {
        log.info(
                "Get object with namespaceName:{}, bucketName: {}, objectName: {}",
                namespaceName,
                bucketName,
                objectName);
        Preconditions.checkArgument(buckets.containsKey(bucketName), "Bucket doesn't exist");
        final LocalBucket bucket = buckets.get(bucketName);
        final OSObject object = bucket.getObject(objectName);
        if (object != null) {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
            exchange.getResponseBody().write(object.getBytes());
            exchange.close();
        } else {
            sendError(exchange, RestStatus.NOT_FOUND, "404", "Object not found");
        }
    }

    private void listObject(
            String namespace, String bucketName, String prefix, HttpExchange exchange)
            throws IOException {
        log.info(
                "list objects with namespaceName:{}, bucketName: {}, prefix: {}",
                namespace,
                bucketName,
                prefix);
        Preconditions.checkArgument(buckets.containsKey(bucketName), "Bucket doesn't exist");
        final LocalBucket bucket = buckets.get(bucketName);
        final List<OSObject> results = bucket.getObjectByPrefix(prefix);
        final ListObjects listObjects =
                ListObjects.builder()
                        .objects(
                                results.stream()
                                        .map(
                                                osObject ->
                                                        ObjectSummary.builder()
                                                                .name(osObject.getPrefix())
                                                                .build())
                                        .collect(Collectors.toList()))
                        .build();

        final String str = MAPPER.writeValueAsString(listObjects);
        final byte[] response = str.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private void deleteObject(
            String namespaceName, String bucketName, String objectName, HttpExchange exchange)
            throws IOException {
        log.info(
                "Delete object with namespaceName:{}, bucketName: {}, objectName: {}",
                namespaceName,
                bucketName,
                objectName);
        Preconditions.checkArgument(buckets.containsKey(bucketName), "Bucket doesn't exist");
        final LocalBucket bucket = buckets.get(bucketName);
        Preconditions.checkArgument(
                bucket.getPrefixToObjectMap().containsKey(objectName), "Object doesn't exist");

        bucket.deleteObject(objectName);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
        exchange.close();
    }

    // Original Range parser from OCI is incompatible with it's own actual "toString" serialization method
    // therefore this is hack is needed.
    private static Range parseRangeValue(String value) {
        log.debug("Attempting to parse range: {}", value);
        value = value.replace("bytes", "").replace("=", "").trim();
        String[] byteValues = value.split("-", -1); // include trailing empty strings
        if (byteValues.length != 2) {
            throw new IllegalArgumentException(
                    "Must provide <start>-<end> format for range request: " + value);
        }
        Long startByte = StringUtils.isBlank(byteValues[0]) ? null : Long.parseLong(byteValues[0]);
        Long endByte = StringUtils.isBlank(byteValues[1]) ? null : Long.parseLong(byteValues[1]);
        if (startByte == null && endByte == null) {
            throw new IllegalArgumentException(
                    "Must provide start/end byte for range request: " + value);
        }
        return new Range(startByte, endByte);
    }
}
