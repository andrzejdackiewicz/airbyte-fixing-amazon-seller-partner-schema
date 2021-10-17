/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.elasticsearch;

import co.elastic.clients.base.*;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._core.BulkRequest;
import co.elastic.clients.elasticsearch._core.BulkResponse;
import co.elastic.clients.elasticsearch._core.CreateResponse;
import co.elastic.clients.elasticsearch._core.SearchResponse;
import co.elastic.clients.elasticsearch._core.bulk.Operation;
import co.elastic.clients.elasticsearch._core.search.Hit;
import co.elastic.clients.elasticsearch._core.search.HitsMetadata;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import jakarta.json.JsonValue;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * All communication with Elasticsearch should be done through this class.
 */
public class ElasticsearchConnection {

    // this is the max number of hits we can query without paging
    private static final int MAX_HITS = 10000;
    private static Logger log = LoggerFactory.getLogger(ElasticsearchConnection.class);

    private final String tmpIndex = "test_airbyte";
    private final ObjectMapper mapper = new ObjectMapper();
    private final ElasticsearchClient client;
    private final HttpHost httpHost;

    /**
     * Creates a new ElasticsearchConnection that can be used to read/write records to indices
     *
     * @param config Configuration parameters for connecting to the Elasticsearch host
     */
    public ElasticsearchConnection(ConnectorConfiguration config) {
        log.info(String.format(
                "creating ElasticsearchConnection: %s:%s", config.getHost(), config.getPort()));

        // Create the low-level client
        httpHost = new HttpHost(config.getHost(), config.getPort());
        RestClient restClient = RestClient.builder(httpHost)
                .setDefaultHeaders(configureHeaders(config))
                .setFailureListener(new failureListener())
                .build();
        // Create the transport that provides JSON and http services to API clients
        Transport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        // And create our API client
        client = new ElasticsearchClient(transport);
    }

    static class failureListener extends RestClient.FailureListener {
        @Override
        public void onFailure(Node node) {
            log.error("RestClient failure: {}", node);
        }
    }

    /**
     * Configures the default headers for requests to the Elasticsearch server
     *
     * @param config connection information
     * @return the default headers
     */
    private Header[] configureHeaders(ConnectorConfiguration config) {
        var headerList = new ArrayList<Header>();
        // add Authorization header if credentials are present
        if (Objects.nonNull(config.getApiKeyId()) && Objects.nonNull(config.getApiKeySecret())) {
            var bytes = (config.getApiKeyId() + ":" + config.getApiKeySecret()).getBytes(StandardCharsets.UTF_8);
            var header = "ApiKey " + Base64.getEncoder().encodeToString(bytes);
            headerList.add(new BasicHeader("Authorization", header));
        }
        return headerList.toArray(new Header[headerList.size()]);
    }

    /**
     * Pings the Elasticsearch server for "up" check, and configuration validation
     *
     * @return true if connection was successful
     */
    public boolean ping() {
        try {
            return client.ping().value();
        } catch (IOException e) {
            log.error("failed to ping elasticsearch server", e);
            return false;
        }
    }

    /**
     * Writes a single record to the Elasticsearch server
     *
     * @param index The index to write the record to
     * @param id    The ID to give the new document
     * @param data  The body of the document (source record)
     * @return results of the create operation
     * @throws Exception if an error is encountered
     */
    public CreateResponse createDocument(String index, String id, JsonNode data) throws Exception {
        CreateResponse createResponse = client.create(builder ->
                builder.id(id).document(data).index(index));
        log.debug("wrote record: {}", createResponse.result());
        return createResponse;
    }

    /**
     * Bulk operation to append multiple documents to an Elasticsearch server
     *
     * @param index   The index to add the documents to
     * @param records The collection of records to create documents from
     * @return the response of the bulk operation
     * @throws IOException if there is server connection problem, or a non-successful operation on the server
     */
    public BulkResponse createDocuments(String index, List<AirbyteRecordMessage> records, ElasticsearchWriteConfig config) throws IOException {
        var bulkRequest = new BulkRequest.Builder<>();
        for (var doc : records) {
            log.debug("adding record to bulk create: {}", doc.getData());
            bulkRequest.addOperation(
                    b -> b.index(
                            c -> c.index(index).id(extractPrimaryKey(doc, config))))
                    .addDocument(doc.getData()).refresh(JsonValue.TRUE);
        }

        try {
            return client.bulk(b -> bulkRequest);
        } catch (ApiException e) {
            throw unwrappedApiException("failed write operation", e);
        }
    }
    /**
     * Bulk operation to update multiple documents in an Elasticsearch server index
     * Attempts to extract a primary key value from the record
     *
     * @param index   The index to add the documents to
     * @param records The collection of records to update documents from
     * @param config write configuration for this stream
     * @return the response of the bulk operation
     * @throws IOException if there is server connection problem, or a non-successful operation on the server
     */
    public BulkResponse updateDocuments(String index, List<AirbyteRecordMessage> records, ElasticsearchWriteConfig config) throws IOException {
        var bulkRequest = new BulkRequest.Builder<>();
        for (var doc : records) {
            log.debug("adding record to bulk create: {}", doc.getData());
            bulkRequest.addOperation(
                    b -> b.update(
                            d -> d.index(index).id(extractPrimaryKey(doc, config))))
                    .addDocument(jsonNodeToDocUpdateObject(doc.getData()));
        }

        try {
            return client.bulk(b -> bulkRequest);
        } catch (ApiException e) {
            throw unwrappedApiException("failed write operation", e);
        }
    }

    private JsonNode jsonNodeToDocUpdateObject(JsonNode data) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("doc", data);
        return mapper.valueToTree(doc);
    }

    // TODO: Can we do something like this?
    private String extractPrimaryKey(AirbyteRecordMessage doc, ElasticsearchWriteConfig config) {
        if (!config.hasPrimaryKey()) {
            return UUID.randomUUID().toString();
        }
        var optFirst = config.getPrimaryKey().stream().findFirst();
        StringBuilder sb = new StringBuilder();
        if (optFirst.isPresent()) {
            log.debug("trying to extract primary key using {}", optFirst.get());
            optFirst.get().stream().forEach(s -> {
                sb.append(String.format("%s.", s));
            });
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length()-1);
            }
        }
        if (sb.length() > 0) {
            JsonPointer ptr = JsonPointer.valueOf(sb.toString());
            var pkNode = doc.getData().at(ptr);
            if (!pkNode.isMissingNode() && pkNode.isValueNode()) {
                return pkNode.asText();
            }
        }
        log.warn("unable to extract primary key");
        return UUID.randomUUID().toString();
    }

    /**
     * returns the first 10k documents of a given index
     *
     * @param index the index to search
     * @return a list of matching documents
     * @throws IOException if there is server communication error, or invalid index
     */
    public List<JsonNode> getRecords(String index) throws IOException {
        log.info("getting records for index: {}", index);
        SearchResponse<JsonNode> search = client.search(s -> s.index(index).size(MAX_HITS), JsonNode.class);
        HitsMetadata<JsonNode> hitMeta = search.hits();
        return hitMeta.hits().stream().map(Hit::source).collect(Collectors.toList());
    }

    /**
     * Shutdown the connection to the Elasticsearch server
     */
    public void close() {
        this.client.shutdown();
    }

    /**
     * Creates an index on Elasticsearch if it's missing
     *
     * @param index the index name to create
     * @throws IOException if there is communication error, or if the index fails to create
     */
    public void createIndexIfMissing(String index) throws IOException {
        try {
            BooleanResponse existsResponse = client.indices().exists(b -> b.index(index));
            if (existsResponse.value()) {
                log.info("index exists: {}", index);
                return;
            }
            log.info("creating index: {}, info: {}", index, client.info());
            final co.elastic.clients.elasticsearch.indices.CreateResponse createResponse = client.indices().create(b -> b.index(index));
            if (createResponse.acknowledged() && createResponse.shardsAcknowledged()) {
                log.info("created index: {}", index);
            } else {
                log.info("did not create index: {}, {}", index, createResponse);
            }
        } catch (IOException e) {
            log.error("failed to create index: {}", e.getMessage());
            throw e;
        }
    }

    public void deleteIndex(String indexName) throws Exception {
        try {
            client.delete(b -> b.id("*").index(indexName));
        } catch (ApiException e) {
            throw unwrappedApiException("failed to delete index", e);
        }
    }

    private RuntimeException unwrappedApiException(String message, ApiException e) {
        log.error(message);
        if (Objects.isNull(e) || Objects.isNull(e.error())) {
            log.error("unknown ApiException");
            return new RuntimeException(e);
        }
        if (ElasticsearchError.class.isAssignableFrom(e.error().getClass())) {
            ElasticsearchError esException = ((ElasticsearchError) e.error());
            String errorMessage = String.format("ElasticsearchError: status:%s, error:%s", esException.status(), esException.error().toString());
            return new RuntimeException(errorMessage);
        }
        return new RuntimeException(e);
    }

}
