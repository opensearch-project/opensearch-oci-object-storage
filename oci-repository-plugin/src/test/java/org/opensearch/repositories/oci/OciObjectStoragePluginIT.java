/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.oci;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Node;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.fixtures.oci.NonJerseyServer;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest.ALL_SNAPSHOTS;

public class OciObjectStoragePluginIT extends OpenSearchRestTestCase {

    private static final String TEST_REPOSITORY_NAME = "myTestRepository";

    @Test
    public void testItAll() throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        final int testObjectStorageServerPort = 8081;

        try (NonJerseyServer nonJerseyServer = new NonJerseyServer(testObjectStorageServerPort)) {
            SocketAccess.doPrivilegedVoidIOException(nonJerseyServer::start);
            try (final RestHighLevelClient restHighLevelClient = getRestHighLevelClient()) {
                logger.info("1. Test cluster can load the plugin");
                // 1. Test cluster can load the plugin
                testExternalStatusShowsGreen(restHighLevelClient);

                logger.info("2. Repository creation");
                // 2. Repository creation
                testCreateRepository(restHighLevelClient, nonJerseyServer.getUrl());

                logger.info("3. Snapshot and restore testing");
                // 3. Snapshot and restore testing
                testSnapshotAndRestore(restHighLevelClient);

                logger.info("4. Delete snapshot from repository");
                // 4. Delete snapshot from repository
                testDeleteSnapshotFromRepository(restHighLevelClient);

                SocketAccess.doPrivilegedVoidIOException(nonJerseyServer::close);
            }
        }
    }

    private void testCreateRepository(RestHighLevelClient restHighLevelClient, String testObjectStorageUrl)
            throws IOException, NoSuchAlgorithmException {
        PutRepositoryRequest putRepositoryRequest = new PutRepositoryRequest();
        putRepositoryRequest.masterNodeTimeout(TimeValue.timeValueSeconds(10));
        putRepositoryRequest.settings(TestConstants.getRepositorySettings(testObjectStorageUrl));
        putRepositoryRequest.type("oci");
        putRepositoryRequest.name(TEST_REPOSITORY_NAME);
        final AcknowledgedResponse acknowledgedResponse =
                restHighLevelClient
                        .snapshot()
                        .createRepository(putRepositoryRequest, RequestOptions.DEFAULT);
        logger.info("Create repository response {}", acknowledgedResponse);
        final GetRepositoriesRequest getRepositoriesRequest = new GetRepositoriesRequest();
        getRepositoriesRequest.masterNodeTimeout(TimeValue.timeValueSeconds(10));
        final GetRepositoriesResponse getRepositoriesResponse =
                restHighLevelClient
                        .snapshot()
                        .getRepository(getRepositoriesRequest, RequestOptions.DEFAULT);
        Assertions.assertThat(getRepositoriesResponse.repositories().size()).isEqualTo(1);
        Assertions.assertThat(getRepositoriesResponse.repositories().get(0).name())
                .isEqualTo(TEST_REPOSITORY_NAME);
    }

    private void testExternalStatusShowsGreen(RestHighLevelClient restHighLevelClient)
            throws IOException {
        // 1. Test cluster can load the plugin
        logger.info("testing rest request");
        final ClusterHealthResponse clusterHealthResponse =
                restHighLevelClient
                        .cluster()
                        .health(
                                new ClusterHealthRequest().waitForGreenStatus(),
                                RequestOptions.DEFAULT);
        logger.info("rest response received {}", clusterHealthResponse);
        Assertions.assertThat(clusterHealthResponse.getStatus())
                .isEqualTo(ClusterHealthStatus.GREEN);
    }

    private void testSnapshotAndRestore(final RestHighLevelClient restClient)
            throws IOException, ExecutionException, InterruptedException {
        // Create 3 snapshots
        createPopulatedTestIndex("my_test_index1", restClient);
        testExternalStatusShowsGreen(restClient);
        snapshotCluster(TEST_REPOSITORY_NAME, "my_snapshot1", restClient);
        updateIndexWithNewDoc("my_test_index1", restClient, 1);
        createPopulatedTestIndex("my_test_index2", restClient);
        testExternalStatusShowsGreen(restClient);
        snapshotCluster(TEST_REPOSITORY_NAME, "my_snapshot2", restClient);
        updateIndexWithNewDoc("my_test_index1", restClient, 2);
        createPopulatedTestIndex("my_test_index3", restClient);
        testExternalStatusShowsGreen(restClient);
        snapshotCluster(TEST_REPOSITORY_NAME, "my_snapshot3", restClient);

        // Restore and test content of each snapshot
        cleanupAllIndices(restClient);
        restoreSnapshot(TEST_REPOSITORY_NAME, "my_snapshot1", restClient);
        searchIndex(restClient, "restored_snapshot_my_snapshot1" + "my_test_index1", 1);

        cleanupAllIndices(restClient);
        restoreSnapshot(TEST_REPOSITORY_NAME, "my_snapshot2", restClient);
        searchIndex(restClient, "restored_snapshot_my_snapshot2" + "my_test_index1", 2);
        searchIndex(restClient, "restored_snapshot_my_snapshot2" + "my_test_index2", 1);

        cleanupAllIndices(restClient);
        restoreSnapshot(TEST_REPOSITORY_NAME, "my_snapshot3", restClient);
        searchIndex(restClient, "restored_snapshot_my_snapshot3" + "my_test_index1", 3);
        searchIndex(restClient, "restored_snapshot_my_snapshot3" + "my_test_index2", 1);
        searchIndex(restClient, "restored_snapshot_my_snapshot3" + "my_test_index3", 1);
    }

    private void testDeleteSnapshotFromRepository(RestHighLevelClient restHighLevelClient) throws IOException {
        snapshotCluster(TEST_REPOSITORY_NAME, "my_temp_test_snapshot1", restHighLevelClient);
        final DeleteSnapshotRequest deleteSnapshotRequest = new DeleteSnapshotRequest();
        deleteSnapshotRequest.repository(TEST_REPOSITORY_NAME)
                .snapshots("my_temp_test_snapshot1");
        restHighLevelClient.snapshot().delete(deleteSnapshotRequest, RequestOptions.DEFAULT);

        final GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest();
        getSnapshotsRequest.repository(TEST_REPOSITORY_NAME);

        final GetSnapshotsResponse getSnapshotsResponse = restHighLevelClient.snapshot().get(getSnapshotsRequest, RequestOptions.DEFAULT);
        logger.info("get snapshots response: {}", getSnapshotsResponse);
        Assertions.assertThat(getSnapshotsResponse.getSnapshots().size()).isEqualTo(3);
    }

    private void snapshotCluster(
            String repositoryName, String snapshotName, RestHighLevelClient restClient) throws IOException {
        final CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest();
        try {
            createSnapshotRequest.snapshot(snapshotName);
            createSnapshotRequest.repository(repositoryName);
            createSnapshotRequest.indices(List.of(ALL_SNAPSHOTS));
            createSnapshotRequest.includeGlobalState(false);
            createSnapshotRequest.waitForCompletion(true);
            final CreateSnapshotResponse createSnapshotResponse =
                    restClient.snapshot().create(createSnapshotRequest, RequestOptions.DEFAULT);
            logger.info(
                    "Snapshot response for snapshot {}, response {}",
                    snapshotName,
                    createSnapshotResponse);
            Assertions.assertThat(createSnapshotResponse.getSnapshotInfo().state().name())
                    .isEqualTo("SUCCESS");
        } catch (OpenSearchStatusException e) {
            if (e.status().getStatus() == 404) {
                logger.info(
                        "Unable to snapshot repository {} since the repository cannot be found",
                        repositoryName);
            } else {
                logger.error("Unable to snapshot repository {}", repositoryName);
            }
            throw new RuntimeException(e);
        }
    }

    private void restoreSnapshot(
            String repositoryName,
            String snapshotName,
            RestHighLevelClient restClient)
            throws IOException {
        final RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest();
        restoreSnapshotRequest.repository(repositoryName).snapshot(snapshotName)
                .renamePattern("(.+)")
                .renameReplacement("restored_snapshot_" + snapshotName + "$1");

        RestoreSnapshotResponse restoreSnapshotResponse =
                restClient.snapshot().restore(restoreSnapshotRequest, RequestOptions.DEFAULT);

        logger.info("restore snapshot response: {}", restoreSnapshotResponse);
        testExternalStatusShowsGreen(restClient);
    }

    private void cleanupAllIndices(RestHighLevelClient restClient)
            throws IOException {
        final DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
        deleteIndexRequest.indices("*");
        restClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        testExternalStatusShowsGreen(restClient);
    }

    private SearchResponse searchIndex(
            RestHighLevelClient restHighLevelClient, String indexName, int expectedResults)
            throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(10000);
        searchRequest.source(searchSourceBuilder);
        final SearchResponse searchResponse =
                restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        logger.info("got search index response to test metrics {}", searchResponse);
        Assertions.assertThat(searchResponse.getHits().getTotalHits().value())
                .isEqualTo(expectedResults);
        return searchResponse;
    }

    private void createPopulatedTestIndex(String indexName, RestHighLevelClient restHighLevelClient)
            throws IOException, ExecutionException, InterruptedException {
        createTestIndex(indexName, restHighLevelClient);
        updateIndexWithNewDoc(indexName, restHighLevelClient, 0);
    }

    private void createTestIndex(String indexName, RestHighLevelClient restHighLevelClient)
            throws IOException {
        logger.info("Creating sample test index {}", indexName);

        final int indexReplicas = 0;
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", indexReplicas)
                    .build());

        restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    private void updateIndexWithNewDoc(String indexName, RestHighLevelClient restHighLevelClient, int docNum)
            throws IOException {
        final String fieldName = "test_field";
        final String docId = "test_doc_id_" + docNum;
        final String docContent = "{\"myField\": \"somet test information\"}";

        final UpdateRequest updateRequest = new UpdateRequest(indexName, docId);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            final InputStream stream =
                    new ByteArrayInputStream(docContent.getBytes(StandardCharsets.UTF_8));
            builder.rawField(fieldName, stream, XContentType.JSON);
            builder.endObject();
            updateRequest.doc(builder).upsert(builder);
        } catch (IOException e) {
            logger.error("Unable to create Manifest payload to ES", e);
            throw e;
        }

        restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        logger.info("Successfully persisted record with value: {}", docContent);
    }

    /**
     * Get the high level rest client for elastic search support
     *
     * @return RestHighLevelClient
     */
    public RestHighLevelClient getRestHighLevelClient() {
        RestClient restClient = client();
        final Node[] nodes = new Node[restClient.getNodes().size()];
        restClient.getNodes().toArray(nodes);
        final RestClientBuilder restClientBuilder = RestClient.builder(nodes);
        return new RestHighLevelClient(restClientBuilder);
    }
}
