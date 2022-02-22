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

import lombok.extern.log4j.Log4j2;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.*;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.fixtures.oci.NonJerseyServer;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4Plugin;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest.ALL_SNAPSHOTS;

@Log4j2
public class OciObjectStoragePluginTest extends OpenSearchIntegTestCase {
    private static final String TEST_REPOSITORY_NAME = "myTestRepository";

    /** ** Enable the http client *** */
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.unmodifiableCollection(
                Lists.newArrayList(Netty4Plugin.class, OciObjectStoragePlugin.class));
    }
    /** ******************************* */
    @Test
    public void testServer() throws Exception {
        try (NonJerseyServer nonJerseyServer = new NonJerseyServer()) {
            SocketAccess.doPrivilegedVoidIOException(nonJerseyServer::start);
            // Test transport
            ensureGreen();
            final Client transportClient = client();
            final ClusterHealthResponse clusterHealthResponse =
                    transportClient.admin().cluster().prepareHealth().get();
            log.info("transport client health response {}", clusterHealthResponse);
            try (final RestHighLevelClient restHighLevelClient = getRestHighLevelClient()) {
                log.info("1. Test cluster can load the plugin");
                // 1. Test cluster can load the plugin
                testExternalStatusShowsGreen(restHighLevelClient);

                log.info("2. Repository creation");
                // 2. Repository creation
                testCreateRepository(restHighLevelClient);

                log.info("3. Snapshot and restore testing");
                // 3. Snapshot and restore testing
                testSnapshotAndRestore(transportClient, restHighLevelClient);

                log.info("4. Delete snapshot from repository");
                // 4. Delete snapshot from repository
                tempDeleteSnapshotFromRepository(transportClient);
            }
            SocketAccess.doPrivilegedVoidIOException(nonJerseyServer::close);
        }
    }

    private void tempDeleteSnapshotFromRepository(Client transportClient) {
        snapshotCluster(TEST_REPOSITORY_NAME, "my_temp_test_snapshot1", transportClient);
        transportClient
                .admin()
                .cluster()
                .prepareDeleteSnapshot(TEST_REPOSITORY_NAME, "my_temp_test_snapshot1")
                .get();
        GetSnapshotsResponse getSnapshotsResponse =
                transportClient.admin().cluster().prepareGetSnapshots(TEST_REPOSITORY_NAME).get();
        log.info("get snapshots response: {}", getSnapshotsResponse);
        Assertions.assertThat(getSnapshotsResponse.getSnapshots().size()).isEqualTo(3);
    }

    private void testSnapshotAndRestore(
            final Client esLocalClient, final RestHighLevelClient esRestClient)
            throws IOException, ExecutionException, InterruptedException {
        // Create 3 snapshots
        createPopulatedTestIndex("my_test_index1", esLocalClient);
        testExternalStatusShowsGreen(esRestClient);
        snapshotCluster(TEST_REPOSITORY_NAME, "my_snapshot1", esLocalClient);
        updateIndexWithNewDoc("my_test_index1", esLocalClient, 1);
        createPopulatedTestIndex("my_test_index2", esLocalClient);
        testExternalStatusShowsGreen(esRestClient);
        snapshotCluster(TEST_REPOSITORY_NAME, "my_snapshot2", esLocalClient);
        updateIndexWithNewDoc("my_test_index1", esLocalClient, 2);
        createPopulatedTestIndex("my_test_index3", esLocalClient);
        testExternalStatusShowsGreen(esRestClient);
        snapshotCluster(TEST_REPOSITORY_NAME, "my_snapshot3", esLocalClient);

        // Restore and test content of each snapshot
        cleanupAllIndices(esLocalClient, esRestClient);
        restoreSnapshot(TEST_REPOSITORY_NAME, "my_snapshot1", esLocalClient, esRestClient);
        searchIndex(esRestClient, "restored_snapshot_my_snapshot1" + "my_test_index1", 1);

        cleanupAllIndices(esLocalClient, esRestClient);
        restoreSnapshot(TEST_REPOSITORY_NAME, "my_snapshot2", esLocalClient, esRestClient);
        searchIndex(esRestClient, "restored_snapshot_my_snapshot2" + "my_test_index1", 2);
        searchIndex(esRestClient, "restored_snapshot_my_snapshot2" + "my_test_index2", 1);

        cleanupAllIndices(esLocalClient, esRestClient);
        restoreSnapshot(TEST_REPOSITORY_NAME, "my_snapshot3", esLocalClient, esRestClient);
        searchIndex(esRestClient, "restored_snapshot_my_snapshot3" + "my_test_index1", 3);
        searchIndex(esRestClient, "restored_snapshot_my_snapshot3" + "my_test_index2", 1);
        searchIndex(esRestClient, "restored_snapshot_my_snapshot3" + "my_test_index3", 1);
    }

    private void restoreSnapshot(
            String repositoryName,
            String snapshotName,
            Client esLocalClient,
            RestHighLevelClient esRestClient)
            throws IOException {
        final RestoreSnapshotResponse restoreSnapshotResponse =
                esLocalClient
                        .admin()
                        .cluster()
                        .prepareRestoreSnapshot(repositoryName, snapshotName)
                        .setRenamePattern("(.+)")
                        .setRenameReplacement("restored_snapshot_" + snapshotName + "$1")
                        .get();

        log.info("restore snapshot response: {}", restoreSnapshotResponse);
        testExternalStatusShowsGreen(esRestClient);
    }

    private void cleanupAllIndices(Client esLocalClient, RestHighLevelClient esRestClient)
            throws IOException {
        esLocalClient.admin().indices().prepareDelete("*").get();
        testExternalStatusShowsGreen(esRestClient);
    }

    private void snapshotCluster(
            String repositoryName, String snapshotName, Client transportClient) {
        final CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest();
        try {
            createSnapshotRequest.snapshot(snapshotName);
            createSnapshotRequest.repository(repositoryName);
            createSnapshotRequest.indices(Arrays.asList(ALL_SNAPSHOTS));
            createSnapshotRequest.includeGlobalState(false);
            createSnapshotRequest.waitForCompletion(true);
            final CreateSnapshotResponse createSnapshotResponse =
                    transportClient
                            .admin()
                            .cluster()
                            .createSnapshot(createSnapshotRequest)
                            .actionGet();
            log.info(
                    "Snapshot response for snapshot {}, response {}",
                    snapshotName,
                    createSnapshotResponse);
            Assertions.assertThat(createSnapshotResponse.getSnapshotInfo().state().name())
                    .isEqualTo("SUCCESS");
        } catch (OpenSearchStatusException e) {
            if (e.status().getStatus() == 404) {
                log.info(
                        "Unable to snapshot repository {} since the repository cannot be found",
                        repositoryName);
            } else {
                log.error("Unable to snapshot repository {}", repositoryName);
            }
            throw new RuntimeException(e);
        }
    }

    private void createPopulatedTestIndex(String indexName, Client transportClient)
            throws IOException, ExecutionException, InterruptedException {
        createTestIndex(indexName, transportClient);
        updateIndexWithNewDoc(indexName, transportClient, 0);
    }

    private void createTestIndex(String indexName, Client transportClient)
            throws ExecutionException, InterruptedException, IOException {
        log.info("Creating sample test index {}", indexName);
        final ClusterStatsResponse clusterStatsResponse =
                transportClient.admin().cluster().prepareClusterStats().execute().get();
        final int totalNodes = clusterStatsResponse.getNodesStats().getCounts().getTotal();
        // We would create the index with 1 replica depending on what is the number of nodes in
        // the cluster

        final int indexReplicas = totalNodes > 1 ? 1 : 0;
        transportClient
                .admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                        Settings.builder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", indexReplicas)
                                .build())
                .execute()
                .actionGet();
    }

    private void updateIndexWithNewDoc(String indexName, Client transportClient, int docNum)
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
            log.error("Unable to create Manifest payload to ES", e);
            throw e;
        }

        transportClient.update(updateRequest).actionGet();
        log.info("Successfully persisted record with value: {}", docContent);
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

        log.info("got search index response to test metrics {}", searchResponse);
        Assertions.assertThat(searchResponse.getHits().getTotalHits().value)
                .isEqualTo(expectedResults);
        return searchResponse;
    }

    private GetIndexResponse getIndex(
            RestHighLevelClient esRestClient, String indexName, int expectedResults)
            throws IOException {
        final GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        final GetIndexResponse getIndexResponse =
                esRestClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        log.info("get index response: {}", getIndexResponse);
        Assertions.assertThat(getIndexResponse.getIndices().length).isEqualTo(expectedResults);

        return getIndexResponse;
    }

    private void testCreateRepository(RestHighLevelClient restHighLevelClient)
            throws IOException, NoSuchAlgorithmException {
        PutRepositoryRequest putRepositoryRequest = new PutRepositoryRequest();
        putRepositoryRequest.masterNodeTimeout(TimeValue.timeValueSeconds(10));
        putRepositoryRequest.settings(TestConstants.getRepositorySettings());
        putRepositoryRequest.type("oci");
        putRepositoryRequest.name(TEST_REPOSITORY_NAME);
        final AcknowledgedResponse acknowledgedResponse =
                restHighLevelClient
                        .snapshot()
                        .createRepository(putRepositoryRequest, RequestOptions.DEFAULT);
        log.info("Create repository response {}", acknowledgedResponse);
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
        log.info("testing rest request");
        final ClusterHealthResponse clusterHealthResponse =
                restHighLevelClient
                        .cluster()
                        .health(
                                new ClusterHealthRequest().waitForGreenStatus(),
                                RequestOptions.DEFAULT);
        log.info("rest response received {}", clusterHealthResponse);
        Assertions.assertThat(clusterHealthResponse.getStatus())
                .isEqualTo(ClusterHealthStatus.GREEN);
    }
    /**
     * Get the high level rest client for elastic search support
     *
     * @return RestHighLevelClient
     */
    public RestHighLevelClient getRestHighLevelClient() {
        RestClient restClient = getRestClient();
        final Node[] nodes = new Node[restClient.getNodes().size()];
        restClient.getNodes().toArray(nodes);
        final RestClientBuilder restClientBuilder = RestClient.builder(nodes);
        return new RestHighLevelClient(restClientBuilder);
    }
}
