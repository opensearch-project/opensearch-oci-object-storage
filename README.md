# OpenSearch OCI Object Storage Plugin
This plugin allows the user to take snapshots into OCI Object storage from the OpenSearch cluster.
Some important features that make this plugin very friendly for production deployments are:
1. Authentication with instance principals
2. Option for force bucket creation for repository

Note: The recommended authentication method using this plugin is instance principals.

## Build and install the plugin
To build the plugin zip distribution file and running all the tests
```bash
mvn clean install -PAdvanced
```

Install the plugin on your OpenSearch cluster
```bash
OPENSEARCH_HOME=<YOUR OPENSEARCH INSTALLATION PATH HERE> # (e.g. /Users/saherman/opensearch)

${OPENSEARCH_HOME}/bin/opensearch-plugin install file://target/releases/oci_repository_plugin-1.2.4.0.zip
```

Start your cluster.
```bash
${OPENSEARCH_HOME}/bin/opensearch
```
Note: if your cluster was already running make sure to restart it.

## 1. Configure the repository
Initially you would have to configure the repository.
You can do so either with user credentials or instance principal.

### Configure with user credentials
```bash
OCI_REGION=us-phoenix-1
OCI_TENANCY=my_prod_tenancy
OCI_BUCKET=opensearch-repository
OCI_BUCKET_COMPARTMENT='ocid1.tenancy.oc1..aaaaaaaarjna4lgtentm4jcujqgb3pjkk422h5dfblctgz4sd62tpacikinq'
CREDENTIALS_FILE_PATH='/Users/saherman/my_sample_key.pem'

curl -XPUT "http://localhost:9200/_snapshot/oci_repository" -H 'Content-Type: application/json' -d"
{
  \"type\": \"oci\",
  \"settings\": {
    \"client\": \"default\",
    \"region\": \"${OCI_REGION}\",
    \"endpoint\": \"https://objectstorage.${OCI_REGION}.oraclecloud.com\",
    \"bucket\": \"${OCI_BUCKET}\",
    \"namespace\": \"bmc_siem_prod\",
    \"forceBucketCreation\" : true,
    \"bucket_compartment_id\": \"${OCI_BUCKET_COMPARTMENT}\",
    \"userId\" : \"ocid1.user.oc1..aaaaaaaa5vtbkg4omdvni7t67izxphsvmqdnkfhhspn54hvo7n5no65332yq\",
    \"tenantId\" : \"ocid1.tenancy.oc1..aaaaaaaafi4l6qecddifs7kew5uzc24xwvtraosoiyvjgc5rq26nciigrhtq\",
    \"fingerprint\" : \"89:36:28:98:a4:ed:0b:ad:46:90:a8:12:7d:2e:1a:a1\",
    \"credentials_file\": \"${CREDENTIALS_FILE_PATH}\"
  }
}
"
``` 

### Configure with instance principal

```bash
# if running on overlay you can configure the repository using instance principal
curl -XPUT "http://localhost:9200/_snapshot/oci_repository" -H 'Content-Type: application/json' -d"
{
  \"type\": \"oci\",
  \"settings\": {
    \"client\": \"default\",
    \"endpoint\": \"https://objectstorage.${OCI_REGION}.oraclecloud.com\",
    \"bucket\": \"${OCI_BUCKET}\",
    \"namespace\": \"bmc_siem_prod\",
    \"bucket_compartment_id\": \"${OCI_BUCKET_COMPARTMENT}\",
    \"useInstancePrincipal\": true,
    \"forceBucketCreation\" : true
  }
}
"
```

## 2. Test the repository
Download some sample data
```bash
curl -O https://download.elastic.co/demos/kibana/gettingstarted/7.x/shakespeare.json
```

Create mapping
```bash
curl -XPUT "http://localhost:9200/shakespeare" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
    "speaker": {"type": "keyword"},
    "play_name": {"type": "keyword"},
    "line_id": {"type": "integer"},
    "speech_number": {"type": "integer"}
    }
  }
}
'
```

Push data to OpenSearch
```bash
curl -H 'Content-Type: application/x-ndjson' -XPOST "http://localhost:9200/shakespeare/_bulk?pretty" --data-binary @shakespeare.json
```

Create snapshot
```bash
curl -XPUT "http://localhost:9200/_snapshot/oci_repository/snapshot_1?wait_for_completion=true"
```

Verify snapshot created
```bash
curl -X GET "http://localhost:9200/_cat/snapshots/oci_repository?v&s=id&pretty"
```

Restore specific index from snapshot with a replacement/rename strategy
```bash
curl -X POST "http://localhost:9200/_snapshot/oci_repository/snapshot_1/_restore?pretty" -H 'Content-Type: application/json' -d'
{
  "indices": "shakespeare",
  "ignore_unavailable": true,
  "include_global_state": false,
  "rename_pattern": "shakespear(.+)",
  "rename_replacement": "restored_index_$1",
  "include_aliases": false
}
'
```

Check your indices
```bash
curl -X GET "http://localhost:9200/_cat/indices"
```

## 3. Restore new cluster from repository
Reconfigure your repository (see step 1 for user cred vs instance principals)
```bash
curl -XPUT "http://localhost:9200/_snapshot/oci_repository" -H 'Content-Type: application/json' -d"
{
  \"type\": \"oci\",
  \"settings\": {
    \"client\": \"default\",
    \"region\": \"${OCI_REGION}\",
    \"endpoint\": \"https://objectstorage.${OCI_REGION}.oraclecloud.com\",
    \"bucket\": \"opensearch-repository\",
    \"namespace\": \"bmc_siem_prod\",
    \"userId\" : \"ocid1.user.oc1..aaaaaaaa5vtbkg4omdvni7t67izxphsvmqdnkfhhspn54hvo7n5no65332yq\",
    \"tenantId\" : \"ocid1.tenancy.oc1..aaaaaaaafi4l6qecddifs7kew5uzc24xwvtraosoiyvjgc5rq26nciigrhtq\",
    \"fingerprint\" : \"89:36:28:98:a4:ed:0b:ad:46:90:a8:12:7d:2e:1a:a1\",
    \"credentials_file\": \"${CREDENTIALS_FILE_PATH}"
  }
}
"
```

Now you should be able to see your snapshots
```bash
curl -XGET 'https://localhost:9200/_snapshot/oci_repository/_all?pretty' 
```



## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

