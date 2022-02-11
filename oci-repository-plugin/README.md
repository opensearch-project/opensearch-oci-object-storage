# Build and install the plugin
To build the plugin zip distribution file
```bash
mvn clean install -PAdvanced
```
Install the plugin on your Elasticsearch cluster
```bash
ES_HOME=/Users/saherman/Downloads/elasticsearch-7.6.1

${ES_HOME}/bin/elasticsearch-plugin install file://target/releases/oci_repository_plugin-7.6.1.zip
```

Start your cluster.
```bash
${ES_HOME}/bin/elasticsearch
```
Note: if your cluster was already running make sure to restart it.

# 1. Configure the repository
Initially you would have to configure the repository.
You can do so either with user credentials or instance principal.

## Configure with user credentials
```bash
OCI_REGION=us-phoenix-1
OCI_TENANCY=bmc_siem_prod
OCI_BUCKET=elasticsearch-repository
OCI_BUCKET_COMPARTMENT='ocid1.tenancy.oc1..aaaaaaaarjna4lgtentm4jcujqgb3pjkk422h5dfblctgz4sd62tpacikinq'

curl -XPUT "https://localhost:9200/_snapshot/oci_repository" -H 'Content-Type: application/json' -d"
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
    \"credentials_file\": \"/Users/saherman/IdeaProjects/hippogriff-parser/hippogriff-eswriter/api_key.pem\"
  }
}
" -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
``` 

## Configure with instance principal

```bash
# if running on overlay you can configure the repository using user principal
curl -XPUT "https://localhost:9200/_snapshot/oci_repository" -H 'Content-Type: application/json' -d"
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
" -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
```

# 2. Test the repository
Download some sample data
```bash
curl -O https://download.elastic.co/demos/kibana/gettingstarted/7.x/shakespeare.json
```

Create mapping
```bash
curl -XPUT "https://localhost:9200/shakespeare" -H 'Content-Type: application/json' -d'
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
' -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
```

Push data to ES
```bash
curl -H 'Content-Type: application/x-ndjson' -XPOST "https://localhost:9200/shakespeare/_bulk?pretty" --data-binary @shakespeare.json -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
```

Create snapshot
```bash
curl -XPUT "https://localhost:9200/_snapshot/oci_repository/snapshot_1?wait_for_completion=true" -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
```

Verify snapshot created
```bash
curl -X GET "https://localhost:9200/_cat/snapshots/oci_repository?v&s=id&pretty" -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
```

Restore specific index from snapshot with a replacement/rename strategy
```bash
curl -X POST "localhost:9200/_snapshot/oci_repository/snapshot_1/_restore?pretty" -H 'Content-Type: application/json' -d'
{
  "indices": "shakespeare",
  "ignore_unavailable": true,
  "include_global_state": false,
  "rename_pattern": "shakespear(.+)",
  "rename_replacement": "restored_index_$1",
  "include_aliases": false
}
' -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
```

Check your indices
```bash
curl -X GET "https://localhost:9200/_cat/indices" -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
```

# 3. Restore new cluster from repository
Reconfigure your repository (see step 1 for user cred vs instance principals)
```bash
curl -XPUT "https://localhost:9200/_snapshot/oci_repository" -H 'Content-Type: application/json' -d"
{
  \"type\": \"oci\",
  \"settings\": {
    \"client\": \"default\",
    \"region\": \"${OCI_REGION}\",
    \"endpoint\": \"https://objectstorage.${OCI_REGION}.oraclecloud.com\",
    \"bucket\": \"elasticsearch-repository\",
    \"namespace\": \"bmc_siem_prod\",
    \"userId\" : \"ocid1.user.oc1..aaaaaaaa5vtbkg4omdvni7t67izxphsvmqdnkfhhspn54hvo7n5no65332yq\",
    \"tenantId\" : \"ocid1.tenancy.oc1..aaaaaaaafi4l6qecddifs7kew5uzc24xwvtraosoiyvjgc5rq26nciigrhtq\",
    \"fingerprint\" : \"89:36:28:98:a4:ed:0b:ad:46:90:a8:12:7d:2e:1a:a1\",
    \"credentials_file\": \"/Users/saherman/IdeaProjects/hippogriff-parser/hippogriff-eswriter/api_key.pem\"
  }
}
" -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure
```

Now you should be able to see your snapshots
```bash
curl -XGET 'https://localhost:9200/_snapshot/oci_repository/_all?pretty' -u admin:adminnnnnnnnnnnnnnnnnnnn --insecure 
```

