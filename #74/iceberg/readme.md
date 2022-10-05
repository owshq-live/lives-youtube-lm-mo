# Apache Spark [3.2.1] & Apache Iceberg [0.13.0]
https://iceberg.apache.org/
https://github.com/apache/iceberg

### minio (s3)
```shell
http://minio-ip:9090/
```

### install apache spark on [macos]
```shell
# update outdated packages
brew upgrade && brew update

# install apache spark
pip install -r requirements.txt

# remove spark and dependencies
pip uninstall pyspark
rm -rf /usr/local/lib/python3.9/site-packages/pyspark/jars

# verify pyspark version
spark-submit --version
```

### dealing with [jars]
```shell
# jars list
aws-java-sdk-1.12.254.jar                 
aws-java-sdk-dynamodb-1.12.254.jar        
aws-java-sdk-s3-1.12.254.jar      
aws-java-sdk-core-1.12.254.jar           
aws-java-sdk-kms-1.12.254.jar               
hadoop-aws-3.2.2.jar                      
iceberg-spark-runtime-3.2_2.12-0.13.0.jar
iceberg-hive-runtime-0.13.0.jar          
bundle-2.17.220.jar                       
url-connection-client-2.17.220.jar

# copy jars to spark's folder
/usr/local/lib/python3.9/site-packages/pyspark/jars/
cp -a /Users/luanmorenomaciel/BitBucket/owshq-svc-spark/etl-enriched-users-analysis/iceberg/jars/* /usr/local/lib/python3.9/site-packages/pyspark/jars/
```

### building image
```shell
# build image
# tag image
# push image to registry
docker build . -t etl-enriched-users-analysis-iceberg:3.2.1
docker tag etl-enriched-users-analysis-iceberg:3.2.1 owshq/etl-enriched-users-analysis-iceberg:3.2.1
docker push owshq/etl-enriched-users-analysis-iceberg:3.2.1
```

### deploying application on kubernetes [spark-on-k8s-operator]
```shell
# select cluster to deploy
kubectx YourKubernetesCluster
k top nodes

# verify spark operator
kubens processing  
helm ls -n processing
kgp -n processing

# deploy spark application [kubectl] for testing purposes
# [deploy application]
kubens processing
k apply -f crb-spark-operator-processing.yaml -n processing
k apply -f etl-enriched-users-analysis-iceberg.yaml -n processing

# get yaml detailed info
# verify submit
k get sparkapplications
k get sparkapplications etl-enriched-users-analysis-iceberg -o=yaml
k describe sparkapplication etl-enriched-users-analysis-iceberg

# verify logs in real-time
# port forward to spark ui
POD=etl-enriched-users-analysis-iceberg-driver
k port-forward $POD 4040:4040

# housekeeping
k delete SparkApplication etl-enriched-users-analysis-iceberg -n processing
k delete clusterrolebinding crb-spark-operator-processing
```

### connecting to hive metastore
```sql
-- port forwarder for thrift and database


-- table & database
SELECT t.*
FROM metastore.TBLS t
JOIN metastore.DBS d
ON t.DB_ID = d.DB_ID
WHERE d.NAME='default';

-- location
SELECT s.*
FROM metastore.TBLS t
JOIN metastore.DBS d
ON t.DB_ID = d.DB_ID
JOIN metastore.SDS s
ON t.SD_ID = s.SD_ID
WHERE d.NAME='default';

-- columns
SELECT c.*
FROM metastore.TBLS t
JOIN metastore.DBS d
ON t.DB_ID = d.DB_ID
JOIN metastore.SDS s
ON t.SD_ID = s.SD_ID
JOIN metastore.COLUMNS_V2 c
ON s.CD_ID = c.CD_ID
WHERE d.NAME='default'
ORDER by INTEGER_IDX;
```

### verify minio location
```shell
# bucket = hive
# warehouse
```

### query data using [trino]
```sql
-- access trino deployment using port-forwarder
-- catalog
show catalogs;
show schemas from iceberg;
show tables from iceberg.default;

-- query data
SELECT *
FROM iceberg.default.device
LIMIT 10;

SELECT *
FROM iceberg.default.subscription
LIMIT 10;

SELECT *
FROM iceberg.default.plans
LIMIT 10;

-- count
SELECT COUNT(*)
FROM iceberg.default.plans;
```
