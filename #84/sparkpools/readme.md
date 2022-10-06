# Apache Spark in Azure Synapse Analytics a.k.a [Spark Pools]

## configure az cli

```sh
# site
https://learn.microsoft.com/en-us/cli/azure/

# az cli
az 
az --version
az upgrade

# login
az login
az account show --output table

# set subscription
https://learn.microsoft.com/en-us/cli/azure/manage-azure-subscriptions-azure-cli
```

## create spark pool

```sh
# get workspace name
az synapse workspace list --output table

# list & create spark cluster
az synapse spark pool list  -g OwsHQ_EastUs2 --workspace-name owshq-synapse --output table
az synapse spark pool create --help

# create cluster
az synapse spark pool create \
    --name etlyelppy \
    --workspace-name owshq-synapse \
    --resource-group OwsHQ_EastUs2 \
    --enable-auto-scale true \
    --min-node-count 3 \
    --max-node-count 5 \
    --spark-version 3.2 \
    --node-count 3 \
    --node-size Medium \
    --node-size-family MemoryOptimized
```

## submit spark job to cluster

```sh
# list submitted jobs
az synapse spark job list --spark-pool-name etlyelppy --workspace-name owshq-synapse

# submit a job
az synapse spark job submit --help

# get location [pyspark] resides
az synapse spark job submit \
    --executor-size Medium \
    --executors 3 \
    --main-definition-file abfss://bs-stg-files@owshqblobstg.dfs.core.windows.net/app/cluster.py \
    --name etl-yelp-py-01 \
    --spark-pool-name etlyelppy \
    --workspace-name owshq-synapse \
    --language Python

# collect status
az synapse spark job show \
    --livy-id 1 \
    --spark-pool-name etlyelppy \
    --workspace-name owshq-synapse

# submit app
https://web.azuresynapse.net/en/monitoring/sparkapplication?workspace=%2Fsubscriptions%2F66389d29-a9b6-425b-b699-f6894520d87d%2FresourceGroups%2FOwsHQ_EastUs2%2Fproviders%2FMicrosoft.Synapse%2Fworkspaces%2Fowshq-synapse
```
