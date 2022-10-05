# Google Cloud DataProc - Apache Spark

## configure gcloud cli

```sh
# site
https://cloud.google.com/sdk/gcloud

# init
https://cloud.google.com/sdk/docs/cheatsheet
gcloud init

# gcloud cli
https://cloud.google.com/sdk/gcloud/reference/components/update
gcloud version
gcloud components list
gcloud components update
```

## create spark cluster [dataproc]

```sh
# compute engine [legacy] vs. gke
https://cloud.google.com/dataproc/docs/guides/dpgke/dataproc-gke-overview#:~:text=Unlike%20legacy%20Dataproc%20on%20Compute,pods%20on%20these%20node%20pools.

# create spark cluster
gcloud dataproc clusters create etlyelppy --enable-component-gateway --region us-central1 --zone us-central1-c --master-machine-type n1-standard-2 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 500 --image-version 2.0-debian10 --scopes 'https://www.googleapis.com/auth/cloud-platform' --project silver-charmer-243611

# submit job to the cluster

```

## submit a job on dataproc [serverless] = batch

```sh
# network = private google access (enable)

# list previous jobs
gcloud dataproc batches list

# submit job# submit
https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit?hl=en_US
gcloud dataproc batches submit pyspark --help

gcloud dataproc batches submit pyspark 'gs://owshq-code-repository/py-etl-yelp-reviews.py' \
    --batch=batch-06-py-etl-yelp-reviews  \
    --deps-bucket=gs://owshq-code-repository \
    --region='us-central1'

# get info 
gcloud dataproc batches describe batch-01-py-etl-yelp-reviews 

# verify log
gcloud dataproc batches wait batch-01-py-etl-yelp-reviews 
```
