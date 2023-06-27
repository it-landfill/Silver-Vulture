#!/bin/bash

echo Starting the script....

PROJECT_ID="silver-vulture-1"
CLUSTER_NAME="silver-nest"
BUCKET_NAME="silver-vulture-data"

echo Creating the Cluster...

REGION=europe-west1
ZONE=europe-west1-b

#gcloud dataproc clusters create $CLUSTER_NAME --enable-component-gateway --bucket $BUCKET_NAME --region $REGION --zone $ZONE --single-node --master-machine-type n1-custom-$CORES-$MEMORY --master-boot-disk-size 1000 --image-version 2.1-debian11 --project $PROJECT_ID
gcloud dataproc clusters create $CLUSTER_NAME --enable-component-gateway --region $REGION --zone $ZONE --master-machine-type n1-standard-2 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 2.1-debian11 --project $PROJECT_ID

echo Cluster created
