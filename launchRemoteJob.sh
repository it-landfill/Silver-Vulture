#!/bin/bash

echo Starting the script....

echo Compiling the project
rm -r ./target
sbt package

echo Uploading the jar to the bucket
BUCKET_NAME="silver-vulture-data_2"

JAR_PATH=./target/scala-2.12/
JAR_NAME=Silver-Vulture.jar

gsutil -m cp -R $JAR_PATH$JAR_NAME gs://$BUCKET_NAME/

echo Starting the cluster
CLUSTER_NAME="silver-nest"
REGION=europe-west1

gcloud dataproc clusters start $CLUSTER_NAME --region=$REGION

echo Starting the JOB
gcloud dataproc jobs submit spark --jar gs://$BUCKET_NAME/$JAR_NAME --cluster $CLUSTER_NAME --region $REGION

echo Stopping the cluster
gcloud dataproc clusters stop $CLUSTER_NAME --region=$REGION