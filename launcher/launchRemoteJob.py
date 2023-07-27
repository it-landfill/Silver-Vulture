from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc
import datetime
from resolveAnimeByID import resolve_anime_list

# Configuration

cred_path = "./silver-vulture-2-34e87e1eb647.json"
jar_name = "Silver-Vulture.jar"
is_running_locally = "false"
use_mllib = "false"
regen_ranking = "true"
run_evaluation = "true"
user_id = "8723558"
threshold = "6"
number_of_results = "100"
bucket_name = "silver-vulture-data_2"
project_id = "silver-vulture-2"
region = "europe-west1"
cluster_name = "silver-nest"
similarity_ceil = "0.4"


def upload_file(bucket_name, file_name):
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Uploading latest version of the jar...")
    storage_client = storage.Client.from_service_account_json(json_credentials_path=cred_path)
    bucket = storage.Bucket(storage_client, bucket_name)
    uploader = bucket.blob(file_name)
    uploader.upload_from_filename(file_name)
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Uploaded!")


def download_file(bucket_name, file_name):
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Downloading results...")
    storage_client = storage.Client.from_service_account_json(json_credentials_path=cred_path)
    bucket = storage_client.get_bucket(bucket_name)
    files = list(bucket.list_blobs(prefix=file_name))
    data = []
    for elem in files:
        data.append(elem.download_as_string())
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Downloaded!")
    return data


def parse_csv(data):
    result = []
    for elem in data:
        if len(elem):
            lines = elem.decode("utf-8").split("\n")
            for i in range(1, len(lines)-1):
                elems = lines[i].split(",")
                result.append([int(elems[0]), float(elems[1])])
    return result


def create_and_run_cluster(project_id, region, cluster_name, bucket_name):
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Creating the cluster...")
    cluster_client = dataproc.ClusterControllerClient.from_service_account_json(filename=cred_path, client_options={
        "api_endpoint": f"{region}-dataproc.googleapis.com:443"})
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-highmem-2",
                "disk_config": {"boot_disk_size_gb": 100}
            },
            "worker_config": {
                "num_instances": 5,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {"boot_disk_size_gb": 100}
            },
            "endpoint_config": {
                "enable_http_port_access": True
            },
        }
    }

    ops = cluster_client.create_cluster(request={
        "project_id": project_id,
        "region": region,
        "cluster": cluster
    })
    res = ops.result()
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Cluster {res.cluster_name} created successfully.")
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Creating job definition...")

    job_client = dataproc.JobControllerClient.from_service_account_json(filename=cred_path, client_options={
        "api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    job = {
        "placement": {"cluster_name": cluster_name},
        "spark_job": {
            "args": [is_running_locally, use_mllib, regen_ranking, run_evaluation, bucket_name, user_id, threshold,
                     number_of_results, similarity_ceil],
            "main_jar_file_uri": f"gs://{bucket_name}/{jar_name}",
            #"properties": {"spark.sql.autoBroadcastJoinThreshold": "-1"}
            "properties": {"spark.sql.broadcastTimeout": "1200"}
        }
    }
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Job definition complete.")
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Submitting job...")
    start_time = datetime.datetime.now().timestamp()
    ops = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Job submitted.")
    try:
        response = ops.result(timeout=99999999999)
        print(response)
        print(f"[LOG] {datetime.datetime.now().timestamp()} - Done.")
    except Exception as e:
        print(f"[ERR] {datetime.datetime.now().timestamp()} - Something went wrong on the dataproc cluster.\n{e}")
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Deleting the cluster...")
    end_time = datetime.datetime.now().timestamp()
    import json
    print(f"{start_time} - {end_time}")
    with open("experiments.json", "a+") as file:
        json.dump({"config": cluster, "start": str(start_time), "end": str(end_time)}, file)

    ops = cluster_client.delete_cluster(
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    ops.result()
    print(f"[LOG] {datetime.datetime.now().timestamp()} - Cluster deletion complete!")


upload_file(bucket_name, jar_name)
create_and_run_cluster(project_id, region, cluster_name, bucket_name)
result = resolve_anime_list(parse_csv(download_file(bucket_name, f"out/{user_id}")))
for elem in result:
    print(elem)
