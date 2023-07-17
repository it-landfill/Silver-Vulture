from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc
#from resolveAnimeByID import resolve_anime_list

cred_path = "./silver-vulture-2-34e87e1eb647.json"
jar_name = "Silver-Vulture.jar"


def upload_file(bucket_name, file_name):
    print("Uploading latest version of the jar...")
    storage_client = storage.Client.from_service_account_json(json_credentials_path=cred_path)
    bucket = storage.Bucket(storage_client, bucket_name)
    uploader = bucket.blob(file_name)
    uploader.upload_from_filename(file_name)
    print("Uploaded!")


def create_and_run_cluster(project_id, region, cluster_name, bucket_name):
    print("Creating the cluster...")
    cluster_client = dataproc.ClusterControllerClient.from_service_account_json(filename=cred_path, client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_size_gb": 100}
            },
            "worker_config": {
                "num_instances": 2,
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
    print(f"Cluster {res.cluster_name} created successfully.")
    print("Creating job definition...")

    job_client = dataproc.JobControllerClient.from_service_account_json(filename=cred_path, client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    job = {
        "placement": {"cluster_name": cluster_name},
        "spark_job": {
            "args": ["false", "true", "false", "silver-vulture-data_2"],
            "main_jar_file_uri": f"gs://{bucket_name}/{jar_name}"
        }
    }
    print("Job definition complete.")
    print("Submitting job...")
    ops = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    print("Job submitted.")
    try:
        response = ops.result()
        print(response)
        print("Done.")
    except Exception as e:
        print(f"Something went wrong on the dataproc cluster.\n{e}")
    print("Deleting the cluster...")
    ops = cluster_client.delete_cluster(
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    ops.result()
    print("Cluster deletion complete!")
    print("All's done.")


upload_file("silver-vulture-data_2", "Silver-Vulture.jar")
create_and_run_cluster("silver-vulture-2", "europe-west1", "silver-nest", "silver-vulture-data_2")
