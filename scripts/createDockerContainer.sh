#!/bin/bash
docker run -dit -v "/Users/aleben/UniProjects/Silver-Vulture:/silver-vulture" --publish 1080:1080 --name silver-vulture sbtscala/scala-sbt:eclipse-temurin-jammy-8u352-b08_1.9.0_3.3.0

# docker exec -it silver-vulture /bin/bash
# apt-get update
# apt-get install apt-transport-https ca-certificates gnupg curl sudo
# echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
# curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
# sudo apt-get update && sudo apt-get install google-cloud-cli
# gcloud init