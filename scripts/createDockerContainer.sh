#!/bin/bash
docker run -dit -v "/Users/aleben/UniProjects/Silver-Vulture:/silver-vulture" --publish 1080:1080 --publish 4040:4040 --name silver-vulture sbtscala/scala-sbt:eclipse-temurin-jammy-8u352-b08_1.9.0_3.3.0

# docker exec -it silver-vulture /bin/bash