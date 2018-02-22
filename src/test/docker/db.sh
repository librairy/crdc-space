#!/usr/bin/env bash
docker run -d --name crdc-db -p 9042:9042 cassandra:3.10