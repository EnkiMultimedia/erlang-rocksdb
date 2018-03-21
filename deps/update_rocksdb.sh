#!/bin/bash
TAG=$1
URL="https://github.com/facebook/rocksdb/archive/$TAG.tar.gz"
echo "Downloading $URL"
curl -o /tmp/rocksdb.tar.gz -L $URL
tar xvzf /tmp/rocksdb.tar.gz
rm -rf rocksdb
mv rocksdb-${TAG} rocksdb
echo "Use git status, add all files and commit changes."
