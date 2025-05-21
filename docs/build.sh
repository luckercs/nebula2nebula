#!/bin/bash

set -e

if command -v mvn > /dev/null 2>&1
then
    echo "check mvn ok"
else
    echo "mvn not found, please install it first"
    exit 1
fi

if command -v java > /dev/null 2>&1
then
    echo "check java ok"
else
    echo "java not found, please install it first"
    exit 1
fi

script_dir=$(dirname "$0")
cd "$script_dir"
project_dir=$(dirname "$(pwd)")

echo "build ..."
cd $project_dir
mvn clean package -Dmaven.test.skip=true

echo "build successfully"
