#!/bin/bash

CP="target/classes:lib/*"

if [ ! -d "target/classes" ]; then
    echo "Please compile the project first: mvn compile"
    exit 1
fi

if [ ! -d "lib" ]; then
    echo "Dependencies not found. Running: mvn dependency:copy-dependencies"
    mvn dependency:copy-dependencies -DoutputDirectory=lib
fi

echo "Classpath: $CP"

# 运行仿真
java -cp "$CP" joshua.green.data.FedRL.rltest "$@"