#!/bin/bash

wget https://archive.apache.org/dist/flink/flink-1.19.1/flink-1.19.1-bin-scala_2.12.tgz
tar -xvzf flink-1.19.1-bin-scala_2.12.tgz
mv flink-1.19.1 flink
echo FLINK_HOME=$PWD/flink >> ~/.bashrc
source ~/.bashrc

# install java
# check if java is installed
if ! command -v java &> /dev/null; then
    echo "Java is not installed. Installing Java 1.8.0..."
    sudo apt-get update
    sudo apt-get install openjdk-8-jdk -y
    echo JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 >> ~/.bashrc
    source ~/.bashrc
else
    echo "Java is already installed."
fi

# create venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


# test flink
echo "Running word_count.py test job on Flink... (Expected result: Program execution finished)"
${FLINK_HOME}/bin/flink run --jobmanager localhost:8081 --python word_count.py