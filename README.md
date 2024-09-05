# Commands:
## Setup
chmod +x setup.sh
./setup.sh

## Submite jobs
${FLINK_HOME}/bin/flink run --jobmanager localhost:8081 --python word_count.py