"""
Experiment 2 (Flink edition): Custom producer -> Apache Kafka -> Apache Flink

Reads the `server-logs` topic, parses each record
    "yyyy-MM-dd HH:mm:ss | LEVEL | message | server"
and writes the result as JSON files partitioned by server and level.

Install PyFlink (inside the Spark-Notebook container):


Run (inside the Spark-Notebook container):
    python3 flink_consumer.py
"""
import shutil
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_JAR = "file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar"
BOOTSTRAP = "kafka:9093"
TOPIC = "server-logs"
OUTPUT_PATH = "/opt/flink/work-dir/data/streaming/flink_output"
CHECKPOINT_PATH = "/opt/flink/work-dir/checkpoints/flink_logs_checkpoint"

# --- Clean previous runs (same idea as the Spark notebook) -------------------
for p in (OUTPUT_PATH, CHECKPOINT_PATH):
    if Path(p).is_dir():
        shutil.rmtree(p)

# --- 1. Streaming Table environment ------------------------------------------
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
conf = t_env.get_config()
conf.set("pipeline.jars", KAFKA_JAR)
conf.set("parallelism.default", "1")
# The filesystem sink only finalizes files on checkpoints -> checkpointing is mandatory
conf.set("execution.checkpointing.interval", "10 s")
conf.set("execution.checkpointing.dir", f"file://{CHECKPOINT_PATH}")

# --- 2. Source: Kafka topic as a dynamic table ---------------------------------
t_env.execute_sql(f"""
    CREATE TABLE server_logs_src (
        `value`    STRING,
        `kafka_ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = '{TOPIC}',
        'properties.bootstrap.servers' = '{BOOTSTRAP}',
        'properties.group.id'          = 'flink-server-logs',
        'scan.startup.mode'            = 'earliest-offset',
        'format'                       = 'raw'
    )
""")

# --- 3. Sink: partitioned files (equivalent of partitionBy("server","level")) ---
t_env.execute_sql(f"""
    CREATE TABLE server_logs_sink (
        `timestamp` TIMESTAMP(3),
        `message`   STRING,
        `server`    STRING,
        `level`     STRING
    ) PARTITIONED BY (`server`, `level`) WITH (
        'connector'                             = 'filesystem',
        'path'                                  = 'file://{OUTPUT_PATH}',
        'format'                                = 'json',
        'sink.rolling-policy.rollover-interval' = '30 s',
        'sink.partition-commit.policy.kind'     = 'success-file'
    )
""")

# --- 4. Transformation + continuous INSERT -------------------------------------
result = t_env.execute_sql("""
    INSERT INTO server_logs_sink
    SELECT ts, msg, srv, lvl
    FROM (
        SELECT
            TO_TIMESTAMP(TRIM(SPLIT_INDEX(`value`, '|', 0)), 'yyyy-MM-dd HH:mm:ss') AS ts,
            TRIM(SPLIT_INDEX(`value`, '|', 1)) AS lvl,
            TRIM(SPLIT_INDEX(`value`, '|', 2)) AS msg,
            TRIM(SPLIT_INDEX(`value`, '|', 3)) AS srv
        FROM server_logs_src
    )
    WHERE ts IS NOT NULL
""")

job_client = result.get_job_client()
print(f"Flink job submitted: {job_client.get_job_id()}  (Ctrl+C to stop)")

try:
    result.wait()                      # blocks, like awaitAnyTermination()
except KeyboardInterrupt:
    job_client.cancel().result()       # like query.stop()
    print("Job cancelled.")