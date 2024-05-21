Flink on yarn

# 1.pre-job
flink run -t yarn-per-job -m yarn-cluster FlinkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar

# 2.application
flink run-application \
-t yarn-application \
-Djobmanager.memory.process.size=1024m \
-Dtaskmanager.memory.process.size=1024m \
-Dtaskmanager.numberOfTaskSlots=1 \
-Dparallelism.default=1 \
-Dyarn.provided.lib.dirs="hdfs:///flink-1.19.0/lib"  \
-Dyarn.application.name="run-app" \
FlinkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar



