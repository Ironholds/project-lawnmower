export HADOOP_HEAPSIZE=1024 && hive -f ./queries/q1_query.sql > ./data/q1_baseline.tsv && R CMD BATCH q1.R
R CMD BATCH q2.R && hive -f ./queries/q3_query.sql > ./data/q3_results.tsv