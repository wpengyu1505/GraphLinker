INPUT=$1
OUTPUT=$2

hadoop fs -rmr $OUTPUT
spark-submit --class wpy.graphlinker.LinkRunner \
    --num-executors 1 \
    graphlinker-1.0.0.jar \
    $INPUT \
    $OUTPUT
