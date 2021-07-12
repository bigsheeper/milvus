cd ..

echo "starting rootcoord"
nohup ./bin/milvus run rootcoord > /tmp/rootcoord.log 2>&1 &

echo "starting datacoord"
nohup ./bin/milvus run datacoord > /tmp/datacoord.log 2>&1 &

echo "starting datanode"
nohup ./bin/milvus run datanode > /tmp/datanode.log 2>&1 &

echo "starting proxy"
nohup ./bin/milvus run proxy > /tmp/proxy.log 2>&1 &

echo "starting querycoord"
nohup ./bin/milvus run querycoord > /tmp/querycoord.log 2>&1 &

echo "starting querynode1"
nohup ./bin/milvus run querynode --alias querynode:1 > /tmp/querynode1.log 2>&1 &

echo "starting querynode2"
nohup ./bin/milvus run querynode --alias querynode:2 > /tmp/querynode2.log 2>&1 &

echo "starting querynode3"
nohup ./bin/milvus run querynode --alias querynode:3 > /tmp/querynode3.log 2>&1 &

echo "starting querynode4"
nohup ./bin/milvus run querynode --alias querynode:4 > /tmp/querynode4.log 2>&1 &




echo "starting querynode5"
nohup ./bin/milvus run querynode --alias querynode:5 > /tmp/querynode5.log 2>&1 &

echo "starting querynode6"
nohup ./bin/milvus run querynode --alias querynode:6 > /tmp/querynode6.log 2>&1 &

echo "starting querynode7"
nohup ./bin/milvus run querynode --alias querynode:7 > /tmp/querynode7.log 2>&1 &

echo "starting querynode8"
nohup ./bin/milvus run querynode --alias querynode:8 > /tmp/querynode8.log 2>&1 &





echo "starting indexcoord"
nohup ./bin/milvus run indexcoord > /tmp/indexcoord.log 2>&1 &

echo "starting indexnode"
nohup ./bin/milvus run indexnode > /tmp/indexnode.log 2>&1 &