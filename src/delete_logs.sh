if [ -f "kvInfo.log" ]; then
    rm "kvInfo.log"
    echo "kvInfo.log Deleted"
else
    echo "kvInfo.log not exist"
fi

if [ -f "shardInfo.log" ]; then
    rm "shardInfo.log"
    echo "shardInfo.log Deleted"
else
    echo "shardInfo.log not exist"
fi


if [ -f "raftInfo.log" ]; then
    rm "raftInfo.log"
    echo "raftInfo.log Deleted"
else
    echo "raftInfo.log not exist"
fi

if [ -f "raftWarn.log" ]; then
    rm "raftWarn.log"
    echo "raftWarn.log Deleted"
else
    echo "raftWarn.log not exist"
fi
