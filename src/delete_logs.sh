if [ -f "kvInfo.log" ]; then
    rm "kvInfo.log"
    echo "kvInfo.log 已删除"
else
    echo "kvInfo.log 不存在"
fi

if [ -f "shardInfo.log" ]; then
    rm "shardInfo.log"
    echo "shardInfo.log 已删除"
else
    echo "shardInfo.log 不存在"
fi


if [ -f "raftInfo.log" ]; then
    rm "raftInfo.log"
    echo "raftInfo.log 已删除"
else
    echo "raftInfo.log 不存在"
fi

if [ -f "raftWarn.log" ]; then
    rm "raftWarn.log"
    echo "raftWarn.log 已删除"
else
    echo "raftWarn.log 不存在"
fi
