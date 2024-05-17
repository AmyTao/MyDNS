package raft

import (
	"io"
	"log"
	"os"
)

// Debugging
const Debug = 1

var (
	InfoRaft *log.Logger
	WarnRaft *log.Logger

	InfoKV *log.Logger //lab3 log
	ShardInfo *log.Logger //lab4 log
)

// Init log file
func init(){
	// Open new file for log
	infoFile, err := os.OpenFile("raftInfo.log", os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil{
		log.Fatalln("Open infoFile failed.\n", err)
	}
	warnFile, err := os.OpenFile("raftWarn.log", os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil{
		log.Fatalln("Open warnFile failed.\n", err)
	}

	InfoKVFile, err := os.OpenFile("kvInfo.log", os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil{
		log.Fatalln("Open infoKVFile failed.\n", err)
	}

	ShardInfoFile, err := os.OpenFile("shardInfo.log", os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil{
		log.Fatalln("Open shardInfoFile failed.\n", err)
	}

	InfoRaft = log.New(io.MultiWriter(infoFile), "InfoRaft:", log.Ldate | log.Ltime | log.Lshortfile)
	WarnRaft = log.New(io.MultiWriter(warnFile), "WarnRaft:", log.Ldate | log.Ltime | log.Lshortfile)
	InfoKV = log.New(io.MultiWriter(InfoKVFile), "InfoKV:", log.Ldate | log.Ltime | log.Lshortfile)
	ShardInfo = log.New(io.MultiWriter(os.Stderr, ShardInfoFile), "InfoShard:", log.Ldate | log.Ltime |log.Lshortfile)
}

func DPrintf(show bool, level string, format string, a ...interface{}) (n int, err error) {
	if Debug == 0{
		return
	}
	if !show{
		return
	}

	if level == "info"{
		InfoRaft.Printf(format, a...)
	}else if level == "warn"{
		WarnRaft.Printf(format, a...)
	}
	return
}
