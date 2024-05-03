package main
//尝试写socket
import (
    "fmt"
    "net"
    "strings"
	"MIT6.824/labrpc"
	"MIT6.824/raft"
	"MIT6.824/raftkv"

)


listen, err := net.ListenUDP("udp", &net.UDPAddr{
	IP:   net.IPv4(0, 0, 0, 0),
	Port: 9090,
})
if err != nil {
	fmt.Printf("listen failed error:%v\n", err)
	return
}
defer listen.Close() // 使用完关闭服务

for {
	// 接收数据
	var data [1024]byte
	
	n, addr, err := listen.ReadFromUDP(data[:])
	if err != nil {
		fmt.Printf("read data error:%v\n", err)
		return
	}
	fmt.Printf("addr:%v\t count:%v\t data:%v\n", addr, n, string(data[:n]))

	clientData := string(data[:n])
	parts := strings.Split(clientData, ":")
	method := parts[0]
	username :=parts[1]
	ip:=parts[2]

	//处理数据
	if method =="put"{
		Put(cfg, myck, username, ip)
	}
		
	if method =="get"{
		v := Get(cfg, myck, username)
		_, err = listen.WriteToUDP(data[:n], v)
		if err != nil {
			fmt.Printf("send data error:%v\n", err)
			return
	}
	}
	
}
