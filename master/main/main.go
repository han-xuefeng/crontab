package main

import (
	"flag"
	"fmt"
	"github.com/han-xuefeng/crontab/master"
	"runtime"
	"strconv"
	"time"
)
var (
	confFile string
)
func initArgs(){
	flag.StringVar(&confFile,"config","./master.json","指定master.json")
	flag.Parse()
}

func initEnv()  {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	var (
		err error
	)
	//初始化参数
	initArgs()
	//初始化线程
	initEnv()

	//加载配置
	if err = master.InitConfig(confFile);err != nil{
		goto ERR
	}
	fmt.Println("配置文件加载成功")
	if err = master.InitLogMgr();err != nil{
		goto ERR
	}
	fmt.Println("日志启动成功")
	// 初始化服务发现模块
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}
	fmt.Println("注册模块启动成功")
	//初始话etcd
	if err = master.InitJobMgr();err != nil{
		goto ERR
	}
	fmt.Println("任务管理器启动成功")
	//启动api http服务
	if err = master.InitApiServer();err != nil{
		goto ERR
	}
	master.InitBanner()
	fmt.Println("应用初始化完成:listen:127.0.0.1:"+ strconv.Itoa(master.G_config.ApiPort))
	for{
		time.Sleep(1*time.Second)
	}
	return
	ERR:
		fmt.Println(err)
}
