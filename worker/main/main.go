package main

import (
	"flag"
	"fmt"
	"github.com/han-xuefeng/crontab/worker"
	"runtime"
	"time"
)
var (
	confFile string
)
func initArgs(){
	flag.StringVar(&confFile,"config","./worker.json","worker.json")
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
	if err = worker.InitConfig(confFile);err != nil{
		goto ERR
	}
	fmt.Println("配置文件加载成功")
	//启动注册器
	if err = worker.InitRegister();err != nil{
		goto ERR
	}
	fmt.Println("服务注册成功")
	//启动日志
	if err = worker.InitLogSink();err != nil{
		goto ERR
	}
	fmt.Println("日志启动成功")
	//启动执行器
	if err = worker.InitExecutor();err != nil{
		goto ERR
	}
	fmt.Println("执行器启动成功")
	// 启动调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}
	fmt.Println("调度器启动成功")
	if err = worker.InitJobMgr();err != nil{
		goto ERR
	}
	fmt.Println("任务管理器启动成功")
	worker.InitBanner()
	fmt.Println("应用初始化完成")
	for{
		time.Sleep(1*time.Second)
	}
	return
ERR:
	fmt.Println(err)
}
