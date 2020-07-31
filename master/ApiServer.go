package master

import (
	"encoding/json"
	"net"
	"net/http"
	"owenliang/crontab/common"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var (
	G_apiServer *ApiServer
)

func handleJobSave(w http.ResponseWriter,r *http.Request)  {
	//1.解析post表单
	var (
		err error
		postJob string
		job common.Job
		oldJob *common.Job
		reps []byte
	)
	if err = r.ParseForm();err != nil{
		goto ERR
	}
	//2.取表单中的字段
	postJob = r.PostForm.Get("job")
	//3.反序列化job
	if err = json.Unmarshal([]byte(postJob),&job); err != nil{
		goto ERR
	}
	if oldJob,err = G_jobMgr.SaveJob(&job);err != nil{
		goto ERR
	}
	//放回正常应答
	if reps,err = common.BuildResponse(0,"success",oldJob);err == nil{
		w.Write(reps)
	}
	return
ERR:
	//返回异常应答
	if reps ,err = common.BuildResponse(-1,err.Error(),nil);err != nil{
		w.Write(reps)
	}
}

func handleJobDelete(w http.ResponseWriter,r *http.Request)  {
	var (
		err error
		name string
		reps []byte
		oldJob *common.Job
	)
	if err = r.ParseForm();err != nil{
		goto ERR
	}
	//2.取表单中的字段
	name = r.PostForm.Get("name")
	if oldJob,err = G_jobMgr.DeleteJob(name);err != nil{
		goto ERR
	}
	if reps,err = common.BuildResponse(0,"success",oldJob);err == nil{
		w.Write(reps)
	}

	return
ERR:
	if reps ,err = common.BuildResponse(-1,err.Error(),nil);err != nil{
		w.Write(reps)
	}
}

func handleJobList(w http.ResponseWriter,r *http.Request)  {
	var (
		list []*common.Job
		err error
		reps []byte
	)
	if list,err = G_jobMgr.ListJob();err != nil{
		goto ERR
	}
	if reps,err = common.BuildResponse(0,"success",list);err == nil{
		w.Write(reps)
	}
	return
ERR:
	if reps ,err = common.BuildResponse(-1,err.Error(),nil);err != nil{
		w.Write(reps)
	}
}

func handleJobKill(w http.ResponseWriter,r *http.Request){
	var (
		err error
		name string
		bytes []byte
	)
	if err = r.ParseForm();err != nil{
		goto ERR
	}
	//2.取表单中的字段
	name = r.PostForm.Get("name")

	if err = G_jobMgr.KillJob(name);err != nil{
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}

}

func handleJobLog(w http.ResponseWriter,r *http.Request){
	var (
		err error
		name string
		bytes []byte
		logArr []*common.JobLog

		skipParam string// 从第几条开始
		limitParam string // 返回多少条
		skip int
		limit int

		logCount int64
		logData map[string]interface{}
	)
	if err = r.ParseForm();err != nil{
		goto ERR
	}
	//2.取表单中的字段
	name = r.Form.Get("name")
	skipParam = r.Form.Get("skip")
	limitParam = r.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}

	//统计长度
	if logCount,err = G_logMgr.Count(name);err != nil{
		goto ERR
	}

	if logArr,err = G_logMgr.List(name,skip,limit);err != nil{
		goto ERR
	}
	logData = make(map[string]interface{})
	logData["logCount"] = logCount
	logData["logArr"] = logArr
	if bytes, err = common.BuildResponse(0, "success", logData); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}
func handleWorkerList (w http.ResponseWriter,r *http.Request){

	var (
		workerArr []string
		err error
		bytes []byte
	)

	if workerArr,err = G_workerMgr.ListWorkers();err != nil{
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}
func InitApiServer() (err error) {
	var (
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir
		staticHandler http.Handler
	)

	//配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save",handleJobSave)
	mux.HandleFunc("/job/delete",handleJobDelete)
	mux.HandleFunc("/job/list",handleJobList)
	mux.HandleFunc("/job/kill",handleJobKill)
	mux.HandleFunc("/job/log",handleJobLog)
	mux.HandleFunc("/worker/list",handleWorkerList)

	//静态文件目录
	staticDir = http.Dir(G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/",http.StripPrefix("/",staticHandler))

	//启动Tcp监听
	if listener,err = net.Listen("tcp",":"+strconv.Itoa(G_config.ApiPort));err != nil{
		return
	}

	//创建一个http服务
	httpServer = &http.Server{
		ReadTimeout: time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler: mux,
	}
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	go httpServer.Serve(listener)

	return
}
