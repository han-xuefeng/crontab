package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

//定时任务
type Job struct {
	Name string `json:"name"`
	Command string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

//任务调度计划
type JobSchedulerPlan struct {
	Job *Job
	Expr *cronexpr.Expression
	NextTime time.Time
}
//任务执行状态
type JobExecuteInfo struct {
	Job *Job //任务信息
	PlanTime time.Time //理论上的调度时间
	RealTime time.Time //实际调度时间
	CancelFunc context.CancelFunc
	CancelCxt context.Context
}

type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

// 变化事件
type JobEvent struct {
	EventType int //  SAVE, DELETE
	Job *Job
}

//任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

// 任务执行日志
type JobLog struct {
	JobName string `json:"jobName" bson:"jobName"` // 任务名字
	Command string `json:"command" bson:"command"` // 脚本命令
	Err string `json:"err" bson:"err"` // 错误原因
	Output string `json:"output" bson:"output"`	// 脚本输出
	PlanTime int64 `json:"planTime" bson:"planTime"` // 计划开始时间
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"` // 实际调度时间
	StartTime int64 `json:"startTime" bson:"startTime"` // 任务执行开始时间
	EndTime int64 `json:"endTime" bson:"endTime"` // 任务执行结束时间
}

type LogBatch struct {
	Logs []interface{}
}

// 任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

func BuildResponse(errno int,msg string,data interface{}) (reps []byte,err error) {
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	reps ,err = json.Marshal(response)

	return
}

func UnpackJob(value []byte)(job *Job,err error)  {
	var (
		ret *Job
	)

	ret = &Job{}
	if err = json.Unmarshal(value,ret);err !=nil{
		return
	}
	job = ret
	return
}

// 任务变化事件有2种：1）更新任务 2）删除任务
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job: job,
	}
}

func ExtractJobName(jobKey string) string  {
	return strings.TrimPrefix(jobKey,JOB_SAVE_DIR)
}

func ExtractKillerName(jobKey string) string  {
	return strings.TrimPrefix(jobKey,JOB_KILLER_DIR)
}

func BuildJobSchedulePlan(job *Job) (jobSchedulerPlan *JobSchedulerPlan,err error){
	var (
		expr *cronexpr.Expression
	)
	if expr,err = cronexpr.Parse(job.CronExpr);err != nil{
		return
	}

	//生成调取计划
	jobSchedulerPlan = &JobSchedulerPlan{
		Job: job,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

func BuildJobExecuteInfo(jobSchedulerPlan *JobSchedulerPlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job: jobSchedulerPlan.Job,
		PlanTime: jobSchedulerPlan.NextTime,
		RealTime: time.Now(),
	}
	jobExecuteInfo.CancelCxt,jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

// 提取worker的IP
func ExtractWorkerIP(regKey string) (string) {
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}