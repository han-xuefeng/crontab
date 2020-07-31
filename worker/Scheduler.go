package worker

import (
	"fmt"
	"owenliang/crontab/common"
	"time"
)

type Scheduler struct {
	jobEventChan chan *common.JobEvent //etcd事件
	jobPlanTable map[string]*common.JobSchedulerPlan //任务调度计划表
	jobExecutingTable map[string] *common.JobExecuteInfo
	jobResultChan chan *common.JobExecuteResult //任务执行队列
}

var (
	G_scheduler *Scheduler
)



//处理任务
func (scheduler *Scheduler)handleJobEvent(jobEvent *common.JobEvent){
	var (
		jobSchedulePlan *common.JobSchedulerPlan
		//jobExisted bool
		err error
		jobExecuteInfo *common.JobExecuteInfo
		jobExisted bool
	)
	fmt.Println(jobEvent)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		if jobSchedulePlan,err = common.BuildJobSchedulePlan(jobEvent.Job);err != nil{
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE:
		if jobSchedulePlan,jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name];jobExisted {
			delete(scheduler.jobPlanTable,jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		//判断任务是否在执行  如果在执行  杀死

		if jobExecuteInfo,jobExisted = scheduler.jobExecutingTable[jobEvent.Job.Name];jobExisted{
			//杀死了
			jobExecuteInfo.CancelFunc()
		}
		
	}
}

func (scheduler *Scheduler)TryStartJob(jobPlan *common.JobSchedulerPlan)  {
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)
	if jobExecuteInfo,jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name];jobExecuting{
		//任务在执行
		fmt.Println("执行尚无退出",jobPlan.Job.Name)
		return
	}
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	//执行任务
	G_executor.ExecuteJob(jobExecuteInfo)
	return
}

//重新计算任务调度状态
func (scheduler *Scheduler)TyeSchedule()(scheduleAfter time.Duration)  {
	var (
		jobPlan *common.JobSchedulerPlan
		now time.Time
		nearTime *time.Time
	)

	if len(scheduler.jobPlanTable) == 0{
		scheduleAfter = 1 *time.Second
		return
	}

	now = time.Now()
	//1.遍历多少有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now){
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now)
		}

		//统计最近一个要到期的时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime){
			nearTime = &jobPlan.NextTime
		}
	}
	//下次调度间隔
	scheduleAfter = (*nearTime).Sub(now)
	return
}

func (scheduler *Scheduler)handleJobExecuteResult(jobExecuteResult *common.JobExecuteResult)  {
	delete(scheduler.jobExecutingTable,jobExecuteResult.ExecuteInfo.Job.Name)
	//if jobExecuteResult.Err != nil{
	//	fmt.Println(jobExecuteResult.Err,jobExecuteResult.ExecuteInfo.Job.Name)
	//}

	var (
		jobLog *common.JobLog
	)

	fmt.Println("任务执行完成",string(jobExecuteResult.Output),jobExecuteResult.Err)
	if jobExecuteResult.Err != common.ERR_LOCK_ALREDY_REQUIRED{
		jobLog = &common.JobLog{
			JobName:      jobExecuteResult.ExecuteInfo.Job.Name,
			Command:      jobExecuteResult.ExecuteInfo.Job.Command,
			Output:       string(jobExecuteResult.Output),
			PlanTime:     jobExecuteResult.ExecuteInfo.PlanTime.UnixNano()/1000/1000,
			ScheduleTime: jobExecuteResult.ExecuteInfo.RealTime.UnixNano()/1000/1000,
			StartTime:    jobExecuteResult.StartTime.UnixNano()/1000/1000,
			EndTime:      jobExecuteResult.EndTime.UnixNano()/1000/1000,
		}
		if jobExecuteResult.Err != nil{
			jobLog.Err = jobExecuteResult.Err.Error()
		}else{
			jobLog.Err = ""
		}

		G_logSink.Append(jobLog)

	}
}

func (scheduler *Scheduler)schedulerLoop()(err error)  {
	//读取管道
	var (
		jobEvent *common.JobEvent
		scheduleAfter time.Duration
		scheduleTime *time.Timer
		jobExecuteResult *common.JobExecuteResult
	)
	scheduleAfter = scheduler.TyeSchedule()
	//调度的延时定时器
	scheduleTime = time.NewTimer(scheduleAfter)
	for{
		select {
		case jobEvent = <-scheduler.jobEventChan:  //监听任务变化事件
			scheduler.handleJobEvent(jobEvent)
		case <- scheduleTime.C: //timeer到了  要执行

		case jobExecuteResult = <-scheduler.jobResultChan: //监听任务执行结果
			scheduler.handleJobExecuteResult(jobExecuteResult)
		}
		scheduleAfter = scheduler.TyeSchedule()
		scheduleTime.Reset(scheduleAfter)
	}
}

//用来接收任务的
func (scheduler *Scheduler)PushJobEvent(jobEvent *common.JobEvent){
	scheduler.jobEventChan <- jobEvent
}

//初始话调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent),
		jobPlanTable: make(map[string]*common.JobSchedulerPlan),
		jobExecutingTable: make(map[string] *common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult),
	}

	//启动协程
	go G_scheduler.schedulerLoop()
	return
}

//回传任务执行结果
func (scheduler *Scheduler)PushJobExecuteResult(jobResult *common.JobExecuteResult)  {
	scheduler.jobResultChan <- jobResult
}
