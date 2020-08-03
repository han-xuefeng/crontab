package worker

import (
	"math/rand"
	"os/exec"
	"github.com/han-xuefeng/crontab/common"
	"time"
)

//执行类
type Executor struct {
	
}

var (
	G_executor *Executor
)

func (executor *Executor)ExecuteJob(info *common.JobExecuteInfo)  {
	//协程执行
	go func() {
		var (
			cmd *exec.Cmd
			err error
			output []byte
			result *common.JobExecuteResult
			jobLock *JobLock
		)


		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte,0),
		}
		result.StartTime = time.Now()
		//初始化锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)
		time.Sleep(time.Duration(rand.Intn(1000))*time.Millisecond)
		err = jobLock.TryLock()
		defer jobLock.UnLock()
		if err != nil{ //上锁失败
			result.Err = err
			result.EndTime = time.Now()
		}else{
			result.StartTime = time.Now()
			cmd = exec.CommandContext(info.CancelCxt,G_config.BinBash,"-c",info.Job.Command)
			output,err = cmd.CombinedOutput()
			result.Output = output
			result.Err = err
			result.EndTime = time.Now()
		}
		G_scheduler.PushJobExecuteResult(result)
	}()
}

//初始化
func InitExecutor() (err error) {
	G_executor = &Executor{

	}
	return
}
