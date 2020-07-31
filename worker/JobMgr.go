package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"owenliang/crontab/common"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}
var (
	G_jobMgr *JobMgr
)

func (jobMgr *JobMgr)watchJobs()(err error)  {
	var (
		getResponse        *clientv3.GetResponse
		kvPair             *mvccpb.KeyValue
		job                *common.Job
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResponse      clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobEvent           *common.JobEvent
		jobName            string
	)
	//1.get一下 /cron/jobs/目录下的所有任务 并且获知当前集群
	if getResponse,err = jobMgr.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix());err != nil{
		return
	}

	for _, kvPair = range getResponse.Kvs {
		if job,err = common.UnpackJob(kvPair.Value);err == nil{
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
			//fmt.Println("要加入任务计划表了")
			G_scheduler.PushJobEvent(jobEvent)
		}
	}
	go func() {
		watchStartRevision = getResponse.Header.Revision + 1
		//启动监听
		watchChan = jobMgr.watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision),clientv3.WithPrefix())
		//处理监听事件
		for watchResponse = range watchChan {
			for _,watchEvent = range watchResponse.Events {

				switch watchEvent.Type {
				case mvccpb.PUT://保存任务事件
					if job,err = common.UnpackJob(watchEvent.Kv.Value);err != nil{
						continue
					}
					//推一个更新时间给scheuler
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
				case mvccpb.DELETE://任务删除
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					job = &common.Job{
						Name: jobName,
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE,job)
					//推一个删除事件给 scheduler
				}
				//推给 调度协程
				//fmt.Println(jobEvent)
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

func(jobMgr *JobMgr)watchKiller(){
	var (
		watchChan clientv3.WatchChan
		watchResponse clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *common.JobEvent
		jobName string
	)

	go func() {
		watchChan = jobMgr.watcher.Watch(context.TODO(),common.JOB_KILLER_DIR,clientv3.WithPrefix())
		for watchResponse = range watchChan {
			for _,watchEvent = range watchResponse.Events {

				switch watchEvent.Type {
				case mvccpb.PUT: //保存任务事件
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL,&common.Job{Name: jobName})
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: //任务删除

				}
			}
		}
	}()
}

func InitJobMgr() (err error) {

	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
	)
	//fmt.Println("aaa")
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	if client,err = clientv3.New(config);err != nil{
		return
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)
	G_jobMgr = &JobMgr{
		client: client,
		kv: kv,
		lease: lease,
		watcher: watcher,
	}
	//启动监听
	G_jobMgr.watchJobs()

	//监听强杀
	G_jobMgr.watchKiller()
	return
}

//创建任务行锁
func (jobMgr *JobMgr)CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = InitJobLock(jobName,jobMgr.kv,jobMgr.lease)
	return
}
