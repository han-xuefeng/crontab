package master

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/han-xuefeng/crontab/common"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
	)

	config = clientv3.Config{
		Endpoints:            G_config.EtcdEndpoints,
		DialTimeout:          time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}
	if client,err = clientv3.New(config);err != nil{
		return err
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	G_jobMgr = &JobMgr{
		kv: kv,
		client: client,
		lease: lease,
	}
	return
}

func (jobMgr *JobMgr)SaveJob(job *common.Job) (oldJob *common.Job,err error) {
	var (
		jobKey string
		jobValue  []byte
		putReps *clientv3.PutResponse
		oldJobObj common.Job
	)
	jobKey = common.JOB_SAVE_DIR+job.Name
	if jobValue,err = json.Marshal(job);err != nil{
		return
	}
	if putReps,err = jobMgr.kv.Put(context.TODO(),jobKey,string(jobValue),clientv3.WithPrevKV());err != nil{
		return
	}
	if putReps.PrevKv != nil{
		if err = json.Unmarshal(putReps.PrevKv.Value,&oldJobObj);err != nil{
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

func (jobMgr *JobMgr)DeleteJob(name string) (oldJob *common.Job,err error) {
	var (
		jobKey string
		deleteResponse *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	jobKey = common.JOB_SAVE_DIR+name

	if deleteResponse,err = jobMgr.kv.Delete(context.TODO(),jobKey,clientv3.WithPrevKV());err != nil{

		return
	}
	if deleteResponse.PrevKvs != nil{
		if err = json.Unmarshal(deleteResponse.PrevKvs[0].Value,&oldJobObj);err !=nil{
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

func (jobMgr *JobMgr)ListJob()(list []*common.Job,err error) {
	var (
		getResponse *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job *common.Job
		dirKey string
	)
	dirKey = common.JOB_SAVE_DIR
	if getResponse,err = jobMgr.kv.Get(context.TODO(),dirKey,clientv3.WithPrefix());err != nil{
		return
	}
	list = make([]*common.Job,0)
	if getResponse.Kvs !=nil{
		for _, kvPair = range getResponse.Kvs {
			job = &common.Job{}
			if err = json.Unmarshal(kvPair.Value,job);err != nil{
				err = nil
				continue
			}
			list = append(list, job)
		}
	}
	return
}

func (jobMgr *JobMgr)KillJob(name string)(err error)  {
	var (
		killerKey string
		leaseGrantResponse *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)
	killerKey = common.JOB_KILLER_DIR + name
	if leaseGrantResponse ,err = jobMgr.lease.Grant(context.TODO(),1);err != nil{
		return
	}
	leaseId = leaseGrantResponse.ID
	if _,err = jobMgr.kv.Put(context.TODO(),killerKey,"",clientv3.WithLease(leaseId));err != nil{
		return
	}
	return
}
