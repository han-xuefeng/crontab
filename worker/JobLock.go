package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/han-xuefeng/crontab/common"
)

type JobLock struct {
	kv clientv3.KV
	lease clientv3.Lease
	jobName string
	cancelFunc context.CancelFunc
	leaseId clientv3.LeaseID
	isLocked bool
}

func InitJobLock(jobName string,kv clientv3.KV,lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv: kv,
		lease: lease,
		jobName: jobName,
	}
	return
}

//抢锁
func (jobLock *JobLock)TryLock()(err error)  {
	var (
		leaseGrantResponse         *clientv3.LeaseGrantResponse
		leaseId                    clientv3.LeaseID
		leaseKeepAliveResponseChan <-chan *clientv3.LeaseKeepAliveResponse
		cancelCtx                  context.Context
		cancelFunc                 context.CancelFunc
		leaseKeepAliveResponse     *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResponse *clientv3.TxnResponse
	)
	//创建租约

	if leaseGrantResponse,err = jobLock.lease.Grant(context.TODO(),5);err != nil{
		return
	}

	//自动续租
	leaseId = leaseGrantResponse.ID
	cancelCtx,cancelFunc = context.WithCancel(context.TODO())
	if leaseKeepAliveResponseChan,err = jobLock.lease.KeepAlive(cancelCtx,leaseId);err != nil{
		goto FAIL
	}

	//处理自动续约协程
	go func() {
		for{
			select {
			case leaseKeepAliveResponse = <-leaseKeepAliveResponseChan:
				if leaseKeepAliveResponse == nil{
					goto END
				}
			}
		}
		END:
	}()

	//事务抢锁
	txn = jobLock.kv.Txn(context.TODO())

	lockKey = common.JOB_LOCK_DIR+jobLock.jobName
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey),"=",0)).
		Then(clientv3.OpPut(lockKey,"",clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))
	//提交事务
	if txnResponse,err = txn.Commit();err != nil{
		goto FAIL
	}
	if !txnResponse.Succeeded{
		err = common.ERR_LOCK_ALREDY_REQUIRED
		goto FAIL
	}
	jobLock.leaseId = leaseId
	jobLock.cancelFunc =cancelFunc
	jobLock.isLocked = true
	return
	//返回
FAIL:
	cancelFunc()
	jobLock.lease.Revoke(context.TODO(),leaseId)
	return
}

func (jobLock *JobLock)UnLock()  {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId)
	}
}
