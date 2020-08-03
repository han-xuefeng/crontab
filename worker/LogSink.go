package worker

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/han-xuefeng/crontab/common"
	"time"
)

type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

func (logSink *LogSink)saveLogs(logBatch *common.LogBatch){
	//fmt.Println("写日志哇",logBatch)
	logSink.logCollection.InsertMany(context.TODO(),logBatch.Logs)
}

func (logSink *LogSink)writeLoop(){
	var (
		jobLog *common.JobLog
		logBatch *common.LogBatch
		commitTimer *time.Timer
		timeBatch *common.LogBatch

	)

	for {
		select {
		case jobLog = <-logSink.logChan:
			//写mongo
			if logBatch == nil{
				logBatch = &common.LogBatch{}
				//让批次超时自动提交
				commitTimer = time.AfterFunc(
					time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond, func(batch *common.LogBatch)func(){
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)

			}
			logBatch.Logs = append(logBatch.Logs, jobLog)
			if len(logBatch.Logs) >= G_config.JobLogBatchSize{
				logSink.saveLogs(logBatch)
				logBatch = nil
				commitTimer.Stop() //提交了  取消定时器
			}
		case timeBatch = <-logSink.autoCommitChan:
			//写到mongo
			if timeBatch != logBatch{
				continue
			}
			logSink.saveLogs(timeBatch)
			logBatch = nil
		}
	}
}

func InitLogSink()(err error) {
	var (
		client *mongo.Client
		logCollection *mongo.Collection
	)
	if client,err = mongo.Connect(context.TODO(),options.Client().ApplyURI(G_config.MongodbUri));err != nil{
		//fmt.Println(err)
		return
	}
	//fmt.Println("$$")
	logCollection = client.Database("cron").Collection("log")
	G_logSink = &LogSink{
		client: client,
		logCollection: logCollection,
		logChan: make(chan *common.JobLog,1000),
		autoCommitChan: make(chan *common.LogBatch,1000),
	}

	go G_logSink.writeLoop()
	return
}

func (logSink *LogSink)Append(jobLog *common.JobLog)  {
	select {
	case logSink.logChan <- jobLog:
	default:
		//队列满了就丢弃了
	}
}
