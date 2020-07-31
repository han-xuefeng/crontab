package master

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"owenliang/crontab/common"
)

type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)


func InitLogMgr()(err error) {
	var (
		client *mongo.Client
		logCollection *mongo.Collection
	)
	if client,err = mongo.Connect(context.TODO(),options.Client().ApplyURI(G_config.MongodbUri));err != nil{
		return
	}
	logCollection = client.Database("cron").Collection("log")
	G_logMgr = &LogMgr{
		client: client,
		logCollection: logCollection,
	}

	return
}

func (logMgr *LogMgr)List(name string,skip int,limit int)(logArr []*common.JobLog,err error){

	var (
		filter *common.JobLogFilter
		cursor *mongo.Cursor
		jobLog *common.JobLog

	)
	filter = &common.JobLogFilter{
		JobName: name,
	}

	logArr = make([]*common.JobLog,0)

	if cursor ,err = logMgr.logCollection.Find(context.TODO(),filter,options.Find().SetLimit(int64(limit)).SetSkip(int64(skip)).SetSort(bson.D{{"endTime", -1}}));err != nil{
		return 
	}
	defer cursor.Close(context.TODO()) //释放游标

	for cursor.Next(context.TODO()){
		jobLog = &common.JobLog{}
		if err = cursor.Decode(jobLog);err != nil{
			continue
		}
		logArr = append(logArr, jobLog)
	}
	return
}

func (logMgr *LogMgr)Count(name string)(count int64,err error)  {
	filter := &common.JobLogFilter{
		JobName: name,
	}
	count ,err = logMgr.logCollection.CountDocuments(context.TODO(),filter)
	return
}

