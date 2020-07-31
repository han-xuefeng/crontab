package common

const (
	JOB_SAVE_DIR = "/cron/jobs/"
	JOB_KILLER_DIR = "/cron/killer/"
	JOB_LOCK_DIR = "/cron/lock/"
	// 保存任务事件
	JOB_EVENT_SAVE = 1

	// 服务注册目录
	JOB_WORKER_DIR = "/cron/workers/"

	// 删除任务事件
	JOB_EVENT_DELETE = 2

	JOB_EVENT_KILL = 3
)