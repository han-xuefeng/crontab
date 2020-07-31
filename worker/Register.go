package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"net"
	"owenliang/crontab/common"
	"time"
)

type Register struct {

	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	localIp string
}

var (
	G_register *Register
)

// 获取本机网卡IP
func getLocalIP() (ipv4 string, err error) {
	var (
		addrs []net.Addr
		addr net.Addr
		ipNet *net.IPNet // IP地址
		isIpNet bool
	)
	// 获取所有网卡
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}
	// 取第一个非lo的网卡IP
	for _, addr = range addrs {
		// 这个网络地址是IP地址: ipv4, ipv6
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			// 跳过IPV6
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()	// 192.168.1.1
				return
			}
		}
	}

	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}


func(register *Register)keepOnline(){
	var (
		leaseId clientv3.LeaseID
		err error
		leaseGrantResponse *clientv3.LeaseGrantResponse
		//putResponse *clientv3.PutResponse
		leaseKeepAliveResponse <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResponse *clientv3.LeaseKeepAliveResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		regKey string
	)
	for {
		cancelFunc = nil
		regKey = common.JOB_WORKER_DIR + register.localIp
		if leaseGrantResponse ,err = register.lease.Grant(context.TODO(),10);err != nil{
			goto RETRY
		}
		leaseId = leaseGrantResponse.ID

		//自动续租
		if leaseKeepAliveResponse,err = register.lease.KeepAlive(context.TODO(),leaseId);err != nil{
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		//写入
		if _,err = register.kv.Put(cancelCtx,regKey,"",clientv3.WithLease(leaseId));err != nil{
			goto RETRY
		}


		for{
			select {
			case keepAliveResponse = <-leaseKeepAliveResponse:
				//续租成功
				if keepAliveResponse == nil{ //续租失败
					goto RETRY
				}
			}
		}
	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil{
			cancelFunc()
		}
	}


}

func InitRegister()(err error)  {

	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		localIp string
	)

	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout)*time.Millisecond,
	}

	if client,err = clientv3.New(config);err != nil{
		return
	}

	kv = clientv3.NewKV(client)

	lease = clientv3.NewLease(client)

	if localIp,err = getLocalIP();err != nil{
		return
	}

	G_register = &Register{
		client: client,
		kv: kv,
		lease: lease,
		localIp: localIp,
	}
	go G_register.keepOnline()

	return
}
