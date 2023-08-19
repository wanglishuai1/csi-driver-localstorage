/*
Copyright 2021 The Caoyingjunz Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/caoyingjunz/csi-driver-localstorage/pkg/client/clientset/versioned"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/client/informers/externalversions"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/controller/storage"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/runtime"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/signals"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/util"
	localstoragewebhook "github.com/caoyingjunz/csi-driver-localstorage/pkg/webhook"
)

const (
	workers = 5

	LeaseDuration = 15
	RenewDeadline = 10
	RetryPeriod   = 2

	ResourceLock      = "endpointsleases"
	ResourceName      = "localstorage-manager"
	ResourceNamespace = "kube-system"
)

var (
	kubeconfig   = flag.String("kubeconfig", "", "paths to a kubeconfig. Only required if out-of-cluster.")
	kubeAPIQPS   = flag.Int("kube-api-qps", 5, "QPS to use while communicating with the kubernetes apiserver. Defaults to 5")
	kubeAPIBurst = flag.Int("kube-api-burst", 10, "Burst to use while communicating with the kubernetes apiserver. Defaults to 10.")

	// webhook flags
	host     = flag.String("host", "", "host is the ip address that the webhook server binds to")
	port     = flag.Int("port", 8443, "port is the port that the webhook server serves at")
	certDir  = flag.String("cert-dir", "/tmp/webhook-server", "certDir is the directory that contains the server key and certificate")
	certName = flag.String("cert-name", "tls.crt", "certName is the server certificate name. Defaults to tls.crt")
	keyName  = flag.String("key-name", "tls.key", "keyName is the server key name. Defaults to tls.key.")

	// health flag
	healthzPort = flag.Int("healthz-port", 0, "healthzPort is the port of the localhost healthz endpoint (set to 0 to disable)")

	// leaderElect
	leaderElect       = flag.Bool("leader-elect", true, "Start a leader election client and gain leadership before executing the main loop. Enable this when running replicated components for high availability.")
	retryPeriod       = flag.Int("leader-elect-retry-period", RetryPeriod, "The duration the clients should wait between attempting acquisition and renewal of a leadership.")
	resourceLock      = flag.String("leader-elect-resource-lock", ResourceLock, "The type of resource object that is used for locking during leader election. Supported options are `endpoints` (default) and `configmaps`.")
	resourceName      = flag.String("leader-elect-resource-name", ResourceName, "The name of resource object that is used for locking during leader election.")
	resourceNamespace = flag.String("leader-elect-resource-namespace", ResourceNamespace, "The namespace of resource object that is used for locking during leader election.")
	leaseDuration     = flag.Int("leader-elect-lease-duration", LeaseDuration, "The duration that non-leader candidates will wait")
	renewDeadline     = flag.Int("leader-elect-renew-deadline", RenewDeadline, "The interval between attempts by the acting master to renew a leadership slot before it stops leading.")
)

// 用来设置日志参数
func init() {
	_ = flag.Set("logtostderr", "true")
}

func main() {
	//解析了命令行参数
	klog.InitFlags(nil)
	flag.Parse()

	// set up signals so we handle the shutdown signal gracefully(设置信号，以便我们优雅地处理关机信号)
	//设置了信号处理器用来处理终止信号
	ctx := signals.SetupSignalHandler()
	//创建了 kubeconfig 客户端，并使用它来创建 webhookManager。
	kubeConfig, err := util.BuildClientConfig(*kubeconfig)
	if err != nil {
		klog.Fatalf("Failed to build kube config: %v", err)
	}
	kubeConfig.QPS = float32(*kubeAPIQPS) //设置了 QPS 和 Burst 参数
	kubeConfig.Burst = *kubeAPIBurst
	//webhookManager 是 controller-runtime 包的一个组件，用于管理和运行 Webhook 服务器
	webhookManager, err := manager.New(kubeConfig, manager.Options{
		Scheme: runtime.NewScheme(),
		Host:   *host,
		Port:   *port,
	})
	if err != nil {
		klog.Fatalf("unable to set up overall controller manager: %v", err) //设置总控制器管理器失败
	}
	//安装证书
	installCert(webhookManager.GetWebhookServer())

	// Build client to perform kubernetes objects.(构建webhook客户端以执行kubernetes对象。)
	webhookClient := webhookManager.GetClient()

	// Register webhook APIs(注册webhook API)
	klog.Info("Registering webhooks for localstorage APIs")
	//注册webhook API
	webhookManager.GetWebhookServer().Register("/mutate-v1-localstorage", &webhook.Admission{Handler: &localstoragewebhook.LocalstorageMutate{Client: webhookClient}})      //mutate 用于修改请求
	webhookManager.GetWebhookServer().Register("/validate-v1-localstorage", &webhook.Admission{Handler: &localstoragewebhook.LocalstorageValidator{Client: webhookClient}}) // validate 用于验证请求
	go func() {
		klog.Infof("Starting localstorage webhook server")
		if err = webhookManager.Start(ctx); err != nil {
			klog.Fatalf("failed to start localstorage webhook server: %v", err)
		}
	}()
	//创建kubernetes Client
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		klog.Fatalf("Failed to build kube clientSet: %v", err)
	}
	run := func(ctx context.Context) {
		//创建了一个新的 localstorage clientSet（通常CRD创建的client称之为clientSet）
		lsClientSet, err := versioned.NewForConfig(kubeConfig)
		if err != nil {
			klog.Fatalf("Failed to new localstorage clientSet: %v", err)
		}
		sharedInformer := externalversions.NewSharedInformerFactory(lsClientSet, 300*time.Second)
		//创建了 storageController
		sc, err := storage.NewStorageController(ctx,
			sharedInformer.Storage().V1().LocalStorages(),
			lsClientSet,
			kubeClient,
		)
		if err != nil {
			klog.Fatalf("Failed to new storage controller: %s", err)
		}

		klog.Infof("Starting localstorage controller")
		//启动了 storageController
		go sc.Run(ctx, workers)
		//启动了 sharedInformer监听k8s资源变化
		sharedInformer.Start(ctx.Done())
		sharedInformer.WaitForCacheSync(ctx.Done()) //等待缓存同步

		// always wait
		select {}
	}
	//如果设置了健康检查端口，就启动一个健康检查服务器
	if *healthzPort > 0 {
		mux := http.NewServeMux()
		healthz.InstallHandler(mux)
		go wait.Until(func() {
			err = http.ListenAndServe(net.JoinHostPort("", strconv.Itoa(*healthzPort)), mux)
			if err != nil {
				klog.ErrorS(err, "Failed to start healthz server")
			}
		}, 5*time.Second, wait.NeverStop)
	}
	//如果没有设置leader选举，就直接运行
	if !*leaderElect {
		run(ctx)
		klog.Fatalf("unreachable")
	}

	id, err := os.Hostname()
	if err != nil {
		klog.Fatalf("Failed to get hostname: %v", err)
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id = id + "_" + string(uuid.NewUUID())
	//创建了一个新的资源锁
	rl, err := resourcelock.New(
		*resourceLock,
		*resourceNamespace,
		*resourceName,
		kubeClient.CoreV1(),
		kubeClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: util.CreateRecorder(kubeClient),
		})
	if err != nil {
		klog.Fatalf("error creating lock: %v", err)
	}
	//创建了一个新的leader选举
	leaderelection.RunOrDie(context.TODO(), leaderelection.LeaderElectionConfig{
		Lock:          rl,                                          //资源锁
		LeaseDuration: time.Duration(*leaseDuration) * time.Second, //租约时间
		RenewDeadline: time.Duration(*renewDeadline) * time.Second, //续约时间
		RetryPeriod:   time.Duration(*retryPeriod) * time.Second,   //重试时间
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: run, //如果成为leader，就运行run函数
			OnStoppedLeading: func() { //如果失去leader，就退出程序
				klog.Fatalf("leader election lost")
			},
		},
		//WatchDog: electionChecker,
		Name: "localstorage-manager", //leader选举的名称
	})

	klog.Fatalf("unreachable") //不可达
}

// 用于在 Webhook 服务器上安装 TLS 证书和密钥
func installCert(s *webhook.Server) {
	s.CertDir = *certDir
	s.CertName = *certName
	s.KeyName = *keyName
}
