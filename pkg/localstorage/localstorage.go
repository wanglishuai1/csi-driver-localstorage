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

package localstorage

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	v1core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	kubecache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	localstoragev1 "github.com/caoyingjunz/csi-driver-localstorage/pkg/apis/localstorage/v1"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/cache"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/client/clientset/versioned"
	v1 "github.com/caoyingjunz/csi-driver-localstorage/pkg/client/informers/externalversions/localstorage/v1"
	localstorage "github.com/caoyingjunz/csi-driver-localstorage/pkg/client/listers/localstorage/v1"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/util"
	storageutil "github.com/caoyingjunz/csi-driver-localstorage/pkg/util/storage"
)

const (
	DefaultDriverName = "localstorage.csi.caoyingjunz.io"
	StoreFile         = "localstorage.json"

	annNodeSize = "volume.caoyingjunz.io/node-size"
	maxRetries  = 15
)

type localStorage struct {
	config Config      // 配置信息
	cache  cache.Cache // 数据缓存

	lock sync.Mutex // 互斥锁

	client     versioned.Interface  //自定义的clientset
	kubeClient kubernetes.Interface //k8s的clientset

	lsLister       localstorage.LocalStorageLister // 本地存储的Lister
	lsListerSynced kubecache.InformerSynced        // 本地存储的Informer

	queue workqueue.RateLimitingInterface // 任务队列
}

type Config struct {
	DriverName    string // 驱动名称
	Endpoint      string // CSI端点
	VendorVersion string // 驱动版本
	NodeId        string // 节点ID
	// Deprecated: 临时使用，后续删除
	VolumeDir string // 本地存储的目录
}

// NewLocalStorage 创建本地存储
func NewLocalStorage(ctx context.Context, cfg Config, lsInformer v1.LocalStorageInformer, lsClientSet versioned.Interface, kubeClientSet kubernetes.Interface) (*localStorage, error) {
	if cfg.DriverName == "" {
		return nil, fmt.Errorf("no driver name provided")
	}
	if len(cfg.NodeId) == 0 {
		return nil, fmt.Errorf("no node id provided")
	}
	klog.V(2).Infof("Driver: %v version: %v, nodeId: %v", cfg.DriverName, cfg.VendorVersion, cfg.NodeId)
	// 创建本地存储的目录
	if err := makeVolumeDir(cfg.VolumeDir); err != nil {
		return nil, err
	}
	storeFile := path.Join(cfg.VolumeDir, StoreFile)
	klog.V(2).Infof("localstorage will be store in %s", storeFile)
	// 创建缓存
	s, err := cache.New(storeFile)
	if err != nil {
		return nil, err
	}
	// 创建localstorage
	ls, err := &localStorage{
		config:     cfg,
		cache:      s,
		kubeClient: kubeClientSet,
		client:     lsClientSet,
		queue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "plugin"), // 创建带有速率的任务队列，且使用默认的速率限制器，名称为plugin
	}, nil
	if err != nil {
		return nil, err
	}
	// 添加事件处理函数
	lsInformer.Informer().AddEventHandler(kubecache.FilteringResourceEventHandler{
		Handler: kubecache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ls.addStorage(obj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				ls.updateStorage(oldObj, newObj)
			},
		},
		// 对接收到的对象进行过滤
		FilterFunc: func(obj interface{}) bool {
			switch t := obj.(type) {
			case *localstoragev1.LocalStorage: // 本地存储
				return util.AssignedLocalstorage(t, cfg.NodeId) // 判断是否是本地存储
			default:
				klog.Infof("handle object error")
				return false
			}
		},
	})
	// 设置lsLister和lsListerSynced
	ls.lsLister = lsInformer.Lister()
	/*
		提供了一种从本地缓存中获取k8s对象的方式，lister用于从本次存储(即索引器)中获取和列举对象
		Lister 提供了一些方法，比如 List、Get、以及针对不同的 Kubernetes 对象类型提供的特定方法
		（例如，对于 Pod，会有 Pods(namespace).Get(name) 方法），让我们可以在本地快速访问和搜索这些对象。
		这比直接调用 Kubernetes API 服务器要快很多，因为所有的数据都在本地内存中，没有网络延迟

		Lister 使用的数据源是 Informer 的本地缓存，这个缓存会被 Informer 自动更新，
		以保持与 Kubernetes API 服务器的数据同步。
	*/

	ls.lsListerSynced = lsInformer.Informer().HasSynced
	/*
		是informer接口的方法，返回一个bool值，表示是否已经完成了与 Kubernetes API 服务器同步
		当你创建一个 Informer 并启动它时，Informer 会从 API 服务器获取所有符合条件的对象，并保存到本地的缓存中。
		这个过程被称为 "初始同步" 或 "初始列举"。
		当初始同步完成后，Informer 会监听 API 服务器的变化，并将变化的对象更新到本地缓存中。
		HasSynced 方法就是用来检查这个初始同步是否已经完成。
		如果 HasSynced 返回 true，说明 Informer 的本地缓存已经准备好，你可以从缓存中读取对象了。
		如果 HasSynced 返回 false，说明初始同步还没有完成，此时如果你尝试从 Informer 的本地缓存中读取对象，
		可能会得到不完整或者过时的数据

	*/

	return ls, nil
}

// Run 方法启动了一个循环，不断地处理来自任务队列的任务，并且在 gRPC 服务器上提供服务
func (ls *localStorage) Run(ctx context.Context) error {
	defer utilruntime.HandleCrash() // 处理崩溃,程序崩溃时恢复运行，并打印相关的错误日志
	defer ls.queue.ShutDown()       // 关闭任务队列

	// 启动informer
	//这个函数会等待所有的 informer 的缓存都同步完成。
	//如果在这个过程中 context 被取消（例如，接收到终止信号），
	//该函数会返回 false，从而使 Run 方法返回一个错误
	if !kubecache.WaitForNamedCacheSync("localstorage-plugin", ctx.Done(), ls.lsListerSynced) {
		return fmt.Errorf("failed to WaitForNamedCacheSync")
	}
	// 启动worker,worker会从任务队列中获取任务并执行,直到任务队列关闭,或者context被取消,周期为1s
	go wait.UntilWithContext(ctx, ls.worker, time.Second)
	// 启动grpc server
	s := NewNonBlockingGRPCServer()
	//启动了 gRPC 服务器，使其开始监听特定的端点，并提供服务。
	//这里的服务都是通过 localStorage 结构的方法提供的
	s.Start(ls.config.Endpoint, ls, ls, ls)
	//阻塞，直到gRPC服务器停止
	s.Wait()

	return nil
}

func (ls *localStorage) sync(ctx context.Context, dKey string) error {
	// 加锁
	ls.lock.Lock()
	defer ls.lock.Unlock()
	//计算耗时
	startTime := time.Now()
	klog.V(2).InfoS("Started syncing localstorage plugin", "localstorage", "startTime", startTime)
	defer func() {
		klog.V(2).InfoS("Finished syncing localstorage plugin", "localstorage", "duration", time.Since(startTime))
	}()
	//获取localstorage
	localstorage, err := ls.lsLister.Get(dKey)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(2).Infof("localstorage has been deleted", dKey)
			return nil
		}
		return err
	}

	// Deep copy otherwise we are mutating the cache.
	//通过 DeepCopy 方法，我们可以创建一个对象的深拷贝，这样就不会影响到缓存中的对象
	l := localstorage.DeepCopy()
	nodeSize, ok := l.Annotations[annNodeSize] // 获取node size
	if !ok {
		return fmt.Errorf("failed to found node localstorage size")
	}
	klog.Infof("get node size %s from annotations", nodeSize)
	quantity, err := resource.ParseQuantity(nodeSize) // 转化为resource.Quantity对象
	if err != nil {
		return fmt.Errorf("failed to parse node quantity: %v", err)
	}

	l.Status.Capacity = &quantity                     // 设置localstorage的容量
	l.Status.Allocatable = &quantity                  // 设置localstorage的可用容量
	l.Status.Phase = localstoragev1.LocalStorageReady // 设置localstorage的状态为ready，然后更新localstorage
	_, err = ls.client.StorageV1().LocalStorages().Update(ctx, l, metav1.UpdateOptions{})
	return err
}

// 更新localstorage到队列
func (ls *localStorage) updateStorage(old, cur interface{}) {
	oldLs := old.(*localstoragev1.LocalStorage)
	curLs := cur.(*localstoragev1.LocalStorage)
	klog.V(2).Info("Updating localstorage", "localstorage", klog.KObj(oldLs))

	ls.enqueue(curLs)
}

// 增加localstorage到队列；
func (ls *localStorage) addStorage(obj interface{}) {
	localstorage := obj.(*localstoragev1.LocalStorage)
	klog.V(2).Info("Adding localstorage", "localstorage", klog.KObj(localstorage))
	ls.enqueue(localstorage)
}

/*
	典型的 worker goroutine 模式，常见于 Kubernetes 控制器中
	worker 是一个无限循环，一直调用 processNextWorkItem 方法，直到它返回 false，这通常表示工作队列已被关闭
	----------------
	总的来说，这些代码实现了一个 worker goroutine，用于处理从队列中获取的工作项，并处理可能出现的错误。
	这是处理 Kubernetes 资源事件的典型模式，可以保证在多个 goroutine 中处理事件的顺序，
	并通过 rate-limiting 队列防止对单个事件的过度重试。
*/

func (ls *localStorage) worker(ctx context.Context) {
	for ls.processNextWorkItem(ctx) {
	}
}

/*
实际工作的就是这个函数
它从队列中获取一个工作项，如果队列关了（quit为ture），就返回false
*/
func (ls *localStorage) processNextWorkItem(ctx context.Context) bool {
	key, quit := ls.queue.Get()
	if quit {
		return false
	}
	defer ls.queue.Done(key) //

	ls.handleErr(ctx, ls.sync(ctx, key.(string)), key)
	return true
}

// 处理错误
func (ls *localStorage) handleErr(ctx context.Context, err error, key interface{}) {
	// 如果没有错误，或者错误是因为namespace正在被删除，就Forget,Forget表示一个项目已经完成，不再重试
	if err == nil || errors.HasStatusCause(err, v1core.NamespaceTerminatingCause) {
		ls.queue.Forget(key)
		return
	}
	//如果错误存在，则解析键值，键值通常是"namespace/name"形式的字符串，如果键值解析失败，则记录错误
	ns, name, keyErr := kubecache.SplitMetaNamespaceKey(key.(string))
	if keyErr != nil {
		klog.Error(err, "Failed to split meta namespace cache key", "cacheKey", key)
	}
	//检查key的错误重试次数，如果重试次数小于最大允许的重试次数，则使用s.queue.AddRateLimited(key)将该项目重新加入到队列中
	if ls.queue.NumRequeues(key) < maxRetries {
		klog.V(2).Info("Error syncing localstorage", "localstorage", klog.KRef(ns, name), "err", err)
		ls.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	klog.V(2).Info("Dropping localstorage out of the queue", "localstorage", klog.KRef(ns, name), "err", err)
	ls.queue.Forget(key) //Forget表示一个项目已经完成
}

// 将localstorage加入到队列中
func (ls *localStorage) enqueue(s *localstoragev1.LocalStorage) {
	key, err := util.KeyFunc(s)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", ls, err))
		return
	}
	//将key加入到队列中
	ls.queue.Add(key)
}

// 获取node
func (ls *localStorage) GetNode() string {
	return ls.config.NodeId
}

func (ls *localStorage) TryUpdateNode() error {
	nodeClient := ls.kubeClient.CoreV1().Nodes()
	originalNode, err := nodeClient.Get(context.TODO(), ls.GetNode(), metav1.GetOptions{})
	if err != nil {
		return err
	}

	node := originalNode.DeepCopy()
	if storageutil.IsNodeIDInNode(node) {
		return nil
	}

	node = storageutil.UpdateNodeIDInNode(node, ls.GetNode())
	_, err = nodeClient.Update(context.TODO(), node, metav1.UpdateOptions{})

	// TODO: optimised by patch
	//patchBytes := []byte("ddd")
	//updatedNode, err := nodeClient.Patch(context.TODO(), ls.GetNode(), types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	return err
}
