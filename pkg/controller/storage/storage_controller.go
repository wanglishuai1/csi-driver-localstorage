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

package storage

import (
	"context"
	"fmt"
	"time"

	v1core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	localstoragev1 "github.com/caoyingjunz/csi-driver-localstorage/pkg/apis/localstorage/v1"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/client/clientset/versioned"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/client/informers/externalversions/localstorage/v1"
	localstorage "github.com/caoyingjunz/csi-driver-localstorage/pkg/client/listers/localstorage/v1"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/util"
)

const (
	maxRetries = 15
)

var (
	KeyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc
)

// StorageController 是一个用于处理 LocalStorage 对象的控制器
type StorageController struct {
	client     versioned.Interface  // 用来与自定义的apiGroup进行交互，通常是我们自定义的资源的客户端接口
	kubeClient kubernetes.Interface // Kubernetes 客户端，用于和 Kubernetes API 交互

	eventBroadcaster record.EventBroadcaster // 事件广播器，用于发送事件
	eventRecorder    record.EventRecorder    // 事件记录器，用于记录事件

	syncHandler         func(ctx context.Context, dKey string) error // 同步处理函数，当 LocalStorage 对象有变动时，这个函数会被调用
	enqueueLocalstorage func(ls *localstoragev1.LocalStorage)        // 入队函数，将 LocalStorage 对象放入工作队列中

	lsLister       localstorage.LocalStorageLister // Lister，用于列出当前 Kubernetes 集群中的 LocalStorage 对象
	lsListerSynced cache.InformerSynced            // ListerSynced，用于判断 LocalStorage 对象的缓存是否已经同步

	queue workqueue.RateLimitingInterface // 工作队列，用于存储需要处理的 LocalStorage 对象
}

// NewStorageController creates a new StorageController. 创建一个新的StorageController
func NewStorageController(ctx context.Context, lsInformer v1.LocalStorageInformer, lsClientSet versioned.Interface, kubeClientSet kubernetes.Interface) (*StorageController, error) {
	eventBroadcaster := record.NewBroadcaster() // 创建一个事件广播器
	eventBroadcaster.StartLogging(klog.Infof)   //设置 Event Broadcaster 开始使用 klog 来记录事件。这意味着当事件被广播时，信息将被记录在日志中
	/*
		将事件发送（record）到一个指定的 sink。在这个情况下，这个 sink 是 Kubernetes API Server 中的 Events 接口。
		这意味着当事件被广播时，这些事件也会被发送到 Kubernetes 的事件系统中，可以通过 Kubernetes 的命令行工具或 API 来查看这些事件
	*/
	eventBroadcaster.StartRecordingToSink(&typedv1.EventSinkImpl{Interface: kubeClientSet.CoreV1().Events("")})

	sc := &StorageController{
		// 特指用于访问和操作定制资源（CRD）"localstorage"的客户端
		client: lsClientSet,
		// 通用客户端，用来访问和操作 Kubernetes 中的核心资源，如Pods，Services等
		kubeClient: kubeClientSet,
		// 事件广播器，用于发送事件
		eventBroadcaster: eventBroadcaster,
		// 事件记录器，用于记录事件，事件的来源为 LocalstorageManagerUserAgent
		eventRecorder: eventBroadcaster.NewRecorder(scheme.Scheme, v1core.EventSource{Component: util.LocalstorageManagerUserAgent}),
		// 创建一个工作队列，用于存放需要处理的对象，这个队列使用了默认的控制器速率控制器，用于防止对ApiServer的过度调用，队列的名字为 localstorage
		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "localstorage"),
	}
	//添加事件处理函数
	lsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			sc.addStorage(obj) // 当新的 LocalStorage 对象被添加时，这个函数会被调用
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			sc.updateStorage(oldObj, newObj) // 当 LocalStorage 对象被更新时，这个函数会被调用
		},
		DeleteFunc: func(obj interface{}) {
			sc.deleteStorage(obj) // 当 LocalStorage 对象被删除时，这个函数会被调用
		},
	})

	sc.syncHandler = sc.syncStorage     // 设置同步处理函数
	sc.enqueueLocalstorage = sc.enqueue // 设置入队函数

	sc.lsLister = lsInformer.Lister()                   // 设置 Lister
	sc.lsListerSynced = lsInformer.Informer().HasSynced // 设置判断是否同步的函数
	return sc, nil
}

func (s *StorageController) addStorage(obj interface{}) {
	ls, ok := obj.(*localstoragev1.LocalStorage)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("expected localstorage in addStorage, but got %#v", obj))
		return
	}
	klog.V(2).Info("Adding localstorage", "localstorage", klog.KObj(ls))
	s.enqueueLocalstorage(ls) // 将 LocalStorage 对象放入工作队列中
}

func (s *StorageController) updateStorage(old, cur interface{}) {
	oldLs := old.(*localstoragev1.LocalStorage)
	curLs := cur.(*localstoragev1.LocalStorage)
	klog.V(2).Info("Updating localstorage", "localstorage", klog.KObj(oldLs))

	s.enqueueLocalstorage(curLs) // 将 LocalStorage 对象放入工作队列中
}

func (s *StorageController) deleteStorage(obj interface{}) {
	//首先判断对象是否为一个localstorage对象，如果不是，那么就从tombstone中获取localstorage对象
	ls, ok := obj.(*localstoragev1.LocalStorage)
	if !ok {
		//如果不是，尝试转换为一个墓碑对象
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		//如果是墓碑对象，那么就从墓碑对象中获取localstorage对象
		//(k8s事件系统中，删除一个对象后，会将这个对象转换为一个墓碑对象，然后再将这个墓碑对象放入到事件系统中，假删除)
		//这里的墓碑对象中包含了被删除的对象的信息，这样控制器就可以在对象被删除之后继续处理一些清理工作
		ls, ok = tombstone.Obj.(*localstoragev1.LocalStorage)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a localstorage %#v", obj))
			return
		}
	}
	klog.V(2).Info("Deleting localstorage", "localstorage", klog.KObj(ls))
	s.enqueueLocalstorage(ls) // 将 LocalStorage 对象放入工作队列中
}

// 更新传入的localstorage类型对象ls
func (s *StorageController) onlyUpdate(ctx context.Context, ls *localstoragev1.LocalStorage) error {
	_, err := s.client.StorageV1().LocalStorages().Update(ctx, ls, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update localstorage %s: %v", ls, err)
		return err
	}

	return nil
}

// 它主要同步或更新存储状态
// 这个函数处理 LocalStorage 对象的同步、初始化和删除过程，确保其状态与 Kubernetes 中的实际状态保持一致
func (s *StorageController) syncStorage(ctx context.Context, dKey string) error {
	startTime := time.Now()
	//记录耗时
	klog.V(2).InfoS("Started syncing localstorage manager", "localstorage", "startTime", startTime)
	defer func() {
		klog.V(2).InfoS("Finished syncing localstorage manager", "localstorage", "duration", time.Since(startTime))
	}()
	// 从列表中获取localstorage对象
	localstorage, err := s.lsLister.Get(dKey)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(2).Infof("localstorage has been deleted", dKey)
			return nil
		}
		return err
	}
	// Deep copy otherwise we are mutating the cache.(深度复制,防止修改缓存)
	ls := localstorage.DeepCopy()

	// Handler deletion event（处理删除事件）
	if !ls.DeletionTimestamp.IsZero() { //如果这个字段非零，说明该对象正在被删除
		// TODO: to delete some external localstorage object
		if ls.Status.Phase != localstoragev1.LocalStorageTerminating { //如果状态不是正在删除
			ls.Status.Phase = localstoragev1.LocalStorageTerminating //设置状态为LocalStorageTerminating
			//k8s中更新 LocalStorage 对象
			if _, err = s.client.StorageV1().LocalStorages().Update(ctx, ls, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
		return nil
	}
	//如果对象没有被删除，那么就判断对象的状态是否为Pending，如果是，那么就将状态设置为LocalStorageInitiating，然后更新到k8s中
	// TODO: handler somethings
	if util.IsPendingStatus(ls) {
		ls.Status.Phase = localstoragev1.LocalStorageInitiating
		//k8s中更新 LocalStorage 对象
		if _, err = s.client.StorageV1().LocalStorages().Update(ctx, ls, metav1.UpdateOptions{}); err != nil {
			return err
		}
		s.eventRecorder.Eventf(ls, v1core.EventTypeNormal, "initialize", fmt.Sprintf("waiting for plugin to initialize %s localstorage", ls.Name))
	}

	return nil
}

func (s *StorageController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()     // 最后处理异常
	defer s.eventBroadcaster.Shutdown() // 最后关闭事件广播器
	defer s.queue.ShutDown()            // 最后关闭工作队列

	klog.Infof("Starting Localstorage Manager")            // 启动 Localstorage Manager
	defer klog.Infof("Shutting down Localstorage Manager") // 关闭 Localstorage Manager
	// 启动事件广播器
	if !cache.WaitForNamedCacheSync("localstorage-manager", ctx.Done(), s.lsListerSynced) {
		return
	}
	// 启动工作队列(workers个数)
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, s.worker, time.Second) // 启动工作队列
	}

	<-ctx.Done() // 等待上下文结束
}

/*
典型的 worker goroutine 模式，常见于 Kubernetes 控制器中
worker 是一个无限循环，一直调用 processNextWorkItem 方法，直到它返回 false，这通常表示工作队列已被关闭
----------------
总的来说，这些代码实现了一个 worker goroutine，用于处理从队列中获取的工作项，并处理可能出现的错误。
这是处理 Kubernetes 资源事件的典型模式，可以保证在多个 goroutine 中处理事件的顺序，
并通过 rate-limiting 队列防止对单个事件的过度重试。
*/
func (s *StorageController) worker(ctx context.Context) {
	for s.processNextWorkItem(ctx) {
	}
}

/*
实际工作的就是这个函数
它从队列中获取一个工作项，如果队列关了（quit为ture），就返回false
*/
func (s *StorageController) processNextWorkItem(ctx context.Context) bool {
	key, quit := s.queue.Get()
	if quit {
		return false
	}
	defer s.queue.Done(key) //工作项处理完毕后，调用Done方法，以便让工作队列知道这个工作项已经被处理完毕
	//调用syncHandler函数处理这个工作项
	err := s.syncHandler(ctx, key.(string))
	//处理错误
	s.handleErr(ctx, err, key)
	//无论是否成功都会返回true,使得worker继续处理下一个工作项
	return true
}

func (s *StorageController) handleErr(ctx context.Context, err error, key interface{}) {
	//如果 syncHandler 没有返回错误，或者错误是由于命名空间正在终止
	//（v1core.NamespaceTerminatingCause),则将该项目从队列中移除（s.queue.Forget(key)），并结束处理
	if err == nil || errors.HasStatusCause(err, v1core.NamespaceTerminatingCause) {
		s.queue.Forget(key)
		return
	}
	//如果错误存在，则解析键值，键值通常是"namespace/name"形式的字符串，如果键值解析失败，则记录错误
	ns, name, keyErr := cache.SplitMetaNamespaceKey(key.(string))
	if keyErr != nil {
		klog.Error(err, "Failed to split meta namespace cache key", "cacheKey", key)
	}
	//检查key的错误重试次数，如果重试次数小于最大允许的重试次数，则使用s.queue.AddRateLimited(key)将该项目重新加入到队列中
	if s.queue.NumRequeues(key) < maxRetries {
		klog.V(2).Info("Error syncing localstorage", "localstorage", klog.KRef(ns, name), "err", err)
		s.queue.AddRateLimited(key)
		return
	}
	//如果重试次数大于最大允许的重试次数，则再日志中记录错误，并将该项目从队列中移除
	utilruntime.HandleError(err)
	klog.V(2).Info("Dropping localstorage out of the queue", "localstorage", klog.KRef(ns, name), "err", err)
	s.queue.Forget(key)
}

// 将 LocalStorage 对象立即加入到工作队列中
func (s *StorageController) enqueue(ls *localstoragev1.LocalStorage) {
	key, err := KeyFunc(ls) //通过 KeyFunc 函数来获取 LocalStorage 对象的 key
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", ls, err))
		return
	}

	s.queue.Add(key)
}

// 以一种受速率限制的方式将 LocalStorage 对象加入到工作队列。
// 如果队列中的相同的 LocalStorage 对象因为错误而被多次重新加入，那么每次加入的间隔会变得越来越大。
// 这有助于防止因为某个不能处理的 LocalStorage 对象而导致的无限循环
func (s *StorageController) enqueueRateLimited(ls *localstoragev1.LocalStorage) {
	key, err := KeyFunc(ls) //通过 KeyFunc 函数来获取 LocalStorage 对象的 key
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", ls, err))
		return
	}

	s.queue.AddRateLimited(key)
}

// 将 LocalStorage 对象在一段时间后加入到工作队列。这对于需要延迟处理的 LocalStorage 对象很有用
func (s *StorageController) enqueueAfter(ls *localstoragev1.LocalStorage, after time.Duration) {
	key, err := KeyFunc(ls) //通过 KeyFunc 函数来获取 LocalStorage 对象的 key
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", ls, err))
		return
	}
	s.queue.AddAfter(key, after)
}
