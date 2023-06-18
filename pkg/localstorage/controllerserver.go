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
	"fmt"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	localstoragev1 "github.com/caoyingjunz/csi-driver-localstorage/pkg/apis/localstorage/v1"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/util"
)

type Operation string

const (
	AddOperation Operation = "add"
	SubOperation Operation = "sub"
)

// CreateVolume create a volume
func (ls *localStorage) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	//获取名称
	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume name must be provided")
	}
	//获取容量
	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}
	//设置锁
	ls.lock.Lock()
	defer ls.lock.Unlock() //解锁
	//获取当前这个node的localstorage
	localstorage, err := ls.getLocalStorageByNode(ls.GetNode())
	if err != nil {
		return nil, err
	}
	//生成volume的ID
	volumeID := uuid.New().String()

	// TODO: 临时实现，后续修改
	//format一下路径 volumeDir + / + volumeID
	path := ls.parseVolumePath(volumeID)
	//创建目录
	if err := os.MkdirAll(path, 0750); err != nil {
		return nil, err
	}

	// Deep-copy otherwise we are mutating our cache.
	// TODO: Deep-copy only when needed.
	//深度拷贝数据到s
	s := localstorage.DeepCopy()
	//获取 PVC 请求的存储空间大小
	volSize := req.GetCapacityRange().GetRequiredBytes()
	//把volume添加到localstorage的volumes中
	util.AddVolume(s, localstoragev1.Volume{
		VolID:   volumeID,
		VolName: name,
		VolPath: path,
		VolSize: volSize,
	})
	//计算可用空间（传入当前s的可用空间、新的volume）
	s.Status.Allocatable = ls.calculateAllocatedSize(s.Status.Allocatable, volSize, SubOperation)

	// Update the changes immediately(立即更新更改)
	if _, err = ls.client.StorageV1().LocalStorages().Update(ctx, s, metav1.UpdateOptions{}); err != nil {
		return nil, err
	}
	//设置volumeContext
	volumeContext := req.GetParameters()
	if volumeContext == nil {
		volumeContext = make(map[string]string)
	}
	volumeContext["localPath.caoyingjunz.io"] = path

	klog.Infof("pvc %v volume %v successfully deleted", name, volumeID)
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           volumeID,
			CapacityBytes:      req.GetCapacityRange().GetRequiredBytes(), //todo rp 应该用已经获取的变量
			VolumeContext:      volumeContext,
			ContentSource:      req.GetVolumeContentSource(),
			AccessibleTopology: []*csi.Topology{},
		},
	}, nil
}

// DeleteVolume delete a volume
func (ls *localStorage) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	//获取volumeID
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	//上锁
	ls.lock.Lock()
	defer ls.lock.Unlock()
	//获取当前node的localstorage
	localstorage, err := ls.getLocalStorageByNode(ls.GetNode())
	if err != nil {
		return nil, err
	}
	//获取volumeID
	volId := req.GetVolumeId()

	// Deep-copy otherwise we are mutating our cache.
	// TODO: Deep-copy only when needed.
	s := localstorage.DeepCopy()
	//删除volume
	vol := util.RemoveVolume(s, volId)
	//重新计算可用空间（删除之后，可用空间增加，所以是add）
	s.Status.Allocatable = ls.calculateAllocatedSize(s.Status.Allocatable, vol.VolSize, AddOperation)

	// Update the changes immediately(立即更新更改)
	if _, err = ls.client.StorageV1().LocalStorages().Update(ctx, s, metav1.UpdateOptions{}); err != nil {
		return nil, err
	}
	//删除目录
	// TODO: 临时处理
	if err := os.RemoveAll(ls.parseVolumePath(volId)); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	klog.Infof("volume %v successfully deleted", volId)

	return &csi.DeleteVolumeResponse{}, nil
}

// 计算可用空间
func (ls *localStorage) calculateAllocatedSize(allocatableSize *resource.Quantity, volSize int64, op Operation) *resource.Quantity {
	volSizeCap := util.BytesToQuantity(volSize) //把volume的大小转换成resource.Quantity
	//(如果删除volume，可用空间就会变大，如果增加volume，可用空间就会变小）
	switch op {
	case AddOperation: //如果是add操作，就把可用空间加上volume的大小
		allocatableSize.Add(volSizeCap)
	case SubOperation: //如果是sub操作，就把可用空间减去volume的大小
		allocatableSize.Sub(volSizeCap)
	}

	return allocatableSize
}

// get localstorage object by nodeName, error when not found（根据nodeName获取localstorage对象，如果没有找到就报错）
func (ls *localStorage) getLocalStorageByNode(nodeName string) (*localstoragev1.LocalStorage, error) {
	//获取所有的localstorage
	lsNodes, err := ls.lsLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var lsNode *localstoragev1.LocalStorage
	for _, l := range lsNodes {
		//如果localstorage的node和传入的node相同，就把lsNode设置为当前的localstorage
		if l.Spec.Node == nodeName {
			lsNode = l
		}
	}
	//如果没有找到localstorage，就报错
	if lsNode == nil {
		return nil, fmt.Errorf("failed to found localstorage with node %s", nodeName)

	}
	//返回localstorage
	return lsNode, nil
}

// parseVolumePath returns the canonical path for volume(返回卷的规范路径)
func (ls *localStorage) parseVolumePath(volID string) string {
	return filepath.Join(ls.config.VolumeDir, volID)
}

// 暂时没用
func (ls *localStorage) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// 暂时没用
func (ls *localStorage) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// 暂时没用
func (ls *localStorage) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// 暂时没用
func (ls *localStorage) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (ls *localStorage) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	// 定义一个csi.ListVolumesResponse
	volumes := &csi.ListVolumesResponse{ //ListVolumeRPC调用的返回结果，用于返回CSI插件当前管理的所有的卷
		Entries: []*csi.ListVolumesResponse_Entry{}, //表示一个卷及其相关信息,每个entry通常包含俩字段：volume和status
	}
	//上锁
	ls.lock.Lock()
	defer ls.lock.Unlock()
	//遍历cache中的所有的volume
	for _, vol := range ls.cache.GetVolumes() {
		volumes.Entries = append(volumes.Entries, &csi.ListVolumesResponse_Entry{
			//volume信息(容量，id)
			Volume: &csi.Volume{
				VolumeId:      vol.VolID,
				CapacityBytes: vol.VolSize,
			},
			//volume状态（是一个可选字段，提供了这个卷当前的状态，例如是否被控制插件发布）
			Status: &csi.ListVolumesResponse_VolumeStatus{
				PublishedNodeIds: []string{vol.NodeID},
				VolumeCondition:  &csi.VolumeCondition{},
			},
		})
	}
	/*
		这样子当我们需要获取存储系统中所有卷的信息时，可以调用 ListVolumes RPC，
		并通过查看返回的 ListVolumesResponse 中的 ListVolumesResponse_Entry
		来获得每个卷的详细信息
	*/
	klog.Infof("Localstorage volumes are: %+v", volumes)
	return volumes, nil
}

// 暂时没用
func (ls *localStorage) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// 返回CSI 控制器插件支持的功能
func (ls *localStorage) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: ls.getControllerServiceCapabilities(),
	}, nil
}

// 暂时没用
func (ls *localStorage) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// 暂时没用
func (ls *localStorage) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// 暂时没用
func (ls *localStorage) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// 暂时没用
func (ls *localStorage) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (ls *localStorage) getControllerServiceCapabilities() []*csi.ControllerServiceCapability {
	cl := []csi.ControllerServiceCapability_RPC_Type{ //ControllerServiceCapability_RPC_Type是一个枚举类型，表示CSI控制器插件支持的功能
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,     //表示CSI控制器插件支持创建和删除卷
		csi.ControllerServiceCapability_RPC_GET_VOLUME,               //表示CSI控制器插件支持获取卷的信息
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,             //表示CSI控制器插件支持获取存储系统的容量
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,             //表示CSI控制器插件支持获取存储系统中所有卷的信息
		csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,         //表示CSI控制器插件支持获取卷的状态
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER, //表示CSI控制器插件支持单节点多写
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME, //表示CSI控制器插件支持发布和取消发布卷
	}

	var csc []*csi.ControllerServiceCapability
	for _, cap := range cl {
		csc = append(csc, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return csc
}
