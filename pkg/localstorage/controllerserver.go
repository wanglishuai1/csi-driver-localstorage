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
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	localstoragev1 "github.com/caoyingjunz/csi-driver-localstorage/pkg/apis/localstorage/v1"
	"github.com/caoyingjunz/csi-driver-localstorage/pkg/util"
	storageutil "github.com/caoyingjunz/csi-driver-localstorage/pkg/util/storage"
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

	defer ls.lock.Unlock()
	localstorage, err := storageutil.GetLocalStorageByNode(ls.lsLister, ls.GetNode())
	if err != nil {
		return nil, err
	}
	// Deep-copy otherwise we are mutating our cache.
	// TODO: Deep-copy only when needed.
	//深度拷贝数据到s
	s := localstorage.DeepCopy()

	volumeID := uuid.New().String()
	volPath := parseVolumePath(ls.config.VolumeDir, volumeID)
	if err = storageutil.CreateVolumeDir(volPath); err != nil {
		return nil, err
	}

	volSize := req.GetCapacityRange().GetRequiredBytes()
	//把volume添加到localstorage的volumes中
	util.AddVolume(s, localstoragev1.Volume{
		VolID:   volumeID,
		VolName: name,
		VolPath: volPath,
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
	volumeContext["localPath.caoyingjunz.io"] = volPath

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
	localstorage, err := storageutil.GetLocalStorageByNode(ls.lsLister, ls.GetNode())
	if err != nil {
		return nil, err
	}

	// Deep-copy otherwise we are mutating our cache.
	// TODO: Deep-copy only when needed.
	s := localstorage.DeepCopy()

	volId := req.GetVolumeId()

	vol := util.RemoveVolume(s, volId)
	//重新计算可用空间（删除之后，可用空间增加，所以是add）
	s.Status.Allocatable = ls.calculateAllocatedSize(s.Status.Allocatable, vol.VolSize, AddOperation)

	// Update the changes immediately(立即更新更改)
	if _, err = ls.client.StorageV1().LocalStorages().Update(ctx, s, metav1.UpdateOptions{}); err != nil {
		return nil, err
	}
	volPath := parseVolumePath(ls.config.VolumeDir, volId)
	if err = storageutil.DeleteVolumeDir(volPath); err != nil {
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

	// TODO: 后续实现
	// for _, vol := range ls.cache.GetVolumes() {
	// 	volumes.Entries = append(volumes.Entries, &csi.ListVolumesResponse_Entry{
	// 		Volume: &csi.Volume{
	// 			VolumeId:      vol.VolID,
	// 			CapacityBytes: vol.VolSize,
	// 		},
	// 		Status: &csi.ListVolumesResponse_VolumeStatus{
	// 			PublishedNodeIds: []string{vol.NodeID},
	// 			VolumeCondition:  &csi.VolumeCondition{},
	// 		},
	// 	})
	// }

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
