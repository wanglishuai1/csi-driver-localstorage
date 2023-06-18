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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes/wrappers"
)

// IdentityServer is the implementation of the CSI IdentityServer(IdentityServer是CSI IdentityServer的实现)
type IdentityServer struct {
	Driver *localStorage
}

// GetPluginInfo 返回插件的信息
func (ls *localStorage) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          ls.config.DriverName,
		VendorVersion: ls.config.VendorVersion,
	}, nil
}

// Probe 返回插件的健康和可用性
func (ls *localStorage) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: &wrappers.BoolValue{Value: true}}, nil
}

// GetPluginCapabilities 返回插件的可用功能
func (ls *localStorage) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{ //
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE, // 本地存储驱动只支持控制器服务
					},
				},
			},
		},
	}, nil
}
