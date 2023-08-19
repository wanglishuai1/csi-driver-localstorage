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
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

/*
解析 endpoint，返回协议、地址和错误。
例如，如果 ep 是 "unix:///var/run/csi.sock"，那么这个函数会返回 "unix"、"/var/run/csi.sock" 和 nil。
如果 ep 是 "unix://"，那么这个函数会返回 ""、"" 和一个错误
*/
func parseEndpoint(ep string) (string, string, error) {
	//判断是否是unix socket
	if strings.HasPrefix(strings.ToLower(ep), "unix://") {
		//用://分割字符串
		s := strings.SplitN(ep, "://", 2)
		if s[1] != "" {
			return s[0], s[1], nil
		}
	}
	return "", "", fmt.Errorf("invalid endpoint: %v", ep)
}

/*
gRPC的拦截器函数，用于在gRPC调用过程中进行日志记录
*/
func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	klog.V(2).Infof("GRPC call: %s", info.FullMethod)

	resp, err := handler(ctx, req)
	if err != nil {
		klog.Errorf("GRPC error: %v", err)
	}

	return resp, err
}

/*
创建volume目录
*/
func makeVolumeDir(volDir string) error {
	_, err := os.Stat(volDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err = os.MkdirAll(volDir, os.ModePerm); err != nil {
			return err
		}
	}

	return nil
}

// parseVolumePath returns the canonical path for volume
func parseVolumePath(baseDir, volID string) string {
	return filepath.Join(baseDir, volID)
}
