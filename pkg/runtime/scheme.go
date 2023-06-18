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

package runtime

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	localstoragev1 "github.com/caoyingjunz/csi-driver-localstorage/pkg/apis/localstorage/v1"
)

var (
	scheme = runtime.NewScheme()
)

// Scheme用于将Go结构映射到组，版本和种类的字符串，这是反序列化API对象所必需的
// 定义了一个全局的scheme，并在init函数中注册了两种API资源对象的类型：
// 标准的Kubernetes对象（如Pods, Services等）和本地存储的API对象
func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(localstoragev1.AddToScheme(scheme))
}

// NewScheme creates a new Scheme.
func NewScheme() *runtime.Scheme {
	return scheme
}
