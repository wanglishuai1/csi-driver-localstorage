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

package util

import (
	localstoragev1 "github.com/caoyingjunz/csi-driver-localstorage/pkg/apis/localstorage/v1"
)

const (
	// LsProtectionFinalizer is the name of finalizer on ls.
	LsProtectionFinalizer = "caoyingjunz.io/ls-protection"
)

// 终结器的Util

// AddFinalizer accepts an Object and adds the provided finalizer if not present.
// It returns an indication of whether it updated the object's list of finalizers.
/*
	AddFinalizer接受一个Object并添加提供的终结器（如果不存在）。
	它返回是否更新了对象的终结器列表的指示。
*/
func AddFinalizer(o *localstoragev1.LocalStorage, finalizer string) (finalizersUpdated bool) {
	f := o.GetFinalizers()
	for _, e := range f {
		if e == finalizer {
			return false
		}
	}
	o.SetFinalizers(append(f, finalizer))
	return true
}

// RemoveFinalizer accepts an Object and removes the provided finalizer if present.
// It returns an indication of whether it updated the object's list of finalizers.
/*
	RemoveFinalizer接受一个Object并删除提供的终结器（如果存在）。
	它返回是否更新了对象的终结器列表的指示。
*/
func RemoveFinalizer(o *localstoragev1.LocalStorage, finalizer string) (finalizersUpdated bool) {
	f := o.GetFinalizers()
	for i := 0; i < len(f); i++ {
		if f[i] == finalizer {
			f = append(f[:i], f[i+1:]...)
			i--
			finalizersUpdated = true
		}
	}
	o.SetFinalizers(f)
	return
}

// ContainsFinalizer checks an Object that the provided finalizer is present.
// ContainsFinalizer检查提供的终结器存在的对象。
func ContainsFinalizer(o *localstoragev1.LocalStorage, finalizer string) bool {
	f := o.GetFinalizers()
	for _, e := range f {
		if e == finalizer {
			return true
		}
	}
	return false
}
