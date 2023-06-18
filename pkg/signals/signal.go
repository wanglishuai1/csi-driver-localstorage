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

package signals

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

var (
	onlyOneSignalHandler = make(chan struct{})
	shutdownSignals      = []os.Signal{os.Interrupt, syscall.SIGTERM}
)

/*
为SIGTERM和SIGINT注册的SetupSignalHandler。
返回一个上下文，该上下文在其中一个信号上被取消。
如果捕获到第二个信号，程序将以退出代码1终止。
允许你设置一个信号处理程序来处理操作系统发出的终止信号（如Ctrl+C 或者 SIGTERM）。
当接收到这些信号时，它会取消返回的context，这允许正在运行的go程序可以优雅地结束它们的工作
*/
func SetupSignalHandler() context.Context {
	close(onlyOneSignalHandler) // panics when called twice

	c := make(chan os.Signal, 2)
	ctx, cancel := context.WithCancel(context.Background())
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		cancel()
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return ctx
}
