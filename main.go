package main

import (
	"fmt"
	"go-redis/config"
	"go-redis/lib/logger"
	"go-redis/tcp"
	"os"
)

const configFile string = "redis.conf"

var defaultProperties = &config.ServerProperties{ // 如果 configFile 不存在，使用该默认配置
	Bind: "0.0.0.0",
	Port: 6379,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	logger.Setup(&logger.Settings{ // 日志格式
		Path:       "logs",
		Name:       "go-redis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	if fileExists(configFile) { // 判断配置文件是否存在，不存在则使用默认配置
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}

	err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port), // IP:Port
		},
		tcp.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
