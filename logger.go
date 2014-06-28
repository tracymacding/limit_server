/*
** LimitServer 日志模块，对log4go的简单封装
*/

package main

import(
    "os"
    "fmt"
)

import l4g "code.google.com/p/log4go"


var GLogger        l4g.Logger
var GErrorLogger   l4g.Logger
var GStatLogger    l4g.Logger
var GServerLog     map[string]l4g.Logger


func init_specific_logger(logPath string, owner l4g.Logger) {
    logger := l4g.NewFileLogWriter(logPath, false)
    logger.SetRotate(true)
    logger.SetRotateDaily(true)
    logger.SetFormat("[%D %T] [%L] %M")
    owner.AddFilter("file", l4g.INFO, logger)
    return
}


func InitServerLogger(path string) {
    _, ok := GServerLog[path]
    if ok {
        GLogger.Info("Already exist")
        return
    }

    log := make(l4g.Logger)
    init_specific_logger("./logs/" + path + ".log", log)
    GServerLog[path] = log
    return
}


// logger模块初始化
// 1.创建日志目录
// 2.初始化运行日志 run.log
// 3.初始化统计日志 stat.log
// 4.初始化错误日志 error.log
// 5.初始化Nginx Server统计日志，存储于map
func InitLogger() {
    fmt.Println("Start Logger Init")
    err := os.Mkdir("./logs", 0777)
    if err != nil && !os.IsExist(err) {
        fmt.Println("create log dir failed: ", err)
        panic(1)
    }

    GLogger = make(l4g.Logger)
    init_specific_logger("./logs/run.log", GLogger)

    GErrorLogger = make(l4g.Logger)
    init_specific_logger("./logs/error.log", GErrorLogger)

    GStatLogger = make(l4g.Logger)
    init_specific_logger("./logs/stat.log", GStatLogger)

    GServerLog = make(map[string]l4g.Logger)
    fmt.Println("Finish Logger Init")
    return
}
