package main

import (
	"fmt"
    "time"
    "strings"
	"net/http"
	"io/ioutil"
    "encoding/json"
)

type LimitData struct {
    BucketName    string   `json:"LimitBucketName"`
    LimitRate     int64    `json:"LimitBucketRate"`
    LimitConnRate int64    `json:"LimitConnRate"`
    LimitConn     int64    `json:"LimitBucketConn"`
    LimitQps      int64    `json:"LimitBucketQPS"`
}

// Nginx上的桶的限速信息
type LimitList struct {
	BucketLimit []LimitData
}

/* 从@server上获取限速配额信息、
** 返回值：限速配额信息和成功与否
*/
func GetNginxLimit(server string)([]byte, bool) {
    var limitList []byte
    ret := true
    url := "http://" + server + "/?list"
    GLogger.Info("Get Nginx Limit, url is: %s", url)
    res, err := http.Get(url)
    if err != nil {
        GErrorLogger.Error("Get ListLimit failed from %s", server)
        ret = false
        goto RET
    } else {
        defer res.Body.Close()
    }

    limitList, _ = ioutil.ReadAll(res.Body)
RET:
    return limitList, ret
}



/* 从@server上删除@bucket的限速配额
*/
func DelNginxLimit(server string, bucket string) {
    GLogger.Info("Del Nginx %s bucket %s limit quota", server, bucket)
    url := "http://" + server + "/?LimitBucketName=" + bucket
    req, err := http.NewRequest("DELETE", url, nil)
    res, err := http.DefaultClient.Do(req)
    // 删除错误，记录日志并报警
    if err != nil {
        GErrorLogger.Error("Delete %s from %s failed", bucket, server)
        msg := fmt.Sprintf("Delete Bucket %s LimitQuota to %s failed", bucket, server)
        SendWarn(msg)
    } else {
        defer res.Body.Close()
    }
}



/* 更新@server上的@bucket限速配额
*/
func SetNginxLimit(server string, bucket string, rate int64, conn int64, qps int64, connrate int64){
    var pkg LimitData
    pkg.BucketName = bucket
    pkg.LimitRate  = rate
    pkg.LimitConn  = conn
    pkg.LimitQps   = qps
    pkg.LimitConnRate = connrate

    // 封装成json格式
    buf, err := json.Marshal(pkg)
    if err != nil {
        GErrorLogger.Error("Marshal %s failed", pkg)
        return
    }
    GLogger.Info("Set Nginx %s limit quota [%s]", server, string(buf))

    url := "http://" + server
    s := string(buf[:len(buf)])
    res, err := http.Post(url, "text/plain", strings.NewReader(s))
    // 设置错误，记录日志并且报警
    if err != nil {
        GErrorLogger.Error("Set %s failed, content: %s", url, buf)
        msg := fmt.Sprintf("set Limit Data to %s failed", server)
        SendWarn(msg)
    } else {
        defer res.Body.Close()
    }
}


// 启动LimitServer，server形式为ip:port
func limitServer(server string) {
    GLogger.Info("Start limitServer: %s", server)
    contFailed := 0
	for {
        // 从前端Nginx获取
        limitList, ok := GetNginxLimit(server)
        if  !ok {
            contFailed = contFailed + 1
            if(LimitListWarnThreahold > 0 && contFailed >= LimitListWarnThreahold) {
                warn := fmt.Sprintf("Get ListLimit failed %d times continuously from %s", contFailed, server)
                SendWarn(warn)
            }
		    time.Sleep(60000 * time.Millisecond)
			continue
        }
        contFailed = 0

		GLogger.Info("[%s] LimitList: %s", time.Now().Format("2006-01-02 15:04:05"), limitList)

		var blimits LimitList
        err := json.Unmarshal(limitList, &(blimits.BucketLimit))
        if err != nil {
            GErrorLogger.Error("Json unmarshal %s failed: %s\n", limitList, err)
        }

        // LimitServer本地存储的桶限速配额
        localLimits := make(map[string]*BucketQuota)
        for key, value := range QuotaInfo {
            if value[1] != nil {
                localLimits[key] = value[1]
            }
        }

        /* 遍历所有收到的Bucket,对比其值与本地是否相同
        ** 如果相同，从需要更新map中剔除
        ** 如果对端有的，本地没有，那么需要从对端删除
        ** 如果对端没有的，本地有，那么需要更新至对端
        */
        sngx := int64(len(Nginxs))
        for _, v := range (blimits.BucketLimit) {
            l, ok := localLimits[v.BucketName]
            if ok {
                if v.LimitRate == int64(l.RateQuota/sngx) && v.LimitConn == int64(l.ConnQuota/sngx) &&
                   v.LimitQps  == int64(l.QpsQuota/sngx)  && v.LimitConnRate == l.RatePerConn {
                    delete(localLimits, v.BucketName)
                }
            } else {
                // 如果本地没有找到，就应该从Nginx上删除桶配额信息
                GErrorLogger.Error("No bucket %s limit quota found", v.BucketName)
                DelNginxLimit(server, v.BucketName)
            }
        }

        // 逐个更新Nginx上的桶配额信息
        for _, value := range localLimits {
            SetNginxLimit(server, value.BucketName, int64(value.RateQuota/sngx), int64(value.ConnQuota/sngx),
                          int64(value.QpsQuota/sngx), int64(value.RatePerConn/sngx))
        }
		time.Sleep(60000 * time.Millisecond)
	}
	GLogger.Info("Limit Server exit")
	time.Sleep(1000 * time.Millisecond)
}


/* 启动LimitServer
** 首先，读配置文件，解析所有前端Nginx地址
** 然后，为每个前端Nginx启动一个go routine
*/
func startLimitServer() {
    for _, v := range Nginxs {
        go limitServer(v)
    }
}



func main() {
    InitLogger()
	UDPRingChan := make(chan BucketStatistic)
    if LoadConfig() != true {
        panic("load configure")
    }

    /* init post gresql on inspur1 */
    if !PgInit() {
        panic ("postgre init failed")
    }

    startLimitServer()

	go ringManager(UDPRingChan)

	go UdpServer(UDPRingChan)

	go HttpServer()

	go StaticServer()

    QuotaInit()

	for {
		time.Sleep(10000 * time.Millisecond)
	}

    GLogger.Info("Main thread exit")
	GLogger.Info("Limit server exit")
	time.Sleep(1000 * time.Millisecond)
}

