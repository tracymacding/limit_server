/* LimitServer统计和页面展示模块
** 1. 从Nginx接收统计数据&聚合统计数据形成最终结果
** 2. 通过WEB页面展示最近一段时间内每个桶的聚合统计结果
*/

package main

import (
	"fmt"
    "net"
	"time"
    "sort"
    "sync"
    "syscall"
    "strconv"
	"net/http"
	"encoding/json"
	"container/ring"
	"code.google.com/p/plotinum/plot"
	"code.google.com/p/plotinum/plotter"
	"code.google.com/p/plotinum/plotutil"
)


const (
    TOTAL = iota
    TOTAL_FAILED
    LIST
    PUT
    GET
    DELETE
    IMAGE
    VIDEO
)

type timeState struct {
	TimeStamp           int64
    windowData          map[string] *BucketState
}

type BucketPending struct {
	BucketList []string `json:"bucket_list"`
}


type BucketPut struct {
	ExpectedBucketRate  float64  `json:"expected_bucket_rate"`
	ExpectedBucketConn  float64  `json:"expected_bucket_conn"`
	ExpectedBucketQps   float64  `json:"expected_bucket_qps"`
	ExpectedConnRate    float64  `json:"expected_conn_rate"`
}

// LimitServer对QPS进行分类展示
type BucketQPS struct {
    QPSTotal     float64 `json:"qps_total"`
    QPSTotalFailed     float64 `json:"qps_total_failed"`
    QPSGet       float64 `json:"qps_get"`
    QPSPut       float64 `json:"qps_put"`
    QPSDelete    float64 `json:"qps_delete"`
    QPSList      float64 `json:"qps_list"`
    QPSVideo     float64 `json:"qps_video"`
    QPSImage     float64 `json:"qps_image"`
}

// Nginx服务器发送的统计数据格式
type BucketStatistic struct {
	BucketName          string `json:"bucket_name"`
	TimeStamp           int64  `json:"time_stamp"`
    ServerAddr          string `json:"server_addr"`

	ExpectedBucketRate  float64   `json:"expected_bucket_rate"`
	ExpectedBucketConn  float64   `json:"expected_bucket_conn"`
	ExpectedBucketQps   float64   `json:"expected_bucket_qps"`
	ExpectedConnRate    float64   `json:"expected_conn_rate"`

	AssignedBucketRate  float64   `json:"assigned_bucket_rate"`
	AssignedBucketConn  float64   `json:"assigned_bucket_conn"`
	AssignedBucketQps   float64   `json:"assigned_bucket_qps"`

	StatisticBucketRate float64  `json:"statistic_bucket_rate"`
	StatisticBucketConn float64  `json:"statistic_bucket_conn"`
	StatisticBucketConnMax float64  `json:"statistic_bucket_conn_max"`
	StatisticBucketQps  BucketQPS `json:"statistic_bucket_qps"`
}



// 桶统计数据聚合后信息
type BucketState struct {
	BucketName          string    `json:"bucket_name"`
	TimeStamp           int64     `json:"time_stamp"`

	StatisticBucketRate float64   `json:"statistic_bucket_rate"`
	StatisticBucketConn float64   `json:"statistic_bucket_conn"`
	StatisticBucketConnMax float64  `json:"statistic_bucket_conn_max"`
	StatisticBucketQps  BucketQPS `json:"statistic_bucket_qps"`

	ExpectedBucketRate  float64   `json:"expected_bucket_rate"`
	ExpectedBucketConn  float64   `json:"expected_bucket_conn"`
	ExpectedBucketQps   float64   `json:"expected_bucket_qps"`
	ExpectedConnRate    float64   `json:"expected_conn_rate"`

	AssignedBucketRate  float64   `json:"assigned_bucket_rate"`
	AssignedBucketConn  float64   `json:"assigned_bucket_conn"`
	AssignedBucketQps   float64   `json:"assigned_bucket_qps"`

    reportServerNum     int64
    reportServers       [32]string
    reportDone          bool
}


const NUM           = 120
const DURATION      = 5
const WINDOW_SIZE   = 3
const TOTAL_SERVERS = 2


var gPrev int = 0
var gCurr int = 1
var gNext int = 2

// 利用数组构造的滑动窗口
var windows    [WINDOW_SIZE]timeState

// 内存中存储的一段时间桶聚合数据，
// 使用golang的ringBuffer(环形缓冲区)
var ringmap map[string]*ring.Ring

// 所有桶的总下载流量、qps、连接数统计数据
var TotalStatisticRing *ring.Ring = nil

// 保护ringmap的并发读写锁
var rwLocker sync.RWMutex

var LastUpdate time.Time



func (b BucketStatistic) GetTimeStamp() (int64) {
	return b.TimeStamp
}


func (b BucketStatistic) GetBucketRate() (float64, float64, float64) {
	return b.ExpectedBucketRate, b.AssignedBucketRate, b.StatisticBucketRate
}


func (b BucketStatistic) GetBucketQPS() (float64, float64, BucketQPS) {
	return b.ExpectedBucketQps, b.AssignedBucketQps, b.StatisticBucketQps
}


func (b BucketStatistic) GetBucketConn() (float64, float64, float64, float64) {
	return b.ExpectedBucketConn, b.AssignedBucketConn, b.StatisticBucketConn, b.StatisticBucketConnMax
}


func getRingBuffer(bucketName string) *ring.Ring {

	ringBuffer, ok := ringmap[bucketName]
	if !ok {
		GLogger.Error("Get RingBuffer fail, bucketName = %s\n", bucketName)
	}
	return ringBuffer
}


/* 有效窗口外的统计数据为无效数据
** 如果有效，返回统计数据所在的窗口号
** CURR:当前窗口
** PREV:前一个窗口
** NEXT:后一个窗口
*/
func isBucStatisticValid(bucketStatistic BucketStatistic) (valid bool, which int) {
    ts := bucketStatistic.TimeStamp
    for i := 0; i < WINDOW_SIZE; i++ {
        if ts == windows[i].TimeStamp {
            valid = true
            which = i
            return
        }
    }
    valid = false
    which = -1
    return
}


/* 
** 判断统计数据是否重复
*/
func isReplicateStatistic(bucketStatistic *BucketStatistic, bucketStat *BucketState) (bool) {
    var i int64 = 0
    for addr := bucketStat.reportServers[i]; i < bucketStat.reportServerNum; i++ {
        if addr == bucketStatistic.ServerAddr {
            return true
        }
    }
    return false
}


func initQPS(qps *BucketQPS) {
   qps.QPSPut    = 0
   qps.QPSGet    = 0
   qps.QPSVideo  = 0
   qps.QPSList   = 0
   qps.QPSTotal  = 0
   qps.QPSTotalFailed  = 0
   qps.QPSDelete = 0
   qps.QPSImage  = 0
}


/* res += data */
func updateQps(res *BucketQPS, data *BucketQPS) {
    res.QPSPut    += data.QPSPut
    res.QPSGet    += data.QPSGet
    res.QPSVideo  += data.QPSVideo
    res.QPSList   += data.QPSList
    res.QPSTotal  += data.QPSTotal
    res.QPSTotalFailed  += data.QPSTotalFailed
    res.QPSDelete += data.QPSDelete
    res.QPSImage  += data.QPSImage
}


/* 桶统计数据聚合
** 参数：
** 1. bucketStatistic:Nginx推送的统计数据
** 2. bucketStat:桶聚合数据
** 返回值：true表示聚合成功
**         false聚合失败，可能由于统计数据已经重复
*/
func updateBucketStat(bucketStatistic *BucketStatistic, bucketStat *BucketState) (bool) {

    if isReplicateStatistic(bucketStatistic, bucketStat) {
        stime := time.Unix(bucketStatistic.TimeStamp, 0).String()
        GErrorLogger.Error("Replicate BucketStatistic, TimeStamp: %s, BucketName: %s, Server: %s",
                        stime, bucketStatistic.BucketName, bucketStatistic.ServerAddr)
        return false
    }

    bucketStat.TimeStamp = bucketStatistic.TimeStamp
    bucketStat.StatisticBucketRate += bucketStatistic.StatisticBucketRate
    bucketStat.StatisticBucketConn += bucketStatistic.StatisticBucketConn
    bucketStat.StatisticBucketConnMax += bucketStatistic.StatisticBucketConnMax
    // QPS单独处理，包含多个细项(GET/PUT/DELETE...)
    updateQps(&(bucketStat.StatisticBucketQps), &(bucketStatistic.StatisticBucketQps))

    bucketStat.AssignedBucketRate += bucketStatistic.AssignedBucketRate
    bucketStat.AssignedBucketConn += bucketStatistic.AssignedBucketConn
    bucketStat.AssignedBucketQps  += bucketStatistic.AssignedBucketQps

    bucketStat.ExpectedBucketRate += bucketStatistic.ExpectedBucketRate
    bucketStat.ExpectedBucketConn += bucketStatistic.ExpectedBucketConn
    bucketStat.ExpectedBucketQps  += bucketStatistic.ExpectedBucketQps

    bucketStat.reportServers[bucketStat.reportServerNum] = bucketStatistic.ServerAddr
    bucketStat.reportServerNum += 1

    return true
}



/* 滑动窗口初始化
** 以收到的第一个统计数据的TimeStamp为当前窗口
*/
func init_stat_windows(bucketStatistic *BucketStatistic) {
    // 考虑到可能中断后有可能重新初始化窗口
    // 最好将原来窗口中的所有统计数据写入日志中，如果有的话
    // 写入的顺序是prev->curr->next
    w := gPrev
    var servers string
    for i := 0; i < WINDOW_SIZE; i++ {
        ts := windows[w].TimeStamp
        stime := time.Unix(ts, 0).Format("2006-01-02 15:04:05")

        for key, value := range windows[w].windowData {
            if value.reportServerNum == 0 {
                continue
            }
            servers = "["
            for i := int64(0); i < value.reportServerNum; i++ {
                servers += value.reportServers[i]
                if i < value.reportServerNum - 1 {
                    servers += " & "
                }
            }
            servers += "]"

            qps, err := json.Marshal(value.StatisticBucketQps)
            if err != nil {
                GErrorLogger.Error("Marshal %s falied", value.StatisticBucketQps)
            }

            GStatLogger.Info("TimeStamp: %s, BucketName: %s, servers: %s, StatisticRate: %.1f, StatisticConn: %.1f, StatisticConnMax: %.1f, StatisticQps: %s, AssignRate: %.1f, AssignConn: %.1f, AssignQps: %.1f, ExpectRate: %.1f, ExpectConn: %.1f, ExpectQps: %.1f",
                         stime, key, servers, value.StatisticBucketRate, value.StatisticBucketConn, value.StatisticBucketConnMax,
                         string(qps[:len(qps)]),
                         value.AssignedBucketRate, value.AssignedBucketConn, value.AssignedBucketQps,
                         value.ExpectedBucketRate, value.ExpectedBucketConn, value.ExpectedBucketQps)


            /* send data to local database */
            {
                var item [11]uint64
                stime := value.TimeStamp
                bname := value.BucketName

                item[0] = uint64(value.StatisticBucketRate)
                item[1] = uint64(value.StatisticBucketConn)
                item[2] = uint64(value.StatisticBucketConnMax)
                item[3] = uint64(value.StatisticBucketQps.QPSTotal)
                item[4] = uint64(value.StatisticBucketQps.QPSTotalFailed)
                item[5] = uint64(value.StatisticBucketQps.QPSGet)
                item[6] = uint64(value.StatisticBucketQps.QPSPut)
                item[7] = uint64(value.StatisticBucketQps.QPSDelete)
                item[8] = uint64(value.StatisticBucketQps.QPSList)
                item[9] = uint64(value.StatisticBucketQps.QPSVideo)
                item[10] = uint64(value.StatisticBucketQps.QPSImage)
                // item := make([]uint64, 0)
                // item  = append(item, rate, conn, tqps, gqps, pqps, dqps, lqps, vqps, iqps)
                PgInsert(stime, bname, servers, item)
            }

            // 检查流量是否超过报警阈值，如果是，根据配置发送报警
            errMsg, over := CheckQuota(key, value.StatisticBucketRate, value.StatisticBucketConnMax,
                                             value.StatisticBucketQps.QPSTotal, 0)
            if over {
                SendWarn("Bucket: " + key + errMsg)
            }

            value.TimeStamp  = 0
            value.StatisticBucketRate = 0
            value.StatisticBucketConn = 0
            value.StatisticBucketConnMax = 0
            initQPS(&(value.StatisticBucketQps))

            value.AssignedBucketRate = 0
            value.AssignedBucketConn = 0
            value.AssignedBucketQps  = 0

            value.ExpectedBucketRate = 0
            value.ExpectedBucketConn = 0
            value.ExpectedBucketQps  = 0

            value.reportServerNum = 0
            value.reportDone = false
        }
        w = (w + 1) % WINDOW_SIZE
    }

    bucketStat := new(BucketState)
    windows[gCurr].TimeStamp = bucketStatistic.TimeStamp
    bucketStat.BucketName = bucketStatistic.BucketName
    bucketStat.TimeStamp = bucketStatistic.TimeStamp
    bucketStat.reportServerNum = 0
    bucketStat.reportDone = false
    updateBucketStat(bucketStatistic, bucketStat)
    windows[gCurr].TimeStamp = bucketStatistic.TimeStamp
    windows[gCurr].windowData = make(map[string]*BucketState)
    windows[gCurr].windowData[bucketStatistic.BucketName] = bucketStat
    windows[gPrev].TimeStamp = bucketStatistic.TimeStamp - DURATION
    windows[gPrev].windowData = make(map[string]*BucketState)
    windows[gNext].TimeStamp = bucketStatistic.TimeStamp + DURATION
    windows[gNext].windowData = make(map[string]*BucketState)

    sts := time.Unix(bucketStatistic.TimeStamp, 0).Format("2006-01-02 15:04:05")
    LastUpdate = time.Now()
    GLogger.Info("Init stat window done, current timestamp: %s", sts)
}


/*
** 时间窗口向前滑动,滑动之前将PREV窗口的聚合数据写入日志
*/
func window_go_forward() (bool) {
    var servers string

    ts := windows[gPrev].TimeStamp
    stime := time.Unix(ts, 0).Format("2006-01-02 15:04:05")
    LastUpdate = time.Now()

    for key, value := range windows[gPrev].windowData {
        if value.reportServerNum == 0 {
            continue
        }
        servers = "["
        for i := int64(0); i < value.reportServerNum; i++ {
            servers += value.reportServers[i]
            if i < value.reportServerNum - 1 {
                servers += " & "
            }
        }
        servers += "]"

        qps, err := json.Marshal(value.StatisticBucketQps)
        if err != nil {
            GErrorLogger.Error("Marshal %s falied", value.StatisticBucketQps)
        }

        GStatLogger.Info("TimeStamp: %s, BucketName: %s, servers: %s, StatisticRate: %.1f, StatisticConn: %.1f, StatisticConnMax: %.1f, StatisticQps: %s, AssignRate: %.1f, AssignConn: %.1f, AssignQps: %.1f, ExpectRate: %.1f, ExpectConn: %.1f, ExpectQps: %.1f",
                     stime, key, servers, value.StatisticBucketRate, value.StatisticBucketConn, value.StatisticBucketConnMax,
                     string(qps[:len(qps)]),
                     value.AssignedBucketRate, value.AssignedBucketConn, value.AssignedBucketQps,
                     value.ExpectedBucketRate, value.ExpectedBucketConn, value.ExpectedBucketQps)

        /* send data to local database */
        {
            var item [11]uint64
            stime := value.TimeStamp
            bname := value.BucketName

            // rate := uint64(value.StatisticBucketRate)
            item[0] = uint64(value.StatisticBucketRate)
            item[1] = uint64(value.StatisticBucketConn)
            item[2] = uint64(value.StatisticBucketConnMax)
            item[3] = uint64(value.StatisticBucketQps.QPSTotal)
            item[4] = uint64(value.StatisticBucketQps.QPSTotalFailed)
            item[5] = uint64(value.StatisticBucketQps.QPSGet)
            item[6] = uint64(value.StatisticBucketQps.QPSPut)
            item[7] = uint64(value.StatisticBucketQps.QPSDelete)
            item[8] = uint64(value.StatisticBucketQps.QPSList)
            item[9] = uint64(value.StatisticBucketQps.QPSVideo)
            item[10] = uint64(value.StatisticBucketQps.QPSImage)
            // item := make([]uint64, 0)
            // item  = append(item, rate, conn, tqps, gqps, pqps, dqps, lqps, vqps, iqps)
            PgInsert(stime, bname, servers, item)
        }


        // 检查流量是否超过报警阈值，如果是，根据配置发送报警
        errMsg, over := CheckQuota(key, value.StatisticBucketRate, value.StatisticBucketConnMax,
                                         value.StatisticBucketQps.QPSTotal, 0)
        if over {
            SendWarn("Bucket: " + key + errMsg)
            // SendWarn(errMsg)
        }

        value.TimeStamp  = 0
        value.StatisticBucketRate = 0
        value.StatisticBucketConn = 0
        value.StatisticBucketConnMax = 0
        initQPS(&(value.StatisticBucketQps))

        value.AssignedBucketRate = 0
        value.AssignedBucketConn = 0
        value.AssignedBucketQps  = 0

        value.ExpectedBucketRate = 0
        value.ExpectedBucketConn = 0
        value.ExpectedBucketQps  = 0

        value.reportServerNum = 0
        value.reportDone = false
    }

    // 时间窗口向前滑动
    oldCurrts := windows[gCurr].TimeStamp
    oldPrevts := windows[gPrev].TimeStamp
    oldNextts := windows[gNext].TimeStamp
    gCurr = (gCurr + 1) % WINDOW_SIZE
    windows[gCurr].TimeStamp = oldCurrts + DURATION
    gPrev = (gPrev + 1) % WINDOW_SIZE
    windows[gPrev].TimeStamp = oldPrevts + DURATION
    gNext = (gNext + 1) % WINDOW_SIZE
    windows[gNext].TimeStamp = oldNextts + DURATION

    sts := time.Unix(windows[gCurr].TimeStamp, 0).Format("2006-01-02 15:04:05")
    GLogger.Debug("Stat window go forward, current timestamp: %s", sts)

    return true
}



// 更新内存总流量、总连接数、总QPS等统计数据
func updateTotalStatistic(bucketStatistic BucketStatistic) {
    update := false
    if TotalStatisticRing == nil {
        TotalStatisticRing = ring.New(NUM)
    }

    curRate, curConn, curQps := 0.0, 0.0, 0.0
    TotalStatisticRing.Do(func (p interface {}) {
        if p != nil {
            bs := p.(*BucketStatistic)
            if bs.TimeStamp == bucketStatistic.TimeStamp {
                //oldRate, oldConn, oldQps := bs.StatisticBucketRate, bs.StatisticBucketConn, bs.StatisticBucketQps.QPSTotal
                bs.StatisticBucketRate += bucketStatistic.StatisticBucketRate
                bs.StatisticBucketConn += bucketStatistic.StatisticBucketConn
                bs.StatisticBucketQps.QPSTotal  += bucketStatistic.StatisticBucketQps.QPSTotal
                update = true
                curRate = bs.StatisticBucketRate
                curConn = bs.StatisticBucketConn
                curQps  = bs.StatisticBucketQps.QPSTotal
                // ts := time.Unix(bs.TimeStamp, 0).Format("15:04:05")
                // fmt.Printf("UpDate from Bucket: %s. timeStamp: %s. Old:{Rate: %.1f, Conn: %.1f, Qps: %.1f}, New:{Rate: %.1f, Conn: %.1f, Qps: %.1f}\n", bucketStatistic.BucketName, ts, oldRate, oldConn, oldQps, bs.StatisticBucketRate, bs.StatisticBucketConn, bs.StatisticBucketQps.QPSTotal)
            }
        }
    })
    if update == false {
        v := new (BucketStatistic)
        v.BucketName = bucketStatistic.BucketName
        v.TimeStamp  = bucketStatistic.TimeStamp
        v.StatisticBucketRate = bucketStatistic.StatisticBucketRate
        v.StatisticBucketConn = bucketStatistic.StatisticBucketConn
        v.StatisticBucketQps  = bucketStatistic.StatisticBucketQps

        TotalStatisticRing.Value = v
        TotalStatisticRing = TotalStatisticRing.Next()
        curRate = v.StatisticBucketRate
        curConn = v.StatisticBucketConn
        curQps  = v.StatisticBucketQps.QPSTotal

        // ts := time.Unix(v.TimeStamp, 0).Format("15:04:05")
        // fmt.Printf("Init from Bucket: %s. timeStamp: %s. value:{Rate: %.1f, Conn: %.1f, Qps: %.1f}\n",
        //             v.BucketName, ts, v.StatisticBucketRate, v.StatisticBucketConn, v.StatisticBucketQps.QPSTotal)
    }

    // 更新完成后应该要检查是否超过报警阈值，如果是，则需要报警
    errMsg, over := CheckQuota("TotalStatistic", curRate, curConn, curQps, 0)
    if over {
        SendWarn("NOS Total" + errMsg)
    }
}


/*
** 更新内存桶的聚合统计数据,以便通过WEB页面展示
*/
func updateDisplayData(bucketStatistic BucketStatistic, bucketStat *BucketState) {
    update := false
    ringBuffer, ok := ringmap[bucketStatistic.BucketName]
    if !ok {
        ringBuffer = ring.New(NUM)
        ringmap[bucketStatistic.BucketName] = ringBuffer
        goto NEW
    }
    // 遍历ring，查看是否需要替换为新聚合的数据，如果不需要替换，说明这是一次新的统计数据
    ringBuffer.Do(func (p interface {}) {
        if p != nil {
            bs := p.(*BucketStatistic)
            if bs.TimeStamp == bucketStat.TimeStamp {
                bs.StatisticBucketRate = bucketStat.StatisticBucketRate
                bs.StatisticBucketConn = bucketStat.StatisticBucketConn
                bs.StatisticBucketConnMax = bucketStat.StatisticBucketConnMax
                bs.StatisticBucketQps.QPSTotal  = bucketStat.StatisticBucketQps.QPSTotal
                bs.StatisticBucketQps.QPSTotalFailed  = bucketStat.StatisticBucketQps.QPSTotalFailed
                bs.StatisticBucketQps.QPSGet    = bucketStat.StatisticBucketQps.QPSGet
                bs.StatisticBucketQps.QPSPut    = bucketStat.StatisticBucketQps.QPSPut
                bs.StatisticBucketQps.QPSDelete = bucketStat.StatisticBucketQps.QPSDelete
                bs.StatisticBucketQps.QPSList   = bucketStat.StatisticBucketQps.QPSList
                bs.StatisticBucketQps.QPSVideo  = bucketStat.StatisticBucketQps.QPSVideo
                bs.StatisticBucketQps.QPSImage  = bucketStat.StatisticBucketQps.QPSImage

                bs.AssignedBucketRate  = bucketStat.AssignedBucketRate
                bs.AssignedBucketConn  = bucketStat.AssignedBucketConn
                bs.AssignedBucketQps   = bucketStat.AssignedBucketQps

                bs.ExpectedBucketRate  = bucketStat.ExpectedBucketRate
                bs.ExpectedBucketConn  = bucketStat.ExpectedBucketConn
                bs.ExpectedBucketQps   = bucketStat.ExpectedBucketQps
                update = true
            }
        }
    })

NEW:
    if update == false {
        v := new (BucketStatistic)
        v.BucketName = bucketStat.BucketName
        v.TimeStamp  = bucketStat.TimeStamp
        v.StatisticBucketRate = bucketStat.StatisticBucketRate
        v.StatisticBucketConn = bucketStat.StatisticBucketConn
        v.StatisticBucketConnMax = bucketStat.StatisticBucketConnMax
        v.StatisticBucketQps  = bucketStat.StatisticBucketQps

        v.AssignedBucketRate = bucketStat.AssignedBucketRate
        v.AssignedBucketConn = bucketStat.AssignedBucketConn
        v.AssignedBucketQps  = bucketStat.AssignedBucketQps

        v.ExpectedBucketRate = bucketStat.ExpectedBucketRate
        v.ExpectedBucketConn = bucketStat.ExpectedBucketConn
        v.ExpectedBucketQps  = bucketStat.ExpectedBucketQps

        ringBuffer.Value = v
        ringBuffer = ringBuffer.Next()
        ringmap[bucketStat.BucketName] = ringBuffer
    }

    // 更新NOS总体下载流量、qps、连接数等统计数据
    updateTotalStatistic(bucketStatistic)
    return
}


/*
** 从管道取出Nginx推送的统计数据并进行统计数据聚合
*/
func ringManager(UDPRingChan chan BucketStatistic) {

    var res              bool
    var bsValid          bool
    var window           int
	var bucketStatistic  BucketStatistic
    var bucketStat       *BucketState = nil

	GLogger.Info("Start ringManager")
	ringmap = make(map[string]*ring.Ring)

    // 使用第一个统计数据作为时间窗口的初始值
    bucketStatistic = <-UDPRingChan
    init_stat_windows(&bucketStatistic)

	for {
		bucketStatistic = <-UDPRingChan
        //fmt.Println("Recive BucketStatistic: ", bucketStatistic)

        // 如果超过10s钟没有窗口滑动，那么表明可能中断过
        // 需要重新初始化滑动窗口,当然这属于极小概率事件
        // 如果重新初始化了，那么就继续下一次
        dur := time.Since(LastUpdate)
        if dur.Seconds() > 10 {
            init_stat_windows(&bucketStatistic)
            bucketStat = windows[gCurr].windowData[bucketStatistic.BucketName]
            goto MEM_UP
        }

        // 统计数据有效性判断
        bsValid, window = isBucStatisticValid(bucketStatistic)
        if bsValid == false {
            stime := time.Unix(bucketStatistic.TimeStamp, 0).Format("2006-01-02 15:04:05")
            GErrorLogger.Error("Invalid BucketStatistic: TimeStamp: %s, BucketName:%s, Server: %s",
                             stime, bucketStatistic.BucketName, bucketStatistic.ServerAddr)
            continue
        }

        // 当前收到的统计数据已经位于NEXT窗口，则进行窗口滑动
        if window == gNext {
            window_go_forward()
        }

        // 统计数据聚合
        bucketStat = windows[window].windowData[bucketStatistic.BucketName]
        if bucketStat == nil {
            bucketStat = new(BucketState)
            bucketStat.BucketName = bucketStatistic.BucketName
            bucketStat.TimeStamp = bucketStatistic.TimeStamp
            bucketStat.reportServerNum = 0
            bucketStat.reportDone = false
            windows[window].windowData[bucketStat.BucketName] = bucketStat
        }
        res = updateBucketStat(&bucketStatistic, bucketStat)
        if res == false {
            continue
        }
MEM_UP:
        // 更新内存统计数据之前需要加写锁
        // 更新完成后再释放
        rwLocker.Lock()
        // 内存统计数据聚合
        updateDisplayData(bucketStatistic, bucketStat)
        rwLocker.Unlock()
	}

	GLogger.Info("ringManager exit")
	time.Sleep(1000 * time.Millisecond)
}




// 监听端口，接收Nginx发送的统计数据
func UdpServer(UDPRingChan chan BucketStatistic) {
	GLogger.Info("Start udpServer port %s", NgxStatPort)

    s, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
    if err != nil {
		GLogger.Error("socket fail %s", err)
        return
    }
    err = syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
    if err != nil {
		GLogger.Error("set SO_REUSEADDR fail %s", err)
        return
    }

    port, _ := strconv.Atoi(NgxStatPort)
    lsa := &syscall.SockaddrInet4{Port: port}
    err = syscall.Bind(s, lsa)
    if err != nil {
        GLogger.Error("Bind UDP port %s fail %s", NgxStatPort, err)
        return
    }

	for {
        var remoteAddr net.UDPAddr;
		data := make([]byte, 4096)
		read, addr, err := syscall.Recvfrom(s, data, 0)
		if err != nil {
			GLogger.Error("RecvFromUDP fail %s", err)
			continue;
		}

        switch addr := addr.(type) {
        case *syscall.SockaddrInet4:
            remoteAddr = net.UDPAddr{IP: addr.Addr[0:], Port: addr.Port}
        case *syscall.SockaddrInet6:
            remoteAddr = net.UDPAddr{IP: addr.Addr[0:], Port: addr.Port}
        }

		var bucketStatistic BucketStatistic
		err = json.Unmarshal(data[:read], &bucketStatistic)
		if err != nil {
			GLogger.Error("json Unmarshal fail!", err)
		}
        bucketStatistic.ServerAddr = remoteAddr.IP.String()


REPEAT:
        log, ok := GServerLog[remoteAddr.IP.String()]
        if !ok {
            InitServerLogger(remoteAddr.IP.String())
            goto REPEAT
        }
        stime := time.Unix(bucketStatistic.TimeStamp, 0).Format("2006-01-02 15:04:05")
        qps, err := json.Marshal(bucketStatistic.StatisticBucketQps)
        if err != nil {
            GErrorLogger.Error("marshal %s failed", bucketStatistic.StatisticBucketQps)
        }

        log.Info("TimeStamp: %s, BucketName: %s, StaticRate: %.1f, StaticConn: %.1f, StaticConnMax: %.1f, StaticQps: %s, AssignRate: %.1f, AssignConn; %.1f, AssignQps: %.1f, ExpectRate: %.1f, ExpectConn: %.1f, ExpectQps: %.1f",
                 stime, bucketStatistic.BucketName, bucketStatistic.StatisticBucketRate,
                 bucketStatistic.StatisticBucketConn, bucketStatistic.StatisticBucketConnMax,string(qps[:len(qps)]),
                 bucketStatistic.AssignedBucketRate, bucketStatistic.AssignedBucketConn, bucketStatistic.AssignedBucketQps,
                 bucketStatistic.ExpectedBucketRate, bucketStatistic.ExpectedBucketConn, bucketStatistic.ExpectedBucketQps)

		UDPRingChan <- bucketStatistic
	}
	GLogger.Info("udpServer exit")
	time.Sleep(1000 * time.Millisecond)
}


/*
** 以下代码为HTTP页面展示桶流量
*/

func drawBucketRate(bucketName string) {
	p, _ := plot.New()
	p.Title.Text = bucketName + " Rate"
	p.X.Label.Text = "time line"
	p.Y.Label.Text = "byte per second"

	count := 0
	ptsExpected  := make(plotter.XYs, NUM)
	ptsAssigned  := make(plotter.XYs, NUM)
	ptsStatistic := make(plotter.XYs, NUM)

    var ringBuffer *ring.Ring = nil
    if bucketName == "TotalStatistic" {
        ringBuffer = TotalStatisticRing
    } else {
	    ringBuffer = getRingBuffer(bucketName)
    }

	ringBuffer.Do(func(p interface{}){
		ptsExpected[count].X = float64(count)
		ptsAssigned[count].X = float64(count)
		ptsStatistic[count].X= float64(count)
		if p != nil {
			ExpectedBucketRate, AssignedBucketRate, StatisticBucketRate :=
						p.(*BucketStatistic).GetBucketRate()

			ptsExpected[count].Y = float64(ExpectedBucketRate)
			ptsAssigned[count].Y = float64(AssignedBucketRate)
			ptsStatistic[count].Y= float64(StatisticBucketRate)
		}
		count = count + 1
	})

	plotutil.AddLinePoints(p,
		"Expected", ptsExpected,
		"Assigned", ptsAssigned,
		"Statistic", ptsStatistic)
	p.Add(plotter.NewGrid())
	if err := p.Save(6, 4, "bucket_rate.png"); err != nil {
		fmt.Println("Save fail")
	}
}


func drawBucketConn(bucketName string) {

	p, _ := plot.New()
	p.Title.Text = bucketName + " Conn"
	p.X.Label.Text = "time line"
	p.Y.Label.Text = "current connections"

	count := 0
	ptsExpected  := make(plotter.XYs, NUM)
	ptsAssigned  := make(plotter.XYs, NUM)
	ptsStatistic := make(plotter.XYs, NUM)
	ptsStatisticMax := make(plotter.XYs, NUM)

    var ringBuffer *ring.Ring = nil
    if bucketName == "TotalStatistic" {
        ringBuffer = TotalStatisticRing
    } else {
	    ringBuffer = getRingBuffer(bucketName)
    }
	// ringBuffer := getRingBuffer(bucketName)
	ringBuffer.Do(func(p interface{}){
		ptsExpected[count].X = float64(count)
		ptsAssigned[count].X = float64(count)
		ptsStatistic[count].X= float64(count)
		ptsStatisticMax[count].X= float64(count)
		if p != nil {
			ExpectedBucketConn, AssignedBucketConn, StatisticBucketConn, StatisticBucketConnMax :=
						p.(*BucketStatistic).GetBucketConn()

			ptsExpected[count].Y = float64(ExpectedBucketConn)
			ptsAssigned[count].Y = float64(AssignedBucketConn)
			ptsStatistic[count].Y= float64(StatisticBucketConn)
			ptsStatisticMax[count].Y= float64(StatisticBucketConnMax)
		}
		count = count + 1
	})

	plotutil.AddLinePoints(p,
		"Expected", ptsExpected,
		"Assigned", ptsAssigned,
		"Statistic", ptsStatistic,
		"StatisticMax", ptsStatisticMax)
	p.Add(plotter.NewGrid())
	if err := p.Save(6, 4, "bucket_conn.png"); err != nil {
		fmt.Println("Save fail")
	}
}


func drawQps(bucketName string, title string, op_type int, save_as string) {
    var e, a, s float64
	P, _ := plot.New()
	P.X.Label.Text = "time line"
	P.Y.Label.Text = title

	count := 0
	ptsExpected  := make(plotter.XYs, NUM)
	ptsAssigned  := make(plotter.XYs, NUM)
	ptsStatistic := make(plotter.XYs, NUM)

    var ringBuffer *ring.Ring = nil
    if bucketName == "TotalStatistic" {
        ringBuffer = TotalStatisticRing
    } else {
	    ringBuffer = getRingBuffer(bucketName)
    }
	//ringBuffer := getRingBuffer(bucketName)
	ringBuffer.Do(func(p interface{}){
		ptsExpected[count].X = float64(count)
		ptsAssigned[count].X = float64(count)
		ptsStatistic[count].X= float64(count)
		if p != nil {
			ExpectedBucketQPS, AssignedBucketQPS, StatisticBucketQPS :=
						p.(*BucketStatistic).GetBucketQPS()

            switch op_type {
            case TOTAL:
	            P.Title.Text = bucketName + " Total QPS"
                //e = ExpectedBucketQPS.QPSTotal
                e = ExpectedBucketQPS
                //a = AssignedBucketQPS.QPSTotal
                a = AssignedBucketQPS
                s = StatisticBucketQPS.QPSTotal
            case TOTAL_FAILED:
	            P.Title.Text = bucketName + " Total Failed QPS"
                //e = ExpectedBucketQPS.QPSTotal
                e = 0.0
                //a = AssignedBucketQPS.QPSTotal
                a = 0.0
                s = StatisticBucketQPS.QPSTotalFailed
            case LIST:
	            P.Title.Text = bucketName + " List QPS"
                //e = ExpectedBucketQPS.QPSList
                //a = AssignedBucketQPS.QPSList
                e = 0
                a = 0
                s = StatisticBucketQPS.QPSList
            case PUT:
	            P.Title.Text = bucketName + " Put QPS"
                //e = ExpectedBucketQPS.QPSPut
                //a = AssignedBucketQPS.QPSPut
                e = 0
                a = 0
                s = StatisticBucketQPS.QPSPut
            case GET:
	            P.Title.Text = bucketName + " Get QPS"
                //e = ExpectedBucketQPS.QPSGet
                //a = AssignedBucketQPS.QPSGet
                e = 0
                a = 0
                s = StatisticBucketQPS.QPSGet
            case DELETE:
	            P.Title.Text = bucketName + " Delete QPS"
                //e = ExpectedBucketQPS.QPSDelete
                //a = AssignedBucketQPS.QPSDelete
                e = 0
                a = 0
                s = StatisticBucketQPS.QPSDelete
            case IMAGE:
	            P.Title.Text = bucketName + " Image QPS"
                //e = ExpectedBucketQPS.QPSImage
                //a = AssignedBucketQPS.QPSImage
                e = 0
                a = 0
                s = StatisticBucketQPS.QPSImage
            case VIDEO:
	            P.Title.Text = bucketName + " Video QPS"
                //e = ExpectedBucketQPS.QPSVideo
                //a = AssignedBucketQPS.QPSVideo
                e = 0
                a = 0
                s = StatisticBucketQPS.QPSVideo
            default:
            }
			ptsExpected[count].Y = float64(e)
			ptsAssigned[count].Y = float64(a)
			ptsStatistic[count].Y= float64(s)

		}
		count = count + 1
	})

	plotutil.AddLinePoints(P,
		"Expected", ptsExpected,
		"Assigned", ptsAssigned,
		"Statistic", ptsStatistic)
	P.Add(plotter.NewGrid())
	if err := P.Save(6, 4, save_as); err != nil {
		fmt.Println("Save fail")
	}
}


func drawBucketQPS(bucketName string) {
    drawQps(bucketName, "total per second", TOTAL, "bucket_qps_total.png")
    if bucketName != "TotalStatistic" {
        drawQps(bucketName, "total failed per second", TOTAL_FAILED, "bucket_qps_total_failed.png")
        drawQps(bucketName, "list per second", LIST, "bucket_qps_list.png")
        drawQps(bucketName, "put per second", PUT, "bucket_qps_put.png")
        drawQps(bucketName, "get per second", GET, "bucket_qps_get.png")
        drawQps(bucketName, "delete per second", DELETE, "bucket_qps_del.png")
        drawQps(bucketName, "image per second", IMAGE, "bucket_qps_image.png")
        drawQps(bucketName, "video per second", VIDEO, "bucket_qps_video.png")
    }
}


func handlerBucket(w http.ResponseWriter, r *http.Request) {
    rwLocker.RLock()
	name := r.FormValue("name")
	drawBucketRate(name)
	drawBucketConn(name)
	drawBucketQPS(name)
    rwLocker.RUnlock()
    url := "http://" + Host + ":" + FileListenPort
    if name != "TotalStatistic" {
        fmt.Fprintf(w, `<table border=0><tr>
            <td><a href="bucket rate" target=_blank><img src="%s/bucket_rate.png"></td>
            <td><a href="bucket conn" target=_blank><img src="%s/bucket_conn.png"></td>
            </tr><tr>
            <td><a href="bucket qps_total" target=_blank><img src="%s/bucket_qps_total.png"></td>
            <td><a href="bucket qps_total_failed" target=_blank><img src="%s/bucket_qps_total_failed.png"></td>
            </tr><tr>
            <td><a href="bucket qps_get" target=_blank><img src="%s/bucket_qps_get.png"></td>
            <td><a href="bucket qps_put" target=_blank><img src="%s/bucket_qps_put.png"></td>
            </tr><tr>
            <td><a href="bucket qps_list" target=_blank><img src="%s/bucket_qps_list.png"></td>
            <td><a href="bucket qps_delete" target=_blank><img src="%s/bucket_qps_del.png"></td>
            </tr><tr>
            <td><a href="bucket qps_image" target=_blank><img src="%s/bucket_qps_image.png"></td>
            <td><a href="bucket qps_video" target=_blank><img src="%s/bucket_qps_video.png"></td>
            </tr>
            </table>`, url, url, url, url, url, url, url, url, url, url)
        } else {
          fmt.Fprintf(w, `<table border=0><tr>
            <td><a href="bucket rate" target=_blank><img src="%s/bucket_rate.png"></td>
            <td><a href="bucket conn" target=_blank><img src="%s/bucket_conn.png"></td>
            </tr><tr>
            <td><a href="bucket qps_total" target=_blank><img src="%s/bucket_qps_total.png"></td>
            </table>`, url, url, url)
        }
}



/* 为了根据桶流量排序
**
*/
type Pair struct {
    Key   string
    Value float64
}

type PairList []Pair
func (p PairList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p PairList) Len() int { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }


/* 根据桶的流量对所有桶(b []string)进行排序
** 排序结果作为返回值
** 注意：函数调用之前确保已经对ringmap加锁了
*/
func sortByRate(b []string) (sorted []string) {
    var bs *BucketStatistic
   // 首先计算每个桶的平均流量值
   m := make(map[string] float64)
   for _, bucket := range b {
       sum, num := 0.0, 0
       r, ok := ringmap[bucket]
       if ok {
           r.Do(func(p interface {}) {
               if p != nil {
                   bs = p.(*BucketStatistic)
                   sum += bs.StatisticBucketRate
                   num = num + 1
               }
           })
           avrage_rate := sum / float64(num)
           m[bucket] = avrage_rate
       }
   }

   // 对map[]中的元素按照Value进行排序  
   // 排序的方法是将map弄成Pair，然后再排序
   p := make(PairList, len(m))
   i := 0
   for k, v := range m {
       p[i] = Pair{k, v}
       i++
   }

   sort.Sort(p)

   for i = (len(p) - 1); i >= 0; i-- {
       sorted = append(sorted, p[i].Key)
   }
   return sorted
}


func handlerAll(w http.ResponseWriter, r *http.Request) {
	var line, lines, out string

    outer := make(map[string]string)

    // 只展示5分钟内活跃的桶
    for key, value := range ringmap {
        ringBuffer := value.Prev()
        ts := (ringBuffer.Value).(*BucketStatistic).GetTimeStamp()
        lastUpdate := time.Unix(ts, 0)
        if time.Since(lastUpdate).Seconds() < 300 {
            outer[key] = lastUpdate.Format("15:04")
        }
    }

    bucketName := make([]string, 0)
    for key, _ := range outer {
        bucketName = append(bucketName, key)
    }

    // 根据桶的一段时间内的平均流量对活跃桶排序
    // 可能还需要对ringmap加读锁
    rwLocker.RLock()
    bucketName = sortByRate(bucketName)
    rwLocker.RUnlock()

    // 首先展示总体的桶流量、连接数、QPS信息
    url := "http://" + Host + ":" + HttpPort
    key := "TotalStatistic"

    t := int64(0)
    if TotalStatisticRing != nil {
        ringBuffer := TotalStatisticRing.Prev()
        if ringBuffer != nil {
            t = (ringBuffer.Value).(*BucketStatistic).GetTimeStamp()
        }
    }

    lastUpdate := time.Unix(t, 0).Format("15:04")

    tRateQ, tConnQ, tQpsQ , _:= GetQuota("TotalStatistic", 0)
    stRateQ := strconv.FormatInt(tRateQ, 10)
    stConnQ := strconv.FormatInt(tConnQ, 10)
    stQpsQ := strconv.FormatInt(tQpsQ, 10)
    lines = fmt.Sprintf(`<p><a href=%s/bucket?name=%s>%s</a> Update %s <a href=%s/quota?warn=true&name=%s&rate=%s&conn=%s&qps=%s>%s </p>`, url, key, key, lastUpdate, url, key, stRateQ, stConnQ, stQpsQ, "WarnQuota")


    // 展示每个桶的流量等统计信息
    for _, key := range bucketName {
        value, _ := outer[key]
        rateQuota, connQuota, qpsQuota, connRate := GetQuota(key, 0)
        swRate := strconv.FormatInt(rateQuota, 10)
        swConn := strconv.FormatInt(connQuota, 10)
        swQps  := strconv.FormatInt(qpsQuota,  10)
        swconnRate  := strconv.FormatInt(connRate,  10)

        rateQuota, connQuota, qpsQuota, connRate = GetQuota(key, 1)
        slRate := strconv.FormatInt(rateQuota, 10)
        slConn := strconv.FormatInt(connQuota, 10)
        slQps  := strconv.FormatInt(qpsQuota,  10)
        slconnRate  := strconv.FormatInt(connRate,  10)

        url := "http://" + Host + ":" + HttpPort

        line = fmt.Sprintf(`<p><a href=%s/bucket?name=%s>%s</a><a>  Update %s</a>  <a href=%s/quota?warn=true&name=%s&rate=%s&conn=%s&qps=%s&connrate=%s>%s  </a><a href=%s/quota?limit=true&name=%s&rate=%s&conn=%s&qps=%s&connrate=%s>%s</a></p>`, url, key, key, value, url, key, swRate, swConn, swQps, swconnRate, "WarnQuota", url, key, slRate, slConn, slQps, slconnRate, "LimitQuota")
		lines += line
    }

	active := fmt.Sprintf("<p><b>当前(5分钟)活跃桶数量 %d</b></p>", len(bucketName))
	out = "<html><body>" + active + "<p><b>点击查看详情<b></p>" + lines + "</body></html>"
	fmt.Fprintf(w, out)
}


func StaticServer() {
    port := ":" + FileListenPort
    err := http.ListenAndServe(port, http.FileServer(http.Dir("./")))
    if err != nil {
        GErrorLogger.Error("Listen file server error: %s", err)
        panic("listen file server")
    }
}


/*
** WEB页面展示桶一段时间内的流量信息
*/
func HttpServer() {
	GLogger.Info("Start httpServer port %s", HttpPort)
	http.HandleFunc("/all", handlerAll)
	http.HandleFunc("/bucket", handlerBucket)
	http.HandleFunc("/quota", QuotaSet)
    port := ":" + HttpPort
    err := http.ListenAndServe(port, nil)
    if err != nil {
        GErrorLogger.Error("HTTP server error: %s", err)
        panic("http server\n")
    }
}
