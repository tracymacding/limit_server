/* LimitServer桶配额模块，提供以下功能:
** 1.通过WEB页面设置桶配额 & 持久化桶配额
** 2.检查桶流量、连接数QPS是否超过配额
*/

package main

import (
    "io"
    "os"
    "fmt"
    "bufio"
    "strconv"
    "net/http"
    "encoding/json"
)


const (
    ADD   = iota
    DEL   = iota
    RESET = iota
)


type BucketQuota struct {
    BucketName  string
    // 桶配额类型，0:报警配额,1:限速配额
    QuotaType   int64
    RateQuota   int64
    ConnQuota   int64
    QpsQuota    int64
    // 单个连接上的流量配额
    RatePerConn int64
}


type Admin struct {
    User      string
    Password  string
}


// 每个桶目前存在两种配额，第一用于报警，第二用于限速
var QuotaInfo map[string] []*BucketQuota
var admins    map[string] string


/* 更新磁盘配额，步骤：
** 1.创建新的配额文件quota.new并将内存配额数据写入其中
** 2.删除原始配额文件quota
** 3.将quota.new重命名为quota
*/
func updateDiskQuota() {
    fname := "./conf/quota"
    newfile := fname + ".new"
    var b []byte

    // 创建新配额文件quota.new
    f, err := os.OpenFile(newfile, os.O_RDWR|os.O_CREATE, 0666)
    if err != nil {
        GErrorLogger.Error("open quota.new for writing failed: [%s]", err)
        goto RET;
    }

    // 配额信息json格式化并写入quota.new,每个桶的配额信息写入一行
    for key, value := range QuotaInfo {
        for _, v := range value {
            if v == nil {
                continue
            }
            b, err = json.Marshal(v)
            if err != nil {
                GErrorLogger.Error("json Marshal[%s]failed: [%s]", key, err)
                continue
            }
            _, err = f.Write(b)
            if err != nil {
                GErrorLogger.Error("write [%s] failed: [%s]", key, err)
            }
            _,err = f.WriteString("\n")
        }
    }

    // 删除原始quota文件
    err = os.Remove(fname)
    if err != nil && os.IsNotExist(err){
        GErrorLogger.Error("delete file quota: [%s]", err)
    }

    os.Rename(newfile, fname)
RET:
    return
}


/*
** 从form中获取桶名、流量、连接数、QPS信息
** 返回值：errMsg保存错误信息,""代表正确
**         "xxxx"代表出错原因
*/
func getParFromReq(r *http.Request) (int, int, int, int, string) {
    var rate     int
    var conn     int
    var qps      int
    var rateConn int
    var err      error

    errMsg := ""
    warn := false

    if r.Form.Get("Warn") == "true" {
        warn = true
    }

    rate, err = strconv.Atoi(r.Form.Get("Rate"))
    if err != nil {
        errMsg = "请检查流量设置，确保输入的是数字"
        goto RET
    }
    if rate < 0 {
        errMsg = "流量范围为[0, 无穷大)"
        goto RET
    }

    conn, err = strconv.Atoi(r.Form.Get("Connection"))
    if err != nil {
        errMsg = "请检查连接数设置，确保输入的是数字"
        goto RET
    }
    if conn < 0 {
        errMsg = "连接数范围为[0, 无穷大)"
        goto RET
    }

    qps, err = strconv.Atoi(r.Form.Get("QPS"))
    if err != nil {
        errMsg = "请检查QPS设置，确保输入的是数字"
        goto RET
    }
    if qps < 0 {
        errMsg = "QPS范围为[0, 无穷大)"
        goto RET
    }

    if warn == false {
        rateConn, err = strconv.Atoi(r.Form.Get("RatePerConn"))
        if err != nil {
            errMsg = "请检查RatePerConn设置，确保输入的是数字"
            goto RET
        }
        if rateConn < 0 {
            errMsg = "RatePerConn范围为[0, 无穷大)"
            goto RET
        }
    }
RET:
    return rate, conn, qps, rateConn, errMsg
}


// 管理员身份检查
func checkLogin(w http.ResponseWriter, r *http.Request) {
    warn := r.FormValue("Warn")
    bucket := r.FormValue("Bucket")
    rate := r.FormValue("Rate")
    conn := r.FormValue("Connection")
    qps  := r.FormValue("QPS")
    connrate := r.FormValue("RatePerConn")

    user := r.Form.Get("Admin")
    passwd := r.Form.Get("Password")

    ok := true
    errMsg := ""
    jump := ""
    value := ""
    find := false

    if user == "" || passwd == "" {
        errMsg = "用户名或密码不能为空"
        ok = false
        goto RET;
    }

    value, find = admins[user]
    if (!find) || (value != passwd) {
        errMsg = "非管理员登陆"
        ok = false
        goto RET;
    }

    // 验证通过，方可进行配额设置
    errMsg, ok = UpdateQuota2(w, r, RESET)

RET:
    // 如果设置成功，跳转至主页面
    // 上述步骤失败，跳转至配额设置页面
    if !ok {
        if bucket == "" {
            bucket = "TotalStatistic"
        }
        if warn == "true" {
            jump = fmt.Sprintf(`<html><head><meta http-equiv="refresh" content="1; url=http://%s:%s/quota?warn=true&name=%s&rate=%s&qps=%s&conn=%s" /></head><body>%s</body></html>`, Host, HttpPort, bucket, rate, qps, conn, errMsg)
        } else {
            jump = fmt.Sprintf(`<html><head><meta http-equiv="refresh" content="1; url=http://%s:%s/quota?limit=true&name=%s&rate=%s&qps=%s&conn=%s&connrate=%s" /></head><body>%s</body></html>`, Host, HttpPort, bucket, rate, qps, conn, connrate, errMsg)
        }
    } else {
        jump = fmt.Sprintf(`<html><head><meta http-equiv="refresh" content="2; url=http://%s:%s/all" /></head><body>OK!</body></html>`, Host, HttpPort)
    }
    fmt.Fprintf(w, jump)
}


// Quota Web页面入口
func QuotaSet(w http.ResponseWriter, r *http.Request) {
    warn := r.FormValue("warn")
    name := r.FormValue("name")
    rate := r.FormValue("rate")
    conn := r.FormValue("conn")
    qps  := r.FormValue("qps")
    connrate  := r.FormValue("connrate")

    if r.Method == "GET" {
        if name == "TotalStatistic" {
            fmt.Fprintf(w, `<html>
                <head><meta charset=utf-8></head>
                <body>
                <form action="/checkLogin" method="post">
                <table border="0">
                <thead valign="middle">
                <tr><td><strong><font size="4">Total Warn Quota Set</font></strong></td></tr>
                </thead>
                <tr><td>Rate(B/s)</td><td><input type="text" name="Rate" value=%s></input></td></tr>
                <tr><td>QPS</td><td><input type="text" name="QPS" value=%s></input></td></tr>
                <tr><td>Connection</td><td><input type="text" name="Connection" value=%s></input></td></tr>
                <tr><td>Admin</td><td><input type="text" name="Admin"></input></td></tr>
                <tr><td><input type="hidden" name="Warn" value=%s></input></td></tr>
                <tr><td>Password</td><td><input type="password" name="Password"></input></td></tr>
                <tr><td><input type="submit" name="设置"></input></td></tr>
                </table>
                </form>
                </body>
                </html>`, rate, qps, conn, warn)
          } else if warn == "true" {
              fmt.Fprintf(w, `<html>
                <head><meta charset=utf-8></head>
                <body>
                <form action="/checkLogin" method="post">
                <table border=0>
                <thead>
                <tr><td><strong><font size="4">Bucket Warn Quota Set</font></strong></td></tr>
                </thead>
                <tr><td>Bucket</td><td><input type="text" name="Bucket" value=%s></input></td></tr>
                <tr><td>Rate(B/s)</td><td><input type="text" name="Rate" value=%s></input></td></tr>
                <tr><td>QPS</td><td><input type="text" name="QPS" value=%s></input></td></tr>
                <tr><td>Connection</td><td><input type="text" name="Connection" value=%s></input></td></tr>
                <tr><td>Admin</td><td><input type="text" name="Admin"></input></td></tr>
                <tr><td><input type="hidden" name="Warn" value=%s></input></td></tr>
                <tr><td>Password</td><td><input type="password" name="Password"></input></td></tr>
                <tr><td><input type="submit" name="设置"></input></td></tr>
                </table>
                </form>
                </body>
                </html>`, name, rate, qps, conn, warn)
            } else {
              fmt.Fprintf(w, `<html>
                <head><meta charset=utf-8></head>
                <body>
                <form action="/checkLogin" method="post">
                <table border=0>
                <thead>
                <tr><td><strong><font size="4">Bucket Limit Quota Set</font></strong></td></tr>
                </thead>
                <tr><td>Bucket</td><td><input type="text" name="Bucket" value=%s></input></td></tr>
                <tr><td>Rate(B/s)</td><td><input type="text" name="Rate" value=%s></input></td></tr>
                <tr><td>QPS</td><td><input type="text" name="QPS" value=%s></input></td></tr>
                <tr><td>Connection</td><td><input type="text" name="Connection" value=%s></input></td></tr>
                <tr><td>RatePerConn(B/s)</td><td><input type="text" name="RatePerConn" value=%s></input></td></tr>
                <tr><td>Admin</td><td><input type="text" name="Admin"></input></td></tr>
                <tr><td><input type="hidden" name="Warn" value=%s></input></td></tr>
                <tr><td>Password</td><td><input type="password" name="Password"></input></td></tr>
                <tr><td><input type="submit" name="设置"></input></td></tr>
                </table>
                </form>
                </body>
                </html>`, name, rate, qps, conn, connrate, warn)
            }
    } else {
        checkLogin(w, r)
    }
}


// 获取桶@bucketName的配额
func GetQuota(bucketName string, qt int)(int64, int64, int64, int64) {
    rateQuota, connQuota, qpsQuota, connRateQuota := int64(0), int64(0), int64(0), int64(0)
    if qt == 0 {
       rateQuota, connQuota, qpsQuota = int64(RateAlarmThreshold), int64(ConnAlarmThreshold), int64(QpsAlarmThreshold)
    }

    value, ok := QuotaInfo[bucketName]
    if ok && value[qt] != nil {
        rateQuota = value[qt].RateQuota
        connQuota = value[qt].ConnQuota
        qpsQuota  = value[qt].QpsQuota
        qpsQuota  = value[qt].QpsQuota
        connRateQuota  = value[qt].RatePerConn
    }
    return rateQuota, connQuota, qpsQuota, connRateQuota
}


func getQuotaType(warn string) (int) {
    var qtype int
    if warn == "true" {
        qtype = 0
    } else {
        qtype = 1
    }

    return qtype;
}


func UpdateQuota2(w http.ResponseWriter, r *http.Request, op int ) (errMsg string, ok bool){
    var rate int
    var conn int
    var qps  int
    var ratePerConn int
    var err  string
    var quotaType int
    var quota *BucketQuota
    var quotarray []*BucketQuota
    ok = true
    needDel := false
    errMsg = "恭喜，设置成功！"

    r.ParseForm()

    // 获取配额类型,0:报警配额,1:限速配额
    quotaType = getQuotaType(r.Form.Get("Warn"))

    // 如果bucketName为空，意味着是整体流量报警信息设置
    // 需要作一些特殊处理
    bucket := r.Form.Get("Bucket")
    if bucket == "" {
        /*
        errMsg = "请填写桶名"
        ok = false
        goto RET
        */
        bucket = "TotalStatistic"
        quotaType = getQuotaType("true")
    }

    quotarray, ok = QuotaInfo[bucket]
    switch op {
    case RESET:
        if !ok {
            quotarray = make([]*BucketQuota, 2)
            QuotaInfo[bucket] = quotarray
            ok = true
        }
        if quotarray[quotaType] == nil {
            quota = new(BucketQuota)
            quota.BucketName = bucket
            quota.QuotaType = int64(quotaType)
            quotarray[quotaType] = quota
        }

        rate, conn, qps, ratePerConn, err = getParFromReq(r)
        if err != "" {
            errMsg = err
            ok = false
            goto RET
        }
        quota = quotarray[quotaType]
        quota.RateQuota   = int64(rate)
        quota.ConnQuota   = int64(conn)
        quota.QpsQuota    = int64(qps)
        quota.RatePerConn = int64(ratePerConn)

        // 如果设置为全0，相当于不限速，从内存限速信息中删除
        if quotaType == 1 && rate == 0 && conn == 0 && qps == 0 && ratePerConn == 0 {
            needDel = true
            quotarray[1] = nil
        }

    default:
        GErrorLogger.Error("Unknown operation, it should be ADD, DEL or RESET")
    }
    // 持久化配额信息
    updateDiskQuota()

    // 如果是设置限速配额，需要更新至所有前端Nginx服务器
    if quotaType == 1 {
        sngx := len(Nginxs)
        for _, v := range Nginxs {
            // 全0就删除Nginx的限速信息
            if needDel {
                DelNginxLimit(v, bucket)
            } else {
                SetNginxLimit(v, bucket, int64(rate / sngx), int64(conn / sngx), int64(qps / sngx), int64(ratePerConn))
            }
        }
    }

RET:
    return;
}


/* 检查桶当前统计值是否超过配额，超过返回true，并返回报警信息 */
func CheckQuota(bucket string, static_rate float64, static_conn float64, static_qps float64, qt int)(string, bool) {
    var value *BucketQuota
    va, ok := QuotaInfo[bucket]
    compareRate, compareConn, compareQps := float64(RateAlarmThreshold), float64(ConnAlarmThreshold), float64(QpsAlarmThreshold)
    if ok {
        value = va[qt]
        if value != nil {
            compareRate = float64(value.RateQuota)
            compareConn = float64(value.ConnQuota)
            compareQps = float64(value.QpsQuota)
        }
    }

    //fmt.Printf("Bucket: %s, Conn Quota is %d, statistic conn is %.1f\n", bucket, value.ConnQuota, static_conn)
    errMsg := ""
    over := false
    if ok {
        // errMsg += "Bucket"
        // errMsg += fmt.Sprintf("<%s>", bucket)

        // if value != nil && value.RateQuota > 0 && static_rate > float64(value.RateQuota) {
        if  compareRate > 0 && static_rate > compareRate {
            errMsg += " Rate exceeds Quota, Current"
            errMsg += fmt.Sprintf("<%.1f>", static_rate)
            errMsg += ",Quota:"
            errMsg += fmt.Sprintf("<%d>", int64(compareRate))
            over = true
        }
        // if value != nil && value.ConnQuota > 0 && static_conn > float64(value.ConnQuota) {
        if compareConn > 0 && static_conn > compareConn {
            errMsg += " Connection exceeds Quota,Current"
            errMsg += fmt.Sprintf("<%.1f>", static_conn)
            errMsg += ",Quota"
            errMsg += fmt.Sprintf("<%d>", int64(compareConn))
            over = true
        }
        // if value != nil && value.QpsQuota > 0 && static_qps > float64(value.QpsQuota) {
        if compareQps > 0 && static_qps > compareQps {
            errMsg += " QPS exceeds Quota,Current"
            errMsg += fmt.Sprintf("<%.1f>", static_qps)
            errMsg += ",Quota"
            errMsg += fmt.Sprintf("<%d>", int64(compareQps))
            over = true
        }
    }
    return errMsg, over
}


// 加载磁盘桶配额信息
func loadQuota() (bool) {
    // 按行读取桶配额
    f, err := os.Open("./conf/quota")
    if err != nil && !os.IsNotExist(err){
        GErrorLogger.Error("open quota file[%s] failed: [%s]", "./conf/quota", err)
        return true
    }

    r := bufio.NewReader(f)
    for {
        buf, _, err := r.ReadLine()
        if(err == io.EOF) {
            GLogger.Info("read quota file [%s] done", "./conf/quota")
            break
        }
        if (err != nil) {
            GErrorLogger.Error("read quota file [%s] failed: [%s]", "./conf/quota", err)
            break
        }

        quota := new(BucketQuota)
        err = json.Unmarshal(buf, quota)
        if err != nil {
            GErrorLogger.Error("Unmarshal [%s] failed: [%s]", buf, err)
        } else {
            qt := quota.QuotaType
            if(qt != 0 && qt != 1) {
                GErrorLogger.Error("Unknown Quota type: %d, Content: %s", qt, buf)
                continue
            }
            qa, ok := QuotaInfo[quota.BucketName]
            if !ok {
                qa = make([]*BucketQuota, 2)
                QuotaInfo[quota.BucketName] = qa
            }
            qa[qt] = quota
        }
    }

    // 加载管理员信息
    f, err = os.Open("./conf/admins")
    if err != nil {
        GErrorLogger.Error("open admin file[%s] failed: [%s]", "./conf/admins", err)
        return true
    }

    r = bufio.NewReader(f)
    for {
        buf, _, err := r.ReadLine()
        if(err == io.EOF) {
            GLogger.Info("read admin file [%s] done", "./conf/admins")
            break
        }
        if (err != nil) {
            GErrorLogger.Error("read admin file [%s] failed: [%s]", "./conf/admins", err)
            break
        }

        a := new(Admin)
        err = json.Unmarshal(buf, a)
        if err != nil {
            GErrorLogger.Error("Unmarshal [%s] failed: [%s]", buf, err)
        } else {
            admins[a.User] = a.Password
        }
    }
    return true
}


func QuotaInit() {
    QuotaInfo = make(map[string] []*BucketQuota)
    admins    = make(map[string] string)

    ret := loadQuota()
    if !ret {
       panic("load quota file failed")
    }

    http.HandleFunc("/checkLogin", checkLogin)
}
