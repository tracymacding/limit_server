package main

import(
    "os"
    "io"
    "strings"
    "bufio"
    "strconv"
)


var Host                   string
var HttpPort               string
var NgxStatPort            string
var FileListenPort         string
var LimitListWarnThreahold int
var RateAlarmThreshold     int
var ConnAlarmThreshold     int
var QpsAlarmThreshold      int


var Nginxs []string

/* 从原始字符串(可能包含" ")中提取出有效字符串
** 如果全是" "，返回nil
*/
func getValue(raw string, index int, delimter string)(string) {
    var res []byte
    res = nil

    ok := false

    for i := index; i < len(raw); i++ {
        if string(raw[i]) != delimter {
            if res == nil {
                res = make([]byte, 0)
            }
            res = append(res, raw[i])
            ok = true
        }
    }
    var s string
    if ok {
        s = string(res)
    } else {
        s = ""
    }
    return s
}




// 主配置文件解析limit.conf
func parseConfig(line []byte)(bool) {
    s := string(line)

    // 空行或者"#"开头的行，直接忽略即可
    if len(s) == 0 || s[0] == '#' {
        return true
    }

    /* 获取配置项的key，根据key决定如何解析value
    ** key包含如下几种: 1).HttpPort；2).FileListenPort；3).NgxStatPort；4).Nginxs；5).Admins
    ** 配置文件中key和value以","分割，如果value是list，也以","分割
    */
    index := strings.Index(s, " ")
    if index == -1 {
        return false
    }

    key := s[:index]
    if key == "Host" {
        value := getValue(s, index, " ")
        if value == "" {
            return false
        }
        Host = value
    } else if key == "HttpPort" {
        value := getValue(s, index, " ")
        if value == "" {
            return false
        }
        HttpPort = value
    } else if key == "FileListenPort" {
        value := getValue(s, index, " ")
        if value == "" {
            return false
        }
        FileListenPort = value
    } else if key == "NgxStatPort" {
        value := getValue(s, index, " ")
        if value == "" {
            return false
        }
        NgxStatPort = value
    } else if key == "LimitListWarnThreahold" {
        value := getValue(s, index, " ")
        if value == "" {
            return false
        }
        LimitListWarnThreahold, _ = strconv.Atoi(value)
    } else if key == "PushStatDB" {
        value := getValue(s, index, " ")
        if (value != "true" && value != "false") {
            return false
        }
        if value == "true" {
            Needpush = true
        } else {
            Needpush = false
        }
    } else if key == "Nginxs" {
        nginx := getValue(s, index, " ")
        if nginx == "" {
            return false
        }
        Nginxs = append(Nginxs, nginx)
    } else if key == "RateAlarmThreshold"{
        value := getValue(s, index, " ")
        if value == "" {
            return false
        }
        RateAlarmThreshold, _ = strconv.Atoi(value)
    } else if key == "ConnAlarmThreshold"{
        value := getValue(s, index, " ")
        if value == "" {
            return false
        }
        ConnAlarmThreshold, _ = strconv.Atoi(value)
    } else if key == "QpsAlarmThreshold"{
        value := getValue(s, index, " ")
        if value == "" {
            return false
        }
        QpsAlarmThreshold , _ = strconv.Atoi(value)
    }
    return true
}



// 读取主配置文件limit.conf并解析
func LoadConfig() (bool) {

    Nginxs = make([]string, 0)

    HttpPort       = "9090"
    FileListenPort = "9091"
    NgxStatPort    = "7778"
    LimitListWarnThreahold = 0
    RateAlarmThreshold = 52428800
    ConnAlarmThreshold = 50
    QpsAlarmThreshold  = 200

    f, err := os.Open("./conf/limit.conf")
    if err != nil {
        GErrorLogger.Error("open conf file[%s] failed: [%s]", "./conf/limit.conf", err)
        return false
    }

    r := bufio.NewReader(f)
    // 逐行读取配置文件，并解析
    for {
        buf, _, err := r.ReadLine()
        if(err == io.EOF) {
            GLogger.Info("read conf file [%s] done", "./conf/limit.conf")
            break
        }
        if (err != nil) {
            GErrorLogger.Error("read conf file [%s] failed: [%s]", "./conf/limit.conf", err)
            return false
        }
        ret := parseConfig(buf)
        if ret != true {
            return ret
        }
    }
    return true
}

