/* LimitServer报警模块,功能：
** 1.报警配置文件解析
** 2.发送报警信息
*/



package main

import(
    "os"
    "io"
    "bufio"
    "net/url"
    "net/http"
)


const (
    POPO = iota
    PHONE
    MAIL
)


var alarmInit bool
var alarmer   map[int][]string


func SendWarn(msg string) {
    uri := ""
    if !alarmInit {
        alarmInit = init_alarm()
        if !alarmInit {
            GErrorLogger.Error("alarmer init failed")
        }
    }

    if alarmInit {
        receiver := alarmer[POPO]
        for _, one := range receiver {
            uri := "http://alert.service.163.org:8182/popo"
            v := url.Values{}
            v.Set("isNormal", "true")
            v.Set("popo", one)
            v.Set("message", msg)

            res_popo, _ := http.PostForm(uri, v)
            if res_popo.StatusCode != 200 {
                GErrorLogger.Error("SEND POPO WARNING FAILED: %d", res_popo.StatusCode)
            }
            defer res_popo.Body.Close()
        }

        receiver = alarmer[MAIL]
        for _, one := range receiver {
            uri = "http://alert.service.163.org:8182/email"
            v := url.Values{}
            v.Set("account", one)
            v.Set("title", "Bucket rate warning")
            v.Set("message", msg)
            res_mail, _ := http.PostForm(uri, v)
            if res_mail.StatusCode != 200 {
                GErrorLogger.Error("SEND EMAIL WARNING FAILED: %d", res_mail.StatusCode)
            }
            defer res_mail.Body.Close()
        }

        receiver = alarmer[PHONE]
        for _, one := range receiver {
            uri := "http://alert.service.163.org:8182/phone"
            v := url.Values{}
            v.Set("isNormal", "true")
            v.Set("phone", one)
            v.Set("message", msg)

            res_phone, _ := http.PostForm(uri, v)
            if res_phone.StatusCode != 200 {
                GErrorLogger.Error("SEND PHONE WARNING FAILED: %d", res_phone.StatusCode)
            }
            defer res_phone.Body.Close()
        }
    }
}


// 读取报警配置文件，解析报警接收者信息
func read_and_parse(fname string, method int) {
    f, err := os.Open(fname)
    if err != nil {
        GErrorLogger.Error("open alarm configure file [%s] failed: [%s]", fname, err)
        return
    }

    r := bufio.NewReader(f)
    receivers := make([]string, 0)
    for {
        buf, _, err := r.ReadLine()
        if(err == io.EOF) {
            break
        }
        if (err != nil) {
            GErrorLogger.Error("read alarm configure file [%s] failed: [%s]", fname, err)
            break
        }

        value := string(buf)
        receivers = append(receivers, value)
    }
    alarmer[method] = receivers
}


func init_alarm() (bool) {
    if alarmInit {
        return true
    }

    alarmer = make(map[int][]string)

    var fname string

    // 每种报警方式对应一个配置文件
    for i := 0; i < 3; i++ {
        switch i {
        case POPO:
            fname = "./conf/alarmer/popo"
        case PHONE:
            fname = "./conf/alarmer/phone"
        case MAIL:
            fname = "./conf/alarmer/mail"
        default:
        }
        read_and_parse(fname, i)
    }
    return true
}
