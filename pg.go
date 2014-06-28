package main

import (
    "database/sql"
    _ "github.com/lib/pq"
)


var DBInit bool = false
var dbins *sql.DB = nil
var Needpush bool = false


func PgInit() (bool) {
    if Needpush {
        var err error
        dbins, err = sql.Open("postgres", "user=nos host=127.0.0.1 dbname=nosstats password=pwdnosstats sslmode=disable")
        if err != nil {
            GErrorLogger.Error("open postgresql error: %s", err)
            goto RET
        }
    }
    DBInit = true;
RET:
    return DBInit
}


func PgInsert(stime int64, bname string, sserv string, items [11]uint64) {
    if Needpush {
        if DBInit == false {
            GErrorLogger.Error("database not initialized, please check")
            if !PgInit() {
                return
            }
        }

        _, err := dbins.Exec("INSERT INTO stat_buckets(stat_time, bucket_name, servers, items) VALUES($1, $2, $3, ARRAY[$4::bigint,$5::bigint,$6::bigint,$7::bigint,$8::bigint,$9::bigint,$10::bigint,$11::bigint,$12::bigint, $13::bigint, $14::bigint])",stime, bname, sserv, items[0], items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], items[9], items[10])
        if err != nil {
            GErrorLogger.Error("insert record into database failed, error: %s", err)
        }
    }
}


func PgStop() {
    if Needpush {
        dbins.Close()
        DBInit = false
    }
}

