#!/usr/bin/env python
# coding:utf-8

import MySQLdb
import sys

from numpy.ma import count


class mysql_wraper():
    def __init__(self,host,port,user,passwd,db):
        self.conn = MySQLdb.connect(host=host,
                                    port=int(port),
                                    user=user,
                                    passwd=passwd,
                                    db=db,
                                    charset='utf8')

    def execute_sql(self, sql):

        cursor = self.conn.cursor()
        cursor.execute(sql)
        self.conn.commit()
        return cursor.fetchall()


def tuple_to_dict(tp):
    dt = {}
    for item in fetch:
        dt[item[0].decode("unicode_escape").encode("utf8")] = item[1].decode("unicode_escape").encode("utf8")

    return dt


def to_sparksql_datatype(dt):

    for i in dt:
        if dt[i] in ("tinyint","smallint","mediumint","int"):
            dt[i] = "integer"
        elif dt[i] in ("char","varchar","tinytext","text","mediumtext","longtext"):
            dt[i] = "string"
        elif dt[i] in ("decimal","double"):
            dt[i] = "double"
        elif dt[i] in ("date","time","datetime","timestamp"):
            dt[i] = "timestamp"
        elif dt[i] in ("bigint"):
            dt[i] = "long"

    return dt


if __name__ == '__main__':

    if count(sys.argv) < 7:
        print "usage: <host> <port> <user> <passwd> <db> <table>"
        sys.exit(0)

    host = sys.argv[1]
    port = sys.argv[2]
    user = sys.argv[3]
    passwd = sys.argv[4]
    db = sys.argv[5]
    table = sys.argv[6]

    mw = mysql_wraper(host,port,user,passwd,db)

    fetch = mw.execute_sql("SELECT COLUMN_NAME,DATA_TYPE "
                           "FROM INFORMATION_SCHEMA.COLUMNS "
                           "WHERE table_name = '%s' AND table_schema = '%s'"%(table,db))

    dt = tuple_to_dict(fetch)

    schema = to_sparksql_datatype(dt)

    print schema


