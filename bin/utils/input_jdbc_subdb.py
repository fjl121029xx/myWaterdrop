#!/usr/bin/env python
# coding:utf-8

import MySQLdb
import sys
import ConfigParser


# 获取url and db type:tuple
def get_db_info(host, port, user, passwd, dbname):
    conn = MySQLdb.connect(host=host, port=int(port), user=user, passwd=passwd, charset='utf8')
    cursor = conn.cursor()

    sql = 'select distinct TABLE_SCHEMA from information_schema.TABLES where TABLE_SCHEMA like "%s%%"' % dbname

    cursor.execute(sql)
    fetchall = cursor.fetchall()
    cursor.close()

    return fetchall


def echo_input_jdbc(dbtp, url, user, passwd, query_type, query):
    for item in dbtp:
        print '    jdbc{'
        print '        host = "%s"' % url
        print '        database = "%s"' % item[0]
        print '        username = "%s"' % user
        print '        password = "%s"' % passwd
        print '        query.type = "%s"' % query_type
        print '        %s' % query
        print '    }'


if __name__ == '__main__':

    cf = ConfigParser.RawConfigParser()
    cf.read(sys.argv[1])

    db_host = cf.get("db", "host")
    db_port = cf.get("db", "port")
    db_user = cf.get("db", "user")
    db_passwd = cf.get("db", "passwd")
    db_name_prefix = cf.get("db", "db_prefix")

    dbs = get_db_info(db_host, db_port, db_user, db_passwd, db_name_prefix)

    query_type = cf.get("input", "type")
    input_query = cf.get("input", "table_or_sql")
    print

    if query_type == "table":
        if (cf.has_option("input", "exclude_table")):
            query = 'query.table.include = "%s"\n        query.table.exclude = "%s"' % (
                input_query, cf.get("input", "exclude_table"))
        else:
            query = 'query.table.include = "%s"' % input_query

    else:
        query_type = 'query.type = "sql"'
        query = 'query.sql = "%s"' % input_query

    print "input{"
    echo_input_jdbc(dbs, str(db_host) + ":" + str(db_port), cf.get("db", "user"), cf.get("db", "passwd"),
                    query_type, query)
    print "}"
