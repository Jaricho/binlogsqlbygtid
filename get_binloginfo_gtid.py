#!/usr/bin/env python
#-*- encoding: utf8 -*-


import sys
reload(sys)
sys.setdefaultencoding('utf8')
import datetime
import MySQLdb
from binloginfo_gtid_util import command_line_args, ssh_outs

class Binloginfo(object):

    def __init__(self, connect_string,server_user, server_password, server_uuid, transno):

        self.connect_string = connect_string
        self.server_user = server_user
        self.server_password = server_password
        self.server_uuid = server_uuid
        self.transno = transno
        # connection
        self.connection = MySQLdb.connect(**self.connect_string)
        #result
        self.querysql = ''
        self.querytype = ''
        self.querymessage = ''
        self.querystatus = True
        self.queryresult = {'querytype': self.querytype, 'querysql': self.querysql, 'querymessage': self.querymessage, 'querystatus':self.querystatus}

    def get_binlog_file_pos(self):
        with self.connection as cursor:
            cursor.execute("show global variables like 'log_bin_basename';")
            binlogfilepos = cursor.fetchone()[1]
            return binlogfilepos[0:binlogfilepos.rfind('/')+1]

    def get_binlog_file(self):
        with self.connection as cursor:
            cursor.execute("show binary logs;")
            binlogfilelist = cursor.fetchall()
            for binlogfile in reversed(binlogfilelist):
                cursor.execute("show binlog events in '{0}' limit 3;".format(binlogfile[0]))
                binlogevents = cursor.fetchall()
                for item in binlogevents:
                    if item[2] == 'Previous_gtids':
                        if item[5]:
                            gtidlist = item[5].replace('\n', '').split(',')
                            for gtid in gtidlist:
                                if self.server_uuid == gtid.split(':')[0]:
                                    if len(gtid.split(':')[1].split('-')) == 1:
                                        previousgtidendpos = gtid.split(':')[1]
                                    else:
                                        previousgtidendpos = gtid.split(':')[1].split('-')[1]
                                    if long(previousgtidendpos) < long(self.transno):
                                        return binlogfile[0]
                                    else:
                                        continue
                        else:
                            return  binlogfile[0]

    def get_binlog_transaction_info(self):
        binlogfile = self.get_binlog_file()
        if not binlogfile:
            self.queryresult['querystatus'] = False
            self.queryresult['querymessage'] = 'Can not found binlog file ,please check binlog file has purged or previous gtid is empty or else'
            print self.queryresult
            return self.queryresult

        binlogfilepos = self.get_binlog_file_pos()
        if not binlogfilepos:
            self.queryresult['querystatus'] = False
            self.queryresult['querymessage'] = 'Can not found binlog file positsion,please check'
            print self.queryresult
            return self.queryresult

        cmd = "var=`which mysqlbinlog`;sudo $var -vv --base64-output=DECODE-ROWS {0}{1} --include-gtids={2}:{3} | grep -A 50 'Rows_query'".format(binlogfilepos, binlogfile, self.server_uuid, self.transno)
        result = ssh_outs(self.connect_string['host'], cmd, self.server_user, self.server_password)
        self.queryresult['querytype'] = 'DML'
        if len(result) == 0:
            self.queryresult['querytype'] = 'DDL'
            cmd = "var=`which mysqlbinlog`;sudo $var -vv --base64-output=DECODE-ROWS {0}{1} --include-gtids={2}:{3} | grep -vE '@@|#|/*!*/|DELIMITER'".format(binlogfilepos, binlogfile, self.server_uuid, self.transno)
            result = ssh_outs(self.connect_string['host'], cmd, self.server_user, self.server_password)
        if self.queryresult['querytype'] == 'DDL':
            self.queryresult['querysql'] = ' '.join(str(r).strip() for r in result)
        elif self.queryresult['querytype'] == 'DML':
            querysql = ' '.join(str(r).strip() for r in result if 'Rows_query' not in r)
            self.queryresult['querysql'] = querysql[:querysql.find('# at')].replace('#', '').strip()
        print self.queryresult
        return self.queryresult

if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    connect_string = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    binloginfo = Binloginfo(connect_string=connect_string, server_user=args.server_user, server_password=args.server_password,server_uuid=args.server_uuid, transno=args.transno)
    binloginfo.get_binlog_transaction_info()