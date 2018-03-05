#!/usr/bin/env python
# _*_ coding: utf-8 _*_

'''list(delete, modify) the jobname, tasks,'''
import os
import sys
import subprocess
from getopt import getopt, GetoptError
from backend.mongoHelper import MongoDBHelper
from termcolor import colored



def check_exist(job_name,*args):
    for i in args:
        task_name = i
    pass

def usage():
    print """
Usage: nagobah_control [options]

Options:
    Operation:
        -h, --help            show help
        -l, --list            the operate was definated as present a list
        -d, --delete          the operate was definated as delete
    Object:
        --jobname=JOBNAME     define a JOBNAME to Operate
        --taskname=TASK       define a TASK to Operate(JOBNAME is mandatory needed!)
    """

def main():
    try:
        # print "Width = ", GetSystemMetrics(0)
        # print "Height = ", GetSystemMetrics(1)
        flag_l = 0
        flag_d = 0
        flag_e = 0
        flag_j = 0
        flag_t = 0
        try:
            opts, args = getopt(
                sys.argv[1:],
                'hlde',
                [   'jobname=',
                    'list',
                    'delete',
                    'taskname=',
                    'help'
                ])
        except GetoptError as err:
            print str(err)
            usage()
            sys.exit(1)

        for opt, value in opts:

            if opt in ('--list', '-l'):
                flag_l = 1
            elif opt in ('--jobname'):
                job_name = value
                flag_j = 1
            elif opt in ('--taskname'):
                task_name = value
                flag_t = 1
            elif opt in ('--delete', '-d'):
                flag_d = 1
            elif opt in ('--edit', '-e'):
                flag_e = 1
            elif opt in ('--help', '-h'):
                usage()
                sys.exit(0)
            else:
                assert False, "Unhandled option"

        cur = MongoDBHelper()
        if flag_l == 1 and flag_d == 0 and flag_e == 0:
            # 可以简化
            #type_ = "list"
            #execute(flag_j, flag_t, type_)
            # 可以考虑使用* args了
            if flag_j == 1 and flag_t == 1:
                check_exist(job_name, task_name)
                cur.present_a_task(job_name, task_name)
            elif flag_j == 1 and flag_t == 0:
                check_exist(job_name)
                cur.present_a_job(job_name)
            elif flag_j == 0 and flag_t == 1:
                print "必须指定所属的job"
                sys.exit(1)
            else:
                cur.present_all_job()

        elif flag_l == 0 and flag_d == 1 and flag_e == 0:

            if flag_j == 1 and flag_t == 1:
                cur.delete_a_task(job_name, task_name)
            elif flag_j == 1 and flag_t == 0:
                cur.delete_a_job(job_name)
            elif flag_j == 0 and flag_t == 1:
                print "必须指定所属的job"
                sys.exit(1)
            else:
                print "要不要删除所有的job"
                cur.delete_all_job()

        elif flag_l == 0 and flag_d == 0 and flag_e == 1:

            if flag_j == 1 and flag_t == 1:
                edit_a_task(job_name, task_name)
            elif flag_j == 1 and flag_t == 0:
                edit_a_job(job_name)
            elif flag_j == 0 and flag_t == 1:
                print "必须指定所属的job"
                sys.exit(1)
            else:
                print "请务必指定一个操作单位"
                sys.exit(1)

        else:
            print "必须且只能指定一种操作方式，list, delete, edit, help"
            usage()
            sys.exit(1)

    except KeyboardInterrupt:
        sys.exit(0)

if __name__ == "__main__":
    main()
