#!/usr/bin/env python
# _*_ coding:utf-8 _*_

u"""this script is used to create distribute tasks on remote host"""

import sys
import requests
import os
import re
from json import dump, load, loads
from getopt import getopt, GetoptError
from backend.functions import *
from pprint import pprint


def check_validation(
        #  data_module, task_string, distri_file,
        data_module, task_string, hosts,
        host_list, input_file, module_tasks_iter,
        task_list, flag_dep, flag_tz):
    """
    首先有了inputfile
    1、-t指定的要分布的task必须是inputfile中已经有了的taskname
    2、-i指定inputfile之后，对其合理性进行检查
    3、-d指定了要分布的计算机的主机名，然后判断，它的主机名是否出现在
    4、host.txt中不能出现重复的主机名
    4、host.txt中的主机名必须在dagobah上已经存在
    6、判断有没有name, command, hostname, 使用dict.has_key()来判断
    """

    # 获取服务端host列表
    # hosts = get_server_host_list()

    # -t和hostlist的重复性测试
    check_repeat(u"-t指定的name列表: " + task_string, task_list)

    #---------检查job必备键,及其job键列表的重复性-----------------


    # 检查job必备的四个键和两个可选键 "dependencies", "timezone"
    check_job_required_key(data_module, flag_dep, flag_tz)
    check_job_name_not_start_with_digit(data_module, input_file)

    # 四键('name', 'cron_schedule', 'tasks', 'notes') 不为空检查， 为str检查,也不能为字典
    # 但是这里只检查tasks不能为[]和None
    args = ['name', 'notes', 'cron_schedule', 'tasks']
    check_job_item_if_null(data_module, args)
    check_if_str(data_module, ['name', 'notes', 'cron_schedule'])

    # 如果说要是有timezone的话，就检查其合理性，没有的话就不检查了
    check_if_in_all_tzs(data_module)

    # 检查tasks类型是否为列表,并且列表内的每个元素必须是字典"""
    check_if_list_dict(data_module,['tasks'])

    # 进入tasks，开始检查tasks的name和command的存在性
    # tasks中的name，command不为空，为str
    check_tasks_required_key(data_module, module_tasks_iter)

    # 检查tasks的name们的重复性
    check_tasks_name_repeat(data_module,module_tasks_iter)

    module_task_list = [data_module[u'tasks'][i][u'name'] for i in module_tasks_iter]

    # 将要修改的task和模板中的task_name做比较，如果要修改的task不是模板中的子集就报错
    check_if_t_name_in_tasks(task_list,module_task_list)

    # 1、hard_soft_timeout不能为空，必须是整数
    # 2、如果name不在分布式列表中，那他必须有hostname字段，并且必须不为None ,不为空
    # 3、hostname也得在服务端列表中
    check_soft_hard_hostname(module_tasks_iter, data_module, task_list, hosts)



    # 检查dependencies字段合法性
    # 1、首先所有的dependencies name必须在module_task_list中
    # 2、值不能依赖键，就是说不能自我依赖
    # 3、键名列表必须是已经存在的task_name
    # 4、循环依赖还没有搞定
    check_dependencies_valid(data_module, input_file, module_task_list)

    # 如果name在job_name_list中就做覆盖判断
    check_job_name_overwrite(data_module)

def main():
    flag_i = 0
    flag_h = 0
    flag_t = 0
    flag_h_f = 0
    flag_h_r = 0
    flag_f1_r0 = 0
    flag_f0_r1 = 0

    try:
        opts, args = getopt(
            sys.argv[1:], 'i:t:Hh',
            [
                'input-file=',
                'task-to-distribute=',
                'fname=',
                'repr=',
                'help'
                ])
    except GetoptError as err:
        print str(err)
        usage()
        sys.exit(1)


    task_string = u""
    for opt, value in opts:

        if opt in ("--input-file", "-i"):
            input_file = value
            flag_i = 1
        elif opt in ("-H"):
            flag_h = 1
        elif opt in ("--fname"):
            distri_file = value
            flag_h_f = 1
        elif opt in ("--repr"):
            distri_repr = value
            flag_h_r = 1
        elif opt in ("--task-to-distribute", "-t"):
            task_string = value
            flag_t = 1
        elif opt in ("--help", "-h"):
            usage()
            sys.exit(0)
        else:
            assert False, "unhandled option"

    if flag_i != 1:
        print "Error: 命令格式错误，必须使用\"-i\"指定模板文件."
        print "Use \"nagobah -h\" for more help!"
        sys.exit(1)

    if flag_h != 1:
        print "Error: 命令格式错误，必须使用\"-H\"指定分布主机(提供两种方式--fname和--repr)."
        print "Use \"nagobah -h\" for more help!"
        sys.exit(1)
    else:
        if flag_h_f == 1 and flag_h_r == 0:
            flag_f1_r0 = 1
        elif flag_h_f == 0 and flag_h_r == 1:
            flag_f0_r1 = 1
        else:
            print "--repr和--fname只能且必须使用一个"
            print "Use \"nagobah -h\" for more help!"
            sys.exit(1)

    try:
        inputfile = open(input_file, 'rb')
        try:
            #  data_module2 = load(inputfile)
            #  print data_module2
            data_module = decode_import_json(inputfile.read())
        except ValueError, err:
            print "\"" + input_file + "\"格式问题:"
            print err.__class__.__name__, err
            sys.exit(1)
        finally:
            inputfile.close()
    except IOError, err:
        print "Error: 文件打开错误"
        print str(err)
        sys.exit(1)

    # 要修改(分布)的task的name列表, 这些列表从外部来，服从的是coding，要转换为unicode和原来数据比较
    task_list = filter(None, task_string.split(','))
    task_list = [unicode(x, 'utf-8') for x in task_list]

    #  获取要分布的主机名的列表
    #  host_list = trans_file_to_list(distri_file)

    # 获取服务端host列表
    hosts = get_server_host_list()

    if flag_f1_r0 == 1:
        host_list = trans_file_to_list(distri_file)
        # -h指定的文件的重复检测
        check_repeat(distri_file, host_list)
        # -h指定的文件在服务端是否存在
        check_host_exist_in_server(hosts, host_list, distri_file)
    else:
        host_list = filter_hosts_by_repr(hosts, distri_repr)

    #host_list = [unicode(x, 'utf-8') for x in host_list]
    host_iter = range(len(host_list))

    # 所有模板中的任务数
    module_tasks_iter = range(len(data_module[u'tasks']))


    # 用户的dep完全可以不写代表单个任务
    flag_dep = 1
    if u'dependencies' not in data_module:
        data_module[u'dependencies'] = {}
        flag_dep = 0

    # 用户也可以不用书写时区，这样时区就默认为服务器当前时区
    flag_tz = 1
    if u'timezone' not in data_module:
        with open('/etc/timezone', 'rb') as f:
            timezone = f.read()[:-1]
        data_module[u'timezone'] = timezone
        flag_tz = 0

    # 如果输入的-H是文件的话就先去检查文件是否符合标准
    # 否则就应该是repr，直接执行下面步骤就行了,现在讲所有的distri_file剥离出来

    # check validation
    check_validation(
        #  data_module, task_string, distri_file,
        data_module, task_string, hosts,
        host_list, input_file, module_tasks_iter,
        task_list, flag_dep, flag_tz)

    # 模板中tasks中的name列表
    module_task_list = [data_module[u'tasks'][i][u'name'] for i in module_tasks_iter]


    for i in module_task_list:
        if i not in data_module[u'dependencies'].keys():
            data_module[u'dependencies'][i] = []


    # unchanged_task_name = [l for l in module_task_list if l not in task_list]

    # 初始化一个数据结构data_real
    # jobname, schedule, notes取自data_module
    # tasks和dependencies都设置为空
    data_real = init_data_real(data_module)

    for task_id in module_tasks_iter:
        if data_module['tasks'][task_id]['name'] in task_list:
            for host in host_iter:
                update_data_real(data_module, data_real,
                                 task_id, host_list[host],
                                 task_list, module_tasks_iter)
        else:
            update_data_real2(data_module, data_real,
                              task_id, task_list,
                              host_iter, host_list,
                              module_tasks_iter)

        print u"Tasks: " + data_module['tasks'][task_id]['name'] + u" reconstruction complete!"

    # init session and post data to server
    post_to_server(data_real)

if __name__ == "__main__":
    main()
