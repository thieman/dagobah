#!/usr/bin/env python
# _*_ coding: utf-8

import sys
import requests
import os
import pymongo
import random
import string
from json import dump, load, loads


def check_job_name_overwrite(data_module):
    """如果name在job_name_list中就做覆盖判断"""

    # 获取服务端jobname列表
    job_name_list = get_server_job_name()

    if data_module['name'] in job_name_list:

        print "Warning: Your Job name is conflict with server's job_name_list, Sure to overwrite？(Y/n)"

        choice = raw_input("\"Y\" to keep go,\"n\" to stop and quit:")

        try:
            while choice.lower() != "y" and choice.lower() != "n":
                choice = raw_input("\"Y\" to keep go,\"n\" to stop and quit:")
        except KeyboardInterrupt, err:
            print str(err)
            sys.exit(0)

        if choice.lower() == "n":
            print "quiting..."
            sys.exit(0)

        elif choice.lower() == "y":
            pass


def check_dependencies_valid(data_module,input_file,module_task_list):
    """
    # 1、首先所有的dependencies name必须在module_task_list中
    # 2、值不能依赖键，就是说不能自我依赖
    # 3、键名列表必须是已经存在的task_name
    # 4、循环依赖还没有搞定
    # 5、添加检查schedule
    """
    #module_key_list = data_module.keys()
    #print module_key_list

    #if "dependencies" in module_key_list:
    module_depend_key_list = data_module[u'dependencies'].keys()

    check_repeat(input_file + u'中dependencies中的name', module_depend_key_list)

    if not check_issub(module_depend_key_list, module_task_list):
        print input_file, "模板中task_name列表：", module_task_list
        print input_file, "模板中dependencies键名列表：", module_depend_key_list
        print "键名必须是已存在的task_name"
        s1 = set(module_depend_key_list).difference(set(module_task_list))
        print "Error: 未知键：",
        for i in list(s1):
            print u"u'" + i + u"' ",
        sys.exit(1)

    for i in module_depend_key_list:
        check_repeat(u"dependencies字段有问题: \"" + i + u"\": " + str(data_module['dependencies'][i]), data_module['dependencies'][i])

        for j in data_module['dependencies'][i]:
            if j == i:
                print u"dependencies错误点: u'" + i + u"': ", data_module['dependencies'][i]
                print "Error: 任务不能依赖自己"
                sys.exit(1)

        if not check_issub(data_module['dependencies'][i], module_task_list):
            print "模板中task_name列表: ", module_task_list
            print u"dependencies错误点: u'" + i + u"': ", data_module['dependencies'][i]
            set_1 = set(data_module['dependencies'][i]).difference(set(module_task_list))
            print u"Error: 未知字段: ",
            for i in set_1:
                print u"u'" + i + u"' ",
            sys.exit(1)
    try:
        session_check = requests.session()
        try_to_login(session_check)
        job_tmpname = randomword(10)
        try_to_modulate_job_tasks(session_check, module_task_list, job_tmpname)
        # 用于检测schedule语法的正确性"""
        try_to_modulate_job_schedule(data_module, session_check, job_tmpname)

        for i in module_depend_key_list:
            import_dep = "http://localhost:9000/api/add_dependency"
            for j in data_module['dependencies'][i]:

                res = session_check.post(import_dep,
                                         {"job_name": job_tmpname,
                                          "from_task_name": i,
                                          "to_task_name": j})
                if res.status_code != 200:
                    print "Error: dependencies中存在循环依赖, 请检查依赖性"
                    sys.exit(1)
    finally:
        try_to_recycle_job(session_check, job_tmpname)


def randomword(length):

    return ''.join(random.choice(string.lowercase) for i in range(length))


def try_to_recycle_job(session_check, job_tmpname):
    """用于依赖性检测：回收测试job"""
    url = "http://localhost:9000/api/delete_job"
    session_check.post(url, {"job_name": job_tmpname})
    session_check.close()


def try_to_modulate_job_tasks(sess, module_task_list, job_tmpname):
    import_job_url = "http://localhost:9000/api/add_job"
    import_task_url = "http://localhost:9000/api/add_task_to_job"
    sess.post(import_job_url, {"job_name": job_tmpname})
    for i in module_task_list:
        sess.post(import_task_url,
                         {"job_name": job_tmpname,
                          "task_name": i,
                          "task_command": "echo 1"})


def try_to_modulate_job_schedule(data_module, session_check, job_tmpname):
    """用于检测schedule语法的正确性"""
    import_sche_url = "http://localhost:9000/api/schedule_job"
    schedule = data_module['cron_schedule']
    res = session_check.post(import_sche_url, {"job_name": job_tmpname, "cron_schedule": schedule})

    if res.status_code != 200:
        print u"cron_schedule: \"" + schedule + u"\""
        print "Error: cron_schedule语法错误！导入失败！！！"
        sys.exit(1)


def get_server_host_list():
    """获取服务端的主机列表"""

    os.system("cat /root/.ssh/config | grep \"^Host\" | awk '{print $2}' > /tmp/tmpfile")
    with open('/tmp/tmpfile', 'rb') as file_:
        list_ = [line[:-1] for line in file_]
    return list_


def check_host_exist_in_server(hosts, host_list, distri_file):
    """ -h指定的文件在服务端是否存在 """

    for i in host_list:
        if i not in hosts:
            print "服务端主机列表: ", hosts
            print distri_file + u"主机列表: ", host_list
            print u"Error: 未知主机 " + i, "该主机不存在"
            sys.exit(1)


def check_job_required_key(data_module, flag_dep):
    """ 检查job必备的四个键和一个可选键 dependencies"""

    module_key_list = data_module.keys()
    list_ = [u"name",u"tasks",u"cron_schedule",u"dependencies",u"notes"]

    if not check_issub(module_key_list,list_):
        print "模板必备字段: ", list_, " ,其中'dependencies'字段可选"
        if flag_dep == 0:
            module_key_list.remove(u'dependencies')
        print "你的字段列表： ", module_key_list
        s1 = set(module_key_list).difference(set(list_))
        print "Error: 未知字段： ",
        for i in list(s1):
            print u"u'" + i + u"' ",
        sys.exit()

    elif len(module_key_list) < 5:
        print "模板必备字段: ", list_, " ,其中'dependencies'字段可选"
        if flag_dep == 0:
            module_key_list.remove(u'dependencies')
            list_.remove(u'dependencies')
        print "你的字段列表: ", module_key_list
        s1 = set(list_).difference(set(module_key_list))
        print "Error: 缺少字段: ",
        for i in list(s1):
            print u"u'" + i + u"' ",
        sys.exit()


def get_server_job_name():
    """ 获取dagobah服务端的job_name列表"""

    client = pymongo.MongoClient('mongodb://localhost', 27017)
    db = client['dagobah']
    collect = db['dagobah']
    l1 = []
    for item in collect.find():
        for task in item['jobs']:
            l1.append(task['name'])
    return l1


def check_job_item_if_null(data,args):
    """检查data的args字段是否为空，args必须是列表"""

    for arg in args:
        if data[arg] == "" or data[arg] == None or data[arg] == []:
            print u"u'"+arg+"': ",data[arg]
            print u"Error: u\"" + arg + u"\" 字段值不能空"
            sys.exit(1)


def check_tasks_item_if_null(data,args):
    """检查data的args字段是否为空，args必须是列表"""
    for arg in args:
        if data[arg] == "" or data[arg] == None:
            print data
            print u"Error: u\"" + arg + u"\" 字段值不能空"
            sys.exit(1)


def check_if_str(data,args):
    """检查data的args字段是否为字符串，args必须是列表"""

    for arg in args:
        if type(data[arg]) != unicode:
            print u"u'"+arg+"': ",data[arg]
            print u"Error: u\"" + arg + u"\" 字段类型必须是字符串"
            sys.exit(1)

def check_tasks_item_if_str(data,args):
    """检查data的args字段是否为字符串，args必须是列表"""

    for arg in args:
        if type(data[arg]) != unicode:
            print data
            print u"Error: u\"" + arg + u"\" 字段类型必须是字符串"
            sys.exit(1)

def check_if_list_dict(data,args):
    """检查tasks类型是否为列表,并且列表内的每个元素必须是字典"""
    for arg in args:
        if type(data[arg]) != list:
            print u"u'"+arg+"': ",data[arg]
            print "Error: ", arg, "字段类型必须是列表"
            sys.exit(1)
    for i in data['tasks']:
        if type(i) != dict:
            print u"u'"+arg+"': ",data[arg]
            print "Error: ", arg, "的列表内的item必须是字典"
            sys.exit(1)



def check_if_int(data,args):
    """检查data的args字段是否为字符串，args必须是列表"""

    for arg in args:
        if type(data[arg]) != int:
            print data
            print u"Error: u\"" + arg + u"\" 字段类型必须是整数"
            sys.exit(1)

def check_soft_hard_hostname(module_tasks_iter, data_module, task_list, hosts):
    """
    检查soft，hard，hostname的合法性，
    1、hard_soft_timeout不能为空，必须是整数
    2、如果name不在分布式列表中，那他必须有hostname字段，并且必须不为None ,不为空
    3、hostname也得在服务端列表中

    """
    for id_ in module_tasks_iter:
        command = data_module['tasks'][id_]['command']
        name = data_module['tasks'][id_]['name']

        if u"soft_timeout" in data_module['tasks'][id_].keys():
            check_tasks_item_if_null(data_module['tasks'][id_], ['soft_timeout'])
            check_if_int(data_module['tasks'][id_], ['soft_timeout'])

        if u"hard_timeout" in data_module['tasks'][id_].keys():
            check_tasks_item_if_null(data_module['tasks'][id_], ['hard_timeout'])
            check_if_int(data_module['tasks'][id_], ['hard_timeout'])

        if name not in task_list:
            if u"hostname" not in data_module['tasks'][id_].keys():
                print data_module['tasks'][id_]
                print u"Error: 此任务不做分布, 必须具备hostname字段，请重新编辑"
                sys.exit(1)
            else:
                hostname = data_module['tasks'][id_]['hostname']
                if hostname == "" or hostname == None:
                    print data_module['tasks'][id_]
                    print u"Error: 此任务不做分布式, hostname值不能为空"
                    sys.exit(1)
                elif hostname not in hosts:
                    print "服务端主机列表: ", hosts
                    print u"模板任务:", data_module['tasks'][id_], "中的hostname字段"
                    print u"Error: 未知主机: \"" + str(hostname) + u"\""
                    sys.exit(1)
                else:
                    pass

def check_if_t_name_in_tasks(task_list,module_task_list):
    """将要修改的task和模板中的task_name做比较，
    如果要修改的task不是模板中的子集就报错"""
    if not check_issub(task_list, module_task_list):
        print u"模板中task_name列表: " + str(module_task_list)
        print u"-t指定的name列表: " + str(task_list)
        s1 = set(task_list).difference(set(module_task_list))
        print u"Error: 指定列表中有未知name: ",
        for i in list(s1):
            print "\""+i+"\".",
        sys.exit(1)


def check_tasks_required_key(data_module, module_tasks_iter):
    """进入tasks，开始检查tasks的name和command的存在性"""

    for i in module_tasks_iter:
        if 'name' not in data_module['tasks'][i].keys():
            print data_module['tasks'][i]
            print "Error: 该任务缺少name字段!"
            sys.exit(1)
        if 'command' not in data_module['tasks'][i].keys():
            print data_module['tasks'][i]
            print "Error: 该任务缺少command字段!"
            sys.exit(1)
        check_tasks_item_if_null(data_module['tasks'][i],['name','command'])
        check_tasks_item_if_str(data_module['tasks'][i],['name','command'])


def integration(module_tasks_iter,data_module):
    """将tasks中task的id和其name对应的值整合成一个dict"""
    dict_ = {}
    for i in module_tasks_iter:
        dict_[i] = data_module['tasks'][i]['name']

    return dict_


def check_repeat(describe, list_):
    """check if the list_ has repeated item"""

    if len(list_) != len(set(list_)):
        print u"Error: " + describe + u" 有重复字段"
        print str(list_)
        sys.exit(1)

def check_tasks_name_repeat(data_module,module_tasks_iter):
    dict_id_name = integration(module_tasks_iter,data_module)
    values = dict_id_name.values()
    for id_1 in dict_id_name:
        yuanben = dict_id_name[id_1]
        values.remove(yuanben)
        if yuanben in values:
            print data_module['tasks'][id_1]
            del dict_id_name[id_1]
            for id_2 in dict_id_name:
                if dict_id_name[id_2] == yuanben:
                    print data_module['tasks'][id_2]
                    print "Error: 这两个任务的name重复，请更正!"
                    sys.exit(1)
        else:
            pass


def check_issub(list1, list2):
    """return true if list1 is the subset of list2"""

    set1 = set(list1)
    set2 = set(list2)

    return set1.issubset(set2)


def decode_import_json(json_doc, transformers=None):
    """ Decode a JSON string based on a list of transformers.
    Each transformer is a pair of ([conditional], transformer). If
    all conditionals are met on each non-list, non-dict object,
        the transformer tries to apply itself.
        conditional: Callable that returns a Bool.
        transformer: Callable transformer on non-dict, non-list objects.
        """

    def custom_decoder(dct):

        def transform(o):

            if not transformers:
                return o

            for conditionals, transformer in transformers:

                conditions_met = True
                for conditional in conditionals:
                    try:
                        condition_met = conditional(o)
                    except:
                        condition_met = False
                    if not condition_met:
                        conditions_met = False
                        break

                if not conditions_met:
                    continue

                try:
                    return transformer(o)
                except:
                    pass

            return o

        for key in dct.iterkeys():
            if isinstance(key, dict):
                custom_decoder(dct[key])
            elif isinstance(key, list):
                [custom_decoder[elem] for elem in dct[key]]
            else:
                dct[key] = transform(dct[key])

        return dct

    return loads(json_doc, object_hook=custom_decoder)


def get_unchanged_task_id(module_tasks_iter, data_module, task_list):
    """获取没有在改变列表的task在tasks中的index列表"""
    list_ = []

    for i in module_tasks_iter:
        if data_module[u'tasks'][i][u'name'] not in task_list:
            list_.append(data_module[u'tasks'].index(data_module[u'tasks'][i]))

    return list_


def get_id_by_name(data_module, module_tasks_iter, name):
    """get task id by it's name"""
    for i in module_tasks_iter:

        if data_module['tasks'][i]['name'] == name:

            return i


def init_data_real(data_module):
    """
    初始化一个只包含tasks, notes, name, cron_schedule, dependencies
    的一个字典，但是task和dependencies是空的，其他直接继承data_module
    """
    data_real = add_empty_job()

    data_real["notes"] = data_module['notes']

    data_real["name"] = data_module['name']

    data_real["cron_schedule"] = data_module['cron_schedule']

    return data_real


def update_data_real(data_module, data_real,
                     task_id, hostname,
                     task_list, module_tasks_iter):
    """"""
    dict_ = add_empty_task()

    if 'soft_timeout' in data_module['tasks'][task_id]:
        dict_['soft_timeout'] = data_module['tasks'][task_id]['soft_timeout']

    if 'hard_timeout' in data_module['tasks'][task_id]:
        dict_['hard_timeout'] = data_module['tasks'][task_id]['hard_timeout']

    dict_['command'] = data_module['tasks'][task_id]['command']

    old_name = data_module['tasks'][task_id]['name']

    dict_['name'] = old_name + u'_at_' + hostname

    dict_['hostname'] = hostname

    data_real['tasks'].append(dict_)

    # 不论线花到哪里，进入此函数的都是要被分布的task，也就是说，哪怕你把所有的任务都圈起来，这里面的东西也只有两种情况：在列表和不在列表！
    # 初始化的对应dependencies的字段都是一个空list，然后从data_module中的字段来判断选择，如果是属于-t指定的task之内的，就将其更名，并添加至新列表
    # 如果是之外的，就将原来的添加进去
    list_ = []
    if data_module['dependencies'][old_name] == []:
        data_real['dependencies'][dict_['name']] = list_
    else:

        for i in data_module['dependencies'][old_name]:

            if i in task_list:
                new = i + u'_at_' + hostname
                list_.append(new)
                data_real['dependencies'][dict_['name']] = [].append(new)
            else:
                i_id = get_id_by_name(data_module, module_tasks_iter, i)
                new = i + u'_at_' + data_module['tasks'][i_id]['hostname']
                list_.append(new)
                data_real['dependencies'][dict_['name']] = [].append(new)

        data_real['dependencies'][dict_['name']] = list_


def update_data_real2(data_module, data_real,
                      task_id, task_list,
                      host_iter, host_list,
                      module_tasks_iter):
    """update unchanegd tasks in module_task_list"""

    dict_ = add_empty_task()

    if 'soft_timeout' in data_module['tasks'][task_id]:
        dict_['soft_timeout'] = data_module['tasks'][task_id]['soft_timeout']

    if 'hard_timeout' in data_module['tasks'][task_id]:
        dict_['hard_timeout'] = data_module['tasks'][task_id]['hard_timeout']

    dict_['command'] = data_module['tasks'][task_id]['command']
    old_name = data_module['tasks'][task_id]['name']
    dict_['hostname'] = data_module['tasks'][task_id]['hostname']
    dict_['name'] = old_name + u'_at_' + dict_['hostname']
    data_real['tasks'].append(dict_)
    list_1 = []
    update_data_real3(data_module, data_real,
                      task_id, host_iter,
                      host_list, task_list,
                      list_1, dict_['name'],
                      module_tasks_iter)


# 不论线花到哪里，进入此函数的都是要被分布的task，也就是说，哪怕你把所有的任务都圈起来，这里面的东西也只有两种情况：在列表和不在列表！
# 初始化的对应dependencies的字段都是一个空list，然后从data_module中的字段来判断选择，如果是属于-t指定的task之内的，就将其更名，并添加至新列表
# 如果是之外的，就将原来的添加进去
def update_data_real3(data_module, data_real,
                      task_id, host_iter,
                      host_list, task_list,
                      list_1, name1,
                      module_tasks_iter):
    """update_unchanged_task_dependencies"""

    name = data_module['tasks'][task_id]['name']
    data_real['dependencies'][name1] = []
    for i in data_module['dependencies'][name]:
        if i in task_list:
            for host in host_iter:
                new = i + u'_at_' + host_list[host]
                list_1.append(new)
        else:
            i_id = get_id_by_name(data_module, module_tasks_iter, i)
            new = i + u'_at_' + data_module['tasks'][i_id]['hostname']
            list_1.append(new)
    data_real['dependencies'][name1] = list_1


def add_empty_task():
    '''add empty task to tasks, equal to init task unit'''

    dict_ = {}
    dict_[u"soft_timeout"] = 0
    dict_[u"hard_timeout"] = 0
    dict_[u"hostname"] = None
    dict_[u"name"] = None
    dict_[u"command"] = None

    return dict_


def add_empty_job():
    '''add empty jobs, equal to init job unit'''

    dict_ = {}
    dict_[u'notes'] = None
    dict_[u'name'] = None
    dict_[u'cron_schedule'] = None
    dict_[u'tasks'] = []
    dict_[u'dependencies'] = {}

    return dict_


def jsonalize(data_real):
    '''创建连接dagobah的回话，然后开始导入数据'''
    filename = 'jiushizheige.json'

    try:
        tmpfile = open(filename, 'wb')
        dump(data_real, tmpfile)
        tmpfile.flush()
    except IOError:
        print "打开文件失败: " + filename
        sys.exit(1)
    finally:
        tmpfile.close()

    try:
        file_ = open(filename, 'rb')

        session = requests.session()

        try_to_login(session)

        import_url = "http://localhost:9000/jobs/import"

        json_file = [('file', (filename, file_, 'application/json'))]

        session.post(import_url, files=json_file)

        print "导入成功"

    finally:
        session.close()
        file_.close()
        os.remove(filename)


def try_to_login(session):
        try:
            session.post('http://localhost:9000/do-login', {"password": "dagobah"})
        except requests.exceptions.ConnectionError, err:
            print str(err)
            print "连接失败，发送终止"
            sys.exit(1)


def usage():
    print """python dagopost.py <options> <args>
        -i | --input-file = <jsonfile>
        -H | --host-file = <hostfile>
        -t | --task-to-distribute = <tasks>
        -h | --help to get help """


def trans_file_to_list(filename):
    """open host-file and transfer file per line to a list"""
    try:
        file_ = open(filename, 'rb')
        list_ = [line[:-1] for line in file_]
        list_ = [str(i).strip() for i in list_]
    except IOError, err:
        print "打开文件失败" + str(err)
    finally:
        file_.close()

    return list_
