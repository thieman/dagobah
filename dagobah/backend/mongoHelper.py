#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import sys
import pymongo
import pprint
import requests
from termcolor import colored


class MongoDBHelper(object):
    def __init__(self):
        self.collect = self.connect_dagobah_mongo()
        self.iter_ = self.jobs_iter()
        self.job_list = self.get_all_job_list()
        self.session = self.login_to_dago()
        self.pp = pprint.PrettyPrinter(indent=4)

    def login_to_dago(self):
        try:
            ss = requests.session()
            ss.post("http://localhost:9000/do-login", {"password": 'dagobah'})
        except requests.exceptions.ConnectionError:
            print "登录失败，请检查服务启动情况"
            sys.exit(1)
        return ss

    def connect_dagobah_mongo(self):
        client = pymongo.MongoClient('mongodb://localhost', 27017)
        db = client['dagobah']
        collect = db['dagobah']
        return collect

    def get_all_job_list(self):
        l1 = []
        for i in self.collect.find():
            for j in i['jobs']:
                l1.append(j['name'])
        return l1

    def get_job_task_list(self, job_name):
        l1 = []
        for i in self.collect.find():
            for j in i['jobs']:
                if j['name'] == job_name:
                    for k in j['tasks']:
                        l1.append(k['name'])
        return l1

    def jobs_iter(self):
        for item in self.collect.find():
            return item['jobs']

    def present_all_job(self):
        print '{:2s} {:-4d} {:2s}'.format(colored('Total: ', 'blue'),
                                          len(self.iter_),
                                          colored('jobs included', 'blue'))
        print "{:25s} {:20s} {:s}".format('<name>',
                                          '<num of tasks>',
                                          '<notes>')
        for jobs in self.iter_:
            print "{:25s} {:29s} {:10s}".format(
                jobs['name'],
                colored(len(jobs['tasks']), 'red'),
                jobs['notes'])

    def present_a_job(self, job_name):
        if job_name not in self.job_list:
            self.present_all_job()
            print colored(u"Error: "+u"job \""+job_name+u"\"" + u" is not exist!", 'red')
            sys.exit(1)
        for jobs in self.iter_:
            if jobs['name'] == job_name:
                for key in ['job_id',
                            'parent_id',
                            'status',
                            'next_run']:
                    del jobs[key]
                for task in jobs['tasks']:
                    for key in ['started_at',
                                'completed_at',
                                'soft_timeout',
                                'hard_timeout',
                                'hostname',
                                'success']:
                        del task[key]
                self.pp.pprint(jobs)

    def present_a_task(self, job_name, task_name):
        if job_name not in self.job_list:
            self.present_all_job()
            print colored(u"Error: "+u"job \""+job_name+u"\"" + u" is not exist!", 'red')
            sys.exit(1)

        task_list = self.get_job_task_list(job_name)
        if task_name not in task_list:
            print colored(u"Error: job \"" + job_name+u"\"" + u" has no tasks \"" + task_name+u"\"", 'red')
            sys.exit(1)

        for jobs in self.iter_:
            if jobs['name'] == job_name:
                print colored(u"This task belongs to job: \"" + job_name + "\"", 'red')
                for j in jobs['tasks']:
                    if j['name'] == task_name:
                        self.pp.pprint(j)

    def delete_a_job(self, job_name):
        if job_name not in self.job_list:
            self.present_all_job()
            print colored(u"Error: job \""+job_name+u"\"" + u" is not exist!", 'red')
            sys.exit(1)
        self.present_a_job(job_name)
        choice = raw_input(colored("You sure to delete the %s (Y/n)", 'red') % job_name)
        while choice.lower() != "y":
            if choice.lower() == "n":
                sys.exit(0)
            else:
                choice = raw_input("You sure to delete the %s (Y/n)" % job_name)
        aa = self.session.post('http://localhost:9000/api/delete_job', {'job_name': job_name})
        if aa.status_code == 200:
            print "删除成功"
            sys.exit(0)
        else:
            print "删除失败，请重新尝试"
            sys.exit(1)
        self.session.close()

    def delete_a_task(self, job_name, task_name):
        if job_name not in self.job_list:
            self.present_all_job()
            print colored(u"Error: job \""+job_name+u"\"" + " is not exist!", 'red')
            sys.exit(1)
        self.present_a_task(job_name, task_name)
        print colored('This might affect on dependencies!', 'red')
        choice = raw_input(colored("You sure to delete this task: %s ?(Y/n)", 'red') % task_name)
        while choice.lower() != 'y':
            if choice.lower() == 'n':
                sys.exit(0)
            else:
                choice = raw_input("You sure to delete this task: %s ?(Y/n)" % task_name)
        if self.session.post('http://localhost:9000/api/delete_task', {'job_name': job_name, "task_name": task_name}).status_code == 200:
            print "删除成功"
            sys.exit(0)
        else:
            print "删除失败"
            sys.exit(1)

        self.session.close()
