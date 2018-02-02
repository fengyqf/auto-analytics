#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import time
import urllib
import urllib2
import pickle
import json
import base64
import csv


import appconfig as cfg

script_dir=os.path.split(os.path.realpath(__file__))[0]+'/'

config_file=script_dir+'/config.ini'




try:
    cache=pickle.load(open(cfg['main']['cache_file_name'],'r'))
except:
    cache={}


class Um_auth:
    def __init__(self, cache_file,email,password):
        self.cache_file=cache_file
        self.email=email
        self.password=password
        try:
            cache=pickle.load(open(cache_file,'r'))
            self.Authorization=cache['umeng__Authorization']
            self.check()
        except:
            self.re_auth()

    def check(self):
        url="http://api.umeng.com/apps/count"
        req=urllib2.Request(url)
        req.add_header('Authorization','Basic %s' %(cache['umeng__Authorization']))
        body_raw=urllib2.urlopen(req).read()
        response=json.loads(body_raw)
        print response


    def re_auth(self):
        url="http://api.umeng.com/authorize"
        req=urllib2.Request(url,
                urllib.urlencode({'email':self.email, 'password':self.password, })
            )

        body_raw=urllib2.urlopen(req).read()
        response=json.loads(body_raw)
        cache['umeng__auth_token']=response['auth_token']
        cache['umeng__Authorization']=base64.b64encode(cache['umeng__auth_token'])
        pickle.dump(cache,open(self.cache_file,'w+'))
        print response
        self.Authorization=cache['umeng__Authorization']
        print 'auth token cached'



def retrive_umeng(appkey,api,date_start,date_end,args={}):
    try:
        period_type=args['period_type']
    except:
        period_type='daily'

    url="http://api.umeng.com/%s?appkey=%s&period_type=%s&start_date=%s&end_date=%s"%(
            api,appkey,period_type,date_start,date_end)
    req=urllib2.Request(url)
    req.add_header('Authorization','Basic %s' %(cache['umeng__Authorization']))
    body_raw=urllib2.urlopen(req).read()
    response=json.loads(body_raw)
    #print "retrive data: \n",response

    #try:
    if True:
        rows=[]
        items_label=response['dates']
        for key in response['data']:
            items_values=response['data'][key]
        print items_label,items_values
        timestamp=int(time.time())
        for i in range(len(items_label)):
            rows.append((items_label[i],items_values[i]))
        return rows
    #except:
    #    print "retrive data Error from response.json\n",response




#-------------------------------------------------------------------------------


um_auth=Um_auth(cfg.main['cache_file_name'],cfg.umeng['email'],cfg.umeng['password'])
umeng__Authorization=um_auth.Authorization

print "umeng__Authorization:",umeng__Authorization

um_keys=[it['appkey'] for it in cfg.um_source]
for it in cfg.um_source:
    appname=it['name']
    applabel=it['label']
    appkey=it['appkey']
    date_start=it['start']
    today=datetime.date.today()
    date_end=today.strftime('%Y-%m-%d')
    print '\n[%s] %s'%(appkey,appname)
    #UV
    #检查存储文件中是否存在
    filepath=script_dir+'data/'+applabel+'_uv.csv'
    print filepath
    header=['date','num']
    lines_dict={}
    lines_keys=[]
    if not os.path.isfile(filepath):
        pass
    else:
        with open(filepath,'rb') as fp:
            f_csv=csv.reader(fp)
            next(f_csv)
            for line in f_csv:
                if line:
                    lines_dict[line[0]]=line[1]
                    lines_keys.append(line[0])
                    date_start=line[0]
    print 'date_start: ',date_start
    if True:
        # TODO 对于过长的时间段，要分多批进行
        rows=retrive_umeng(appkey,'active_users',date_start,date_end,args={})
        rows=[(it[0].encode('utf-8'),'%s'%it[1]) for it in rows]

        #合并 rows 到 lines
        for row in rows:
            lines_dict[row[0]]=row[1]
        line_keys=list(lines_dict)
        line_keys.sort()
        lines=[(it,lines_dict[it]) for it in line_keys]

        with open(filepath,'w+') as fp:
            f_csv=csv.writer(fp)
            f_csv.writerow(header)
            f_csv.writerows(lines)

exit()







today=datetime.date.today()
date_end=today.strftime('%Y-%m-%d')
date_start=(today-datetime.timedelta(days=5)).strftime('%Y-%m-%d')

print date_start,date_end

appkey='57317f1fe0f55a765300216c'
api='active_users'

def fetch_umeng(conn,appkey,api,date_start,date_end,args={}):
    try:
        period_type=args['period_type']
    except:
        period_type='daily'

    args_s=''
    args_keys=args.keys()
    if args_keys:
        for k in args_keys.sort():
            args_s=args_s+'~'+k+'~'+args[k]
    param_string='%s~%s~~%s'%(appkey,api,args_s)
    hashing=hash(param_string)
    print hashing
    print param_string

    days_count=(datetime.datetime.strptime(date_end, '%Y-%m-%d')-datetime.datetime.strptime(date_start, '%Y-%m-%d')).days
    print "days_count: ",days_count

    #检查line中条数是否正确，忽略当日

    url="http://api.umeng.com/%s?appkey=%s&period_type=%s&start_date=%s&end_date=%s"%(
            api,appkey,period_type,date_start,date_end)
    req=urllib2.Request(url)
    req.add_header('Authorization','Basic %s' %(cache['umeng__Authorization']))
    body_raw=urllib2.urlopen(req).read()
    response=json.loads(body_raw)

    #try:
    if True:
        items_label=response['dates']
        for key in response['data']:
            items_values=response['data'][key]
        print items_label,items_values
        values=[]
        timestamp=int(time.time())
        for i in range(len(items_label)):
            values.append((hashing,items_values[i],timestamp,date2int(items_label[i])))
        val_batch=tuple(values)
        print val_batch
        sql="""insert into `data` (`hashing`,`val`,`batch`,`day`)\
            values(%s,%s,%s,%s)"""
        cursor.executemany(sql,val_batch)
    #except:
    #    print "retrive data Error from response.json\n",response


def date2int(txt):
    d=datetime.datetime.strptime(txt, '%Y-%m-%d')
    return d.year*10000+d.month*100+d.day

def int2date(di):
    return '%d-%02d-%02d'%(di/10000,di%1000/100,di%100)


#fetch_umeng(conn,appkey,api,date_start,date_end)

retrive_umeng(conn,appkey,api,date_start,date_end)
