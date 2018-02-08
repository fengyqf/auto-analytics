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
import math


import appconfig as cfg

script_dir=os.path.split(os.path.realpath(__file__))[0]+'/'

config_file=script_dir+'/config.ini'





class Um_auth:
    cache={}
    def __init__(self, cache_file,email,password):
        self.cache_file=cache_file
        self.email=email
        self.password=password
        try:
            Um_auth.cache=pickle.load(open(cache_file,'r'))
            self.Authorization=Um_auth.cache['umeng__Authorization']
            self.check()
        except:
            self.re_auth()

    def check(self):
        url="http://api.umeng.com/apps/count"
        req=urllib2.Request(url)
        req.add_header('Authorization','Basic %s' %(Um_auth.cache['umeng__Authorization']))
        try:
            f=urllib2.urlopen(req)
            body_raw=f.read()
            response=json.loads(body_raw)
            print "cached token Effective/Available.  ",response['count'] ,' Apps found'
        except urllib2.HTTPError,e:
            print e
            self.re_auth()
        except urllib2.URLError,e:
            print e
            self.re_auth()
        except Exception as e:
            print e


    def re_auth(self):
        url="http://api.umeng.com/authorize"
        req=urllib2.Request(url,
                urllib.urlencode({'email':self.email, 'password':self.password, })
            )

        body_raw=urllib2.urlopen(req).read()
        response=json.loads(body_raw)
        Um_auth.cache['umeng__auth_token']=response['auth_token']
        Um_auth.cache['umeng__Authorization']=base64.b64encode(Um_auth.cache['umeng__auth_token'])
        pickle.dump(Um_auth.cache,open(self.cache_file,'w+'))
        print response
        self.Authorization=Um_auth.cache['umeng__Authorization']
        print 'auth token cached. (finished re-auth)'



def retrive_umeng(appkey,api,date_start,date_end,args={}):
    try:
        period_type=args['period_type']
    except:
        period_type='daily'

    url="http://api.umeng.com/%s?appkey=%s&period_type=%s&start_date=%s&end_date=%s"%(
            api,appkey,period_type,date_start,date_end)
    req=urllib2.Request(url)
    req.add_header('Authorization','Basic %s' %(Um_auth.cache['umeng__Authorization']))
    f=urllib2.urlopen(req)
    body_raw=f.read()
    response=json.loads(body_raw)

    if True:
        rows=[]
        items_label=response['dates']
        for key in response['data']:
            items_values=response['data'][key]
        #print items_label,items_values
        timestamp=int(time.time())
        for i in range(len(items_label)):
            rows.append((items_label[i],items_values[i]))
        return rows


# 根据字符串形的起止日期、步长天数，拆分出一系列以列表存储的起止日期元组
# 若起止日期间隔小于1天，则返回最近两天的间隔元组，目的是至少更新最近两天的数据
def batch_date_range(start,end,step=30):
    s=datetime.datetime.strptime(start,'%Y-%m-%d')
    e=datetime.datetime.strptime(end,'%Y-%m-%d')
    if (e-s).days <= 1:
        s=e-datetime.timedelta(days=1)
    cnt=int(math.ceil(float((e-s).days)/step))
    #template [ (s+it*t,s+(it+1)*t-1) for it in range(part_count)]
    rtn=[(  (s+datetime.timedelta(days=it*step)).strftime('%Y-%m-%d')
           ,(s+datetime.timedelta(days=(it+1)*step-1)).strftime('%Y-%m-%d')
         ) for it in range(cnt)]
    max_index=len(rtn)-1
    if max_index >=0:
        rtn[max_index]=(rtn[max_index][0],end)
    return rtn



def fetch_and_save(api,filepath,header,date_start,date_end):
    print "storage: %s"%filepath
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
    print 'to fetch date range: %s ~ %s' %(date_start,date_end)
    if True:
        # 按30天分段，多批进行；日期间隔过长时，友盟返回部分数据有0的空缺
        ranges=batch_date_range(date_start,date_end,30)
        rows=[]
        for item in ranges:
            print '(%s ~ %s)... '%(item[0],item[1]),
            rows+=retrive_umeng(appkey,'active_users',item[0],item[1],args={})
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

    print ''




#-------------------------------------------------------------------------------


try:
    cache=pickle.load(open(cfg['main']['cache_file_name'],'r'))
except:
    cache={}

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
    print '\nAPP: [%s] %s'%(appkey,appname)

    um_api='active_users'
    filepath=script_dir+'data/'+applabel+'_'+um_api+'.csv'
    header=['date','num']

    fetch_and_save(um_api,filepath,header,date_start,date_end)


exit()

if 1==2:
    #UV
    #检查存储文件中是否存在
    filepath=script_dir+'data/'+applabel+'_uv.csv'
    print "storage: %s"%filepath
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
    print 'to fetch date range: %s ~ %s' %(date_start,date_end)
    if True:
        # 按30天分段，多批进行；日期间隔过长时，友盟返回部分数据有0的空缺
        ranges=batch_date_range(date_start,date_end,30)
        rows=[]
        for item in ranges:
            print '(%s ~ %s)... '%(item[0],item[1]),
            rows+=retrive_umeng(appkey,'active_users',item[0],item[1],args={})
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

    print ''

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
