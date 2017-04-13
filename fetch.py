#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import time
import ConfigParser
import urllib
import urllib2
import pickle
import json
import base64
import MySQLdb as db


script_dir=os.path.split(os.path.realpath(__file__))[0]+'/'

config_file=script_dir+'/config.ini'
cp=ConfigParser.ConfigParser()
cp.read(config_file)

cfg={'main':{}, 'umeng':{}, 'ga':{}, }

try:
    debug=int(cp.get('main','debug'))
    cfg['main']['cache_file_name']=cp.get('main','cache_file_name')
    cfg['umeng']['email']=cp.get('umeng','email')
    cfg['umeng']['password']=cp.get('umeng','password')

    cfg['mysql']={}
    cfg['mysql']['host']=cp.get('mysql','host')
    cfg['mysql']['user']=cp.get('mysql','user')
    cfg['mysql']['password']=cp.get('mysql','password')
    cfg['mysql']['db']=cp.get('mysql','db')

except :
    #raise ConfigParser.NoOptionError(e)
    print "config.ini ERROR.  You can copy it from config.ini.sample "
    exit()

try:
    cache=pickle.load(open(cfg['main']['cache_file_name'],'r'))
except:
    cache={}


try:
    cache['umeng__auth_token']
    cache['umeng__Authorization']
    print 'auth cache loaded: ', cache['umeng__auth_token'], cache['umeng__Authorization']
except:
    url="http://api.umeng.com/authorize"
    req=urllib2.Request(url,
            urllib.urlencode({'email':cfg['umeng']['email'], 'password':cfg['umeng']['password'], })
        )

    body_raw=urllib2.urlopen(req).read()
    print 're-authorizing...'
    try:
        response=json.loads(body_raw)
        cache['umeng__auth_token']=response['auth_token']
        cache['umeng__Authorization']=base64.b64encode(cache['umeng__auth_token'])
        pickle.dump(cache,open(cfg['main']['cache_file_name'],'w+'))
        print 'auth token cached'
    except:
        print "error"


conn=db.connect(cfg['mysql']['host'],cfg['mysql']['user'],cfg['mysql']['password'],cfg['mysql']['db'])

url="http://api.umeng.com/apps"
req=urllib2.Request(url)
req.add_header('Authorization','Basic %s' %(cache['umeng__Authorization']))

body_raw=urllib2.urlopen(req).read()
#print body_raw
response=json.loads(body_raw)

for app in response:
    print app['appkey'],app['name'],app['platform']

#url="http://api.umeng.com/active_users?appkey=%s" %(appkey)



#日活
def retrive_umeng(conn,appkey,api,date_start,date_end,args={}):

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

    cursor=conn.cursor(cursorclass=db.cursors.DictCursor)
    #检查  hash映射表中是否有当前对应记录，如无则插入
    sql="select hashing,raw from hash_mapping where hashing='%s'"%(hashing)
    cursor.execute(sql)
    results=cursor.fetchall()
    to_recache=1
    if not results:
        sql="insert into `hash_mapping`(hashing,raw) values(%s,%s)"
        cursor.execute(sql,(hashing,param_string))

    #查读最后一批的批号，根据此批号读数据
    line=[]
    sql="select max(batch) as max_batch from data where hashing='%s'" %(hashing)
    cursor.execute(sql)
    result=cursor.fetchone()
    max_batch=result['max_batch']
    sql="select `id`,`day`,`val` from data where hashing='%s' and `batch`='%s'\
         and `day` > %s and `day` < %s order by `day` asc" %(
        hashing,max_batch,date2int(date_start),date2int(date_end))
    cursor.execute(sql)
    results=cursor.fetchall()
    if len(results)>=days_count:
        to_recache=0
    for row in results:
        line.append({'id':row['id'],'day':row['day'],'val':row['val'],})

    print line
    print '----------------------'

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
