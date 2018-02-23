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



def fetch_and_save(appkey,api,filepath,header,date_start,date_end):
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
        for rng in ranges:
            print '(%s ~ %s)... '%(rng[0],rng[1]),
            rows+=retrive_umeng(appkey,api,rng[0],rng[1],args={})
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


def retrive_retentions(appkey,filepath,date_start,date_end,args={}):
    try:
        period_type=args['period_type']
        rate_count=9999    #留存率数值个数，写csv时按该值补空列
        csv_header=()
    except:
        period_type='daily'
        rate_count=9
        csv_header=('date','1d','2d','3d','4d','5d','6d','7d','14d','30d')

    if period_type!='daily':
        print "[retrive_retentions] Not implement, unless by daily"
        exit()

    print "storage: %s"%filepath
    lines_dict={}
    if not os.path.isfile(filepath):
        pass
    else:
        with open(filepath,'rb') as fp:
            f_csv=csv.reader(fp)
            next(f_csv)
            for line in f_csv:
                if line:
                    lines_dict[line[0]]=tuple(line[1:])
                    csv_date=line[0]
    try:
        #date_start 按csv_date再往前提50天，以获取完整的留存数据列
        date_start=(datetime.datetime.strptime(csv_date,'%Y-%m-%d')-datetime.timedelta(days=50)).strftime('%Y-%m-%d')
    except:
        pass
    print 'to fetch date range: %s ~ %s' %(date_start,date_end)

    ranges=batch_date_range(date_start,date_end,30)
    for rng in ranges:
        print '(%s ~ %s)... '%(rng[0],rng[1]),
        url="http://api.umeng.com/%s?appkey=%s&period_type=%s&start_date=%s&end_date=%s"%(
                'retentions',appkey,period_type,rng[0],rng[1])
        req=urllib2.Request(url)
        req.add_header('Authorization','Basic %s' %(Um_auth.cache['umeng__Authorization']))
        f=urllib2.urlopen(req)
        body_raw=f.read()
        response=json.loads(body_raw)

        for item in response:
            label=item['install_period'].encode('utf-8')
            rates=[str(rate) for rate in item['retention_rate']]
            rates+=['']*(rate_count-len(rates))
            lines_dict[label]=tuple(rates)
    print ''

    keys=lines_dict.keys()
    keys.sort()
    lines=[(k,)+lines_dict[k]  for k in keys]
    with open(filepath,'w+') as fp:
        f_csv=csv.writer(fp)
        f_csv.writerow(csv_header)
        f_csv.writerows(lines)



#-------------------------------------------------------------------------------
def run():
    script_dir=os.path.split(os.path.realpath(__file__))[0]+'/'
    config_file=script_dir+'/config.ini'

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

        um_api='retentions'
        filepath = script_dir+'data/'+applabel+'_'+um_api+'.csv'
        retrive_retentions(appkey,filepath,date_start,date_end)

        for um_api in ['new_users','active_users','launches']:
            filepath=script_dir+'data/'+applabel+'_'+um_api+'.csv'
            csv_header=['date','num']
            fetch_and_save(appkey,um_api,filepath,csv_header,date_start,date_end)
        print ''



if __name__ == '__main__':
    run()


