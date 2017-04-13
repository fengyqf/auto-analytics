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



def date2int(txt):
    d=datetime.datetime.strptime(txt, '%Y-%m-%d')
    return d.year*10000+d.month*100+d.day

def int2date(di):
    return '%d-%02d-%02d'%(di/10000,di%1000/100,di%100)





today=datetime.date.today()
date_end=today.strftime('%Y-%m-%d')
date_start=(today-datetime.timedelta(days=60)).strftime('%Y-%m-%d')

print date_start,date_end

appkey='57317f1fe0f55a765300216c'
api='active_users'

retrive_umeng(conn,appkey,api,date_start,date_end)
