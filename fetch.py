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
import datetime


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
print body_raw
response=json.loads(body_raw)

for app in response:
    print app['appkey'],app['name'],app['platform']

#url="http://api.umeng.com/active_users?appkey=%s" %(appkey)



today=datetime.date.today()
date_end=today.strftime('%Y-%m-%d')
date_start=(today-datetime.timedelta(days=5)).strftime('%Y-%m-%d')

print date_start,date_end





