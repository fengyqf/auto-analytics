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







sys.exit()

time_start=time.time()

stop_words_u=[it.decode('utf-8') for it in stop_words]

for file in os.listdir(script_dir):
    if not (file[0:2] in ['a_','a.'] and file[-4:]=='.txt') :
        continue
    r=open(script_dir+file)
    counts={}
    print "File: %s" %file
    for word_width in range(word_width_min,word_width_max+1):
        r.seek(0)
        print "  %d char-width words" %word_width
        for line in r.readlines():
            if debug:
                print line
            line_u=line.decode('utf-8')
            line_u_len=len(line_u)
            i=0;
            accepted_count=0;
            while(i < line_u_len-word_width):
                i+=1;
                word = line_u[i:(i+word_width)]
                flag_stop=0
                for sw in stop_words_u:
                    if word.find(sw) >= 0:
                        #print '  stoped for %s' %sw
                        flag_stop+=1
                        continue
                if flag_stop==0:
                    counts[word] = counts.get(word, 0) + 1
                    #buff.append(word)
                    accepted_count+=1
                #print '    %s acceped' %(accepted_count)

    r.close()

    print 'finished cutting, %d words.' %len(counts)


    sorted_counts = list(counts.items())
    sorted_counts.sort(lambda a,b: -cmp((a[1], a[0]), (b[1], b[0])))

    output='times\tword\n'
    for item in sorted_counts:
        if item[1] < output_words_min_count:
            break
        output+= '%d\t%s\n' %(item[1],item[0].encode('utf-8'))

    output_file_path=script_dir+'output_'+file[:-4]+'.txt'
    if os.path.exists(output_file_path):
        timestamp=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        os.rename(output_file_path,'%soutput_%s_bak%s.txt'%(script_dir,file[:-4],timestamp))

    w=open(output_file_path,'w+')
    w.write(output)
    w.flush()
    w.close()

print 'written to '+output_file_path

time_end=time.time()
print '\nfrom %f to %f,   %f seconds taken' %(time_start,time_end,time_end-time_start)
