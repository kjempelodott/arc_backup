# -*- coding: utf-8 -*-  
# file: sceclient.py
'''
created by caorongqiang
封装了sce的restapi的一个类
?封封封门方法封我
'''

from hashlib import md5
import json
import os
import time 
from urlparse import urlparse

import requests
from requests_toolbelt import MultipartEncoder


class ApiClient():
    def __init__(self, hostname,path,appid,plainPort=80,sslPort=443):
        '''
                 指定根路径和应用id
        hostname:rest.scgrid.cn #不要现现http或https，系统会自动
        path:/restapi3或/v1或/v2
        appid:应用的ID，如test
        '''
        self.__baseURL = hostname
        self.__path=path
        self.__appid = appid
        self.__plainPort=plainPort
        self.__sslPort=sslPort
        self.__client = requests.Session()
        self.__client.headers.update({'Accept':'Application/json'})
        self.__login_flag = False
    def test(self):
        r = self.__client.get("http://www.baidu.com")
        print(r.text)
    def login(self, username, password):
        if(username == None or password == None):
            return 1;  # 用户名或密码为空
        self.__username=username
        self.__password=password
        if self.__sslPort==443:
            urlstr = "%s://%s%s%s" % \
            ("https", self.__baseURL, self.__path,"/users/login")
        else:
            urlstr="%s://%s:%d%s%s" % \
            ("https", self.__baseURL, self.__sslPort,self.__path,"/users/login")
        form = {'appid':self.__appid, \
              'username':username, \
              'password':password, \
            'remember':False}
        resp = self.__client.post(urlstr, data=form, verify=False)
        if resp.status_code==200:
            rjson=json.loads(resp.text, 'utf8');
            self.__cookie_dict=requests.utils.dict_from_cookiejar(resp.cookies)
            if rjson['status_code']==0:
                self.__md5secret=rjson['md5secret']
                #print("md5secret=%s" %self.__md5secret)
                self.__login_flag = True
                return 0 #表示登录成功
        return 2 #登录失败
    def loginOpenID(self, openID, token):
        if(openID == None or token == None):
            return 1;  # 用户名或密码为空
        self.__openID=openID
        self.__token=token
        if self.__plainPort==443:
            urlstr = "%s://%s%s%s" % \
            ("https", self.__baseURL, self.__path,"/users/login/openid")
        else:
            urlstr="%s://%s:%d%s%s" % \
            ("https", self.__baseURL, self.__sslPort,self.__path,"/users/login/openid")
        form = {'appid':self.__appid, \
              'openid':openID, \
              'token':token, \
            'remember':False}
        resp = self.__client.post(urlstr, data=form, verify=False)
        if resp.status_code==200:
            rjson=json.loads(resp.text, 'utf8');
            self.__cookie_dict=requests.utils.dict_from_cookiejar(resp.cookies)
            if rjson['status_code']==0:
                self.__md5secret=rjson['md5secret']
                #print("md5secret=%s" %self.__md5secret)
                self.__login_flag = True
                return 0 #表示登录成功
        return 2 #登录失败
    def active(self,cookies,md5secret):
        self.__client.cookies.update(cookies)#更新cookies
        self.__md5secret=md5secret
    def getCookies(self):
        return self.__cookie_dict
    def getMd5Secret(self):
        return self.__md5secret
    def isLogin(self):
        return self.__login_flag
                       
    def logout(self):
        '''logout
                        退出系统，0表示正常退出
                  '''
        if(self.__login_flag == True):
            if self.__plainPort==443:
                urlstr = "%s://%s%s%s?appid=%s" \
                    % ("https", self.__baseURL,
                        self.__path,"/users/logout",self.__appid)
            else:
                urlstr = "%s://%s:%d%s%s?appid=%s" \
                    % ("https", self.__baseURL, self.__sslPort,
                       self.__path,"/users/logout", self.__appid)
            
            resp = self.__client.get(urlstr, cookies=self.__cookie_dict,verify=False)            
            self.__login_flag = False
            if  resp.status_code==200:
                return 0 #表示正常退出
        return 1 #表示发生各种意外
    def __getUrlWithSingnature(self,urlstr,httpMethod):
        '''make singnature
                    输入URL和调用方法，返回一个有md5sum的URL
                 '''
        if urlstr==None or httpMethod==None or self.__md5secret==None:
            return None
        urlParser=urlparse(urlstr)
        queryString=urlParser.query.strip()        
        queryList=[]
        if len(queryString)>0:
            querys=queryString.split('&')
            for param in querys:
                paramArr=param.split('=')
                pLength=len(paramArr)
                if pLength==2:
                    queryList.append((paramArr[0],paramArr[1]))
                elif pLength==1:
                    queryList.append((paramArr[0],''))
                else:
                    return None
        timenano=str(int(time.time()*1000))
        queryList.append(('timestamp',timenano))
        queryList.sort()
        oList=[]
        for oQuery in queryList:
            if len(oQuery[1])==0:                
                oList.append(oQuery[0])
            else:
                oList.append("%s=%s" %(oQuery[0],oQuery[1]))
        oURL=''.join(oList)
        
        clearUrlStr="%s%s://%s%s%s%s" \
            %(httpMethod,urlParser.scheme,urlParser.netloc,\
              urlParser.path,oURL,self.__md5secret)
        #print("clearURL=%s" %clearUrlStr)
        md5sum=md5(clearUrlStr.encode('utf8')).hexdigest()
        if len(queryString)==0:
            qmark='?'
        else:
            qmark='&'
        md5url="%s%stimestamp=%s&md5sum=%s" \
            %(urlstr,qmark,timenano,md5sum)
        return md5url
    def __getClearURL(self,purlstr):
        '''GET服务'''
        if self.__plainPort==80:
            urlstr="%s://%s%s%s" %("http", self.__baseURL,
                                 self.__path,purlstr)
        else:
            urlstr="%s://%s:%d%s%s" %("http", self.__baseURL,
                                    self.__plainPort,
                                    self.__path,purlstr)
        return urlstr;
    def __processHttpGET(self,purlstr):
        '''GET服务'''
        urlstr=self.__getClearURL(purlstr)
        md5url=self.__getUrlWithSingnature(urlstr,"GET")
        resp = self.__client.get(md5url)
        #print(md5url)
        return resp.text;
        #offset=0&length=10&order=ID
    def __processHttpDELETE(self,purlstr):
        '''DELETE服务'''
        urlstr=self.__getClearURL(purlstr)
        md5url=self.__getUrlWithSingnature(urlstr,"DELETE");
        resp = self.__client.delete(md5url)
        return resp.text;
        #offset=0&length=10&order=ID
    def bjobs(self,queryString='offset=0&length=10&order=ID'):
        '''bjobs 根据不同的条件，查询作业'''
        urlstr = "%s?%s" \
            % ("/jobs",queryString)
        return self.__processHttpGET(urlstr)
    def formatLL(self,jsonstr):
        retDict={}
        if jsonstr==None:
            retDict["status_code"]=503
            return retDict
        newArrFile=[]
        rjson=json.loads(jsonstr, 'utf8')
        if rjson["status_code"]==0:
            items=rjson["items"]
            if items!=None and len(items)>0:
                for item in items:
                    strArr=item.strip().split(" ")
                    newArr=[]
                    for newstr in strArr:
                        newstr2=newstr.strip()
                        if len(newstr2) >0:
                            newArr.append(newstr2)
                    if len(newArr)==9 or len(newArr)==8:
                        newArrFile.append(newArr)
        retDict["status_code"]=0
        retDict["items"]=newArrFile
        return json.dumps(retDict,ensure_ascii=False)
        
    def listJobCS(self,ujid):
        '''list CS 查询作业在CS的目录'''
        urlstr = "/data/jobs/%s/cs" %ujid
        csstr=self.__processHttpGET(urlstr)
        return self.formatLL(csstr)
    def listJobHPC(self,ujid):
        '''list CS 查询作业在CS的目录'''
        urlstr= "/data/jobs/%s/hpc" %ujid
        csstr=self.__processHttpGET(urlstr)
        return self.formatLL(csstr)
    def listJob(self,ujid):
        listCS=self.listJobCS(ujid)
        listHPC=self.listJobHPC(ujid);
        retDict={}
        retDict2={}
        retDict["status_code"]=503
        cjson=json.loads(listCS,'utf8')
        if cjson["status_code"]==0 and len(cjson["items"])>0:            
            items=cjson["items"]
            for item in items:
                if len(item)==9:
                    timestr="%s %s %s" %(item[5],item[6],item[7])
                    fileFlag=True
                    if item[0].lstrip().startswith('d'):
                        fileFlag=False
                    retDict2[item[8].strip()]=[timestr,item[4],True,False,fileFlag]
                else:
                    timestr="%s %s" %(item[5],item[6])
                    fileFlag=True
                    if item[0].lstrip().startswith('d'):
                        fileFlag=False
                    retDict2[item[7].strip()]=[timestr,item[4],True,False,fileFlag]
        hjson=json.loads(listHPC,'utf8')
        if hjson["status_code"]==0 and len(hjson["items"])>0:
            items=hjson["items"]
            for item in items:
                if len(item)==9:
                    timestr="%s %s %s" %(item[5],item[6],item[7])
                    if item[8].strip() in retDict2:
                        fileItem=retDict2.get(item[8].strip())
                        fileItem[0]=timestr
                        fileItem[1]=item[4]
                        fileItem[3]=True
                        retDict2[item[8].strip()]=fileItem
                    else:
                        fileFlag=True
                        if item[0].lstrip().startswith('d'):
                            fileFlag=False                    
                        retDict2[item[8]]=[timestr,item[4],False,True,fileFlag]
                else:
                    timestr="%s %s" %(item[5],item[6])
                    if item[7].strip() in retDict2:
                        fileItem=retDict2.get(item[7].strip())
                        fileItem[0]=timestr
                        fileItem[1]=item[4]
                        fileItem[3]=True
                        retDict2[item[7].strip()]=fileItem
                    else:
                        fileFlag=True
                        if item[0].lstrip().startswith('d'):
                            fileFlag=False                    
                        retDict2[item[7]]=[timestr,item[4],False,True,fileFlag]
        if len(retDict2)>0:
            retDict["status_code"]=0
            retDict["files"]=retDict2
            retDict['total']=len(retDict2)
        return json.dumps(retDict,ensure_ascii=False)
    def getJobGid(self,ujid):
        queryString='offset=0&ujids='+str(ujid)
        ret_str=self.bjobs(queryString)
        try:
            ret_json=json.loads(ret_str,'utf8')
        except:
            print(ret_str)
            return None
        if ret_json['status_code']==0:
            job_list=ret_json['jobs_list']
#             for job in job_list:
#                 print job
            if job_list!=None and len(job_list)==1:
                ret_ujid=job_list[0]['ujid']
                ret_gid=job_list[0]['gid']
                if ret_ujid==ujid:
                    return ret_gid
        return None
    def downloadFile(self,ujid,fileName,local_path=None):
        '''从CS下载'''
        jobid=self.getJobGid(ujid)        
        retDict={}
        if jobid==None:
            return retDict
        urlstr = "/data/jobs/%s/cs/%s" %(jobid,fileName)
        if self.__plainPort==80:
            urlstr="%s://%s%s%s" %("http", self.__baseURL,
                                 self.__path,urlstr)
        else:
            urlstr="%s://%s:%d%s%s" %("http", self.__baseURL,
                                    self.__plainPort,
                                    self.__path,urlstr)
        md5url=self.__getUrlWithSingnature(urlstr,"GET");
        resp = self.__client.get(md5url, headers={'Accept':'*/*'},stream=True)
        #print(resp.headers)
        #print(resp.status_code)
        retDict['status_code']=resp.status_code
        if resp.status_code==200:
            if local_path==None:            
                base_dir=os.getcwd()
            else:
                base_dir=local_path
            #fpath="%s/%d" %(base_dir,ujid)
            #if os.path.exists(fpath)==False:
            #    os.mkdir(fpath)
            #print(fpath)
            fpath=base_dir+"/"+fileName
            fp=open(fpath,'wb')
            for chunk in resp.iter_content(chunk_size=512): 
                if chunk: # filter out keep-alive new chunks
                    fp.write(chunk)
                    fp.flush()
            fp.close()
            retDict['filePath']=fpath
        return retDict
    
    def viewFileContent(self,jobid,fileName,startNum=0,linesNum=10):        
        '''view file 查看文件在HPC的内容'''
        urlstr = "/data/jobs/%s/hpc/%s/view?start_line=%s&lines_num=%s" \
        %(jobid,fileName,startNum,linesNum)
        return self.__processHttpGET(urlstr)
    def killJob(self,jobid):
        '''kill job 杀掉一个正在执行的作业'''
        urlstr = "/jobs/%s" %jobid            
        return self.__processHttpDELETE(urlstr)
    def getQueues(self,app,walltime,cpunum):
        '''find queues 查找可用的应用资源'''
        urlstr = "/resources/applications/%s?corenum=%s&walltime=%s" \
        %(app,cpunum,walltime)
        return self.__processHttpGET(urlstr)
    def submit(self,jsdlXMLContent):
        '''submit JSDL 提交一个作业'''
        purlstr = "/jobs"
        urlstr=self.__getClearURL(purlstr)
        md5url=self.__getUrlWithSingnature(urlstr,"POST");
        form={'jsdlContent':jsdlXMLContent}
        resp = self.__client.post(md5url, data=json.dumps(form,ensure_ascii=False),headers={'Content-Type': 'application/json'})
        return resp.text;
    def submitJSON(self,formDict):
        '''submit JSDL 提交一个作业'''
        purlstr = "/jobs"
        urlstr=self.__getClearURL(purlstr)
        md5url=self.__getUrlWithSingnature(urlstr,"POST");
        form=formDict
        resp = self.__client.post(md5url, data=json.dumps(form,ensure_ascii=False),headers={'Content-Type': 'application/json'})
        return resp.text;
    def putfiles(self,jobid,filesDict):
        '''upload files， 上传一个或多个文件'''
        purlstr = "/data/jobs/%s/cs" %jobid
        urlstr=self.__getClearURL(purlstr)
        md5url=self.__getUrlWithSingnature(urlstr,"POST");
        #{'file': ('report.xls', open('report.xls', 'rb'))}
        #uploads={'file': open('/home/caorongqiang/dataware/python-web/workspace/sceportal/caorq_1415003896305/autoplus.desktop', 'rb')}
        uploads={}
        count=0
        for fname in filesDict:
            uploads['field'+str(count)]=(fname, open(filesDict[fname],'rb'),'application/octet-stream')
            count+=1
        m=MultipartEncoder(fields=uploads)
        resp = self.__client.post(md5url, data=m,headers={'Content-Type': m.content_type}) 
        return resp.text;
    def run(self,jobid):
        '''start job， 开始运行一个作业'''
        purlstr = "/jobs/%s/status?job_status=start" %jobid
        urlstr=self.__getClearURL(purlstr)
        md5url=self.__getUrlWithSingnature(urlstr,"PUT");
        resp = self.__client.put(md5url)
        return resp.text;
    def update_status(self,minute):
        '''list jobs whose status is updated in recent time'''
        if minute==None or minute<0:
            minute=5
        urlstr = "/jobs/update?timeint=%d" %minute
        return self.__processHttpGET(urlstr)
        
'''
 functions for pbs commands
'''
APP_ID='atlas' # app id,gloable variable
LOGIN_JSON_FILE="/tmp/grid.sceapi.json"

def writeJson2file(login_dict):
    if login_dict:
        out_json=json.dumps(login_dict,ensure_ascii=False)
        output=open(LOGIN_JSON_FILE,'w')
        output.write(out_json)
        output.close()
        
def readJson2Dict():
    finput=open(LOGIN_JSON_FILE,'r')
    json_str_list=finput.readlines()
    finput.close()
    if json_str_list!=None and len(json_str_list)==1:
        login_dict=json.loads(json_str_list[0], 'utf8')
        return (login_dict['cookies'],login_dict['md5secret'],login_dict['hostname'],login_dict['version'],login_dict['plainPort'],login_dict['sslPort'])
    return (None,None,None,None,None,None)

def upLoginTime():
    exist_flag = os.path.exists(LOGIN_JSON_FILE)
    if exist_flag:
        nowtime=time.time()
        os.utime(LOGIN_JSON_FILE,(nowtime, nowtime))

def isLogin():  
    '''
    check wether login?
    '''
    exist_flag = os.path.exists(LOGIN_JSON_FILE)
    login_flag=False    
    nowtime=time.time()
    if exist_flag:
        statinfo=os.stat(LOGIN_JSON_FILE)
        #print(int(statinfo.st_atime))
        difftime=int(nowtime-statinfo.st_atime)
        #print(difftime)
        if difftime>600:
            os.remove(LOGIN_JSON_FILE)
            login_flag=False
        else:
            login_flag=True
            os.utime(LOGIN_JSON_FILE,(nowtime, nowtime))
    return login_flag

def isLoginUser(username):  
    '''
    check wether login?
    '''
    if username==None:
        return False
    if isLogin():
        cookies,md5secret,hostname,version,_,_=readJson2Dict()
        user_cookie=cookies['username']
        if username==user_cookie:
            return True
    return False
def login(username,password,hostname='rest.scgrid.cn',version='v2',port=80,sslport=443):
    if isLogin()==True:# check wether login?
        return   
    httpClient = None
    if port==80:
        httpClient= ApiClient(hostname,"/"+version, APP_ID)
    else:
        if sslport==443:
            httpClient= ApiClient(hostname,"/"+version, APP_ID,plainPort=port) 
        else:
            httpClient= ApiClient(hostname,"/"+version, APP_ID,plainPort=port,sslPort=sslport)
    ret_code=httpClient.login(username, password)
    if ret_code==0:#login successfully;combine json String
        login_json_dict={}
        login_json_dict['hostname']=hostname
        login_json_dict['version']=version
        cookies_dict={}
        ret_cookies=httpClient.getCookies()
        for cookie_name in ret_cookies.keys():
            cookies_dict[cookie_name]=ret_cookies[cookie_name]        
        login_json_dict['cookies']=cookies_dict
        ret_md5secret=httpClient.getMd5Secret();
        login_json_dict['md5secret']=ret_md5secret
        login_json_dict['plainPort']=port
        login_json_dict['sslPort']=sslport
        writeJson2file(login_json_dict)
    return ret_code
def getHttpClient():
    cookies,md5secret,hostname,version,plainPort,sslPort=readJson2Dict()
    if cookies==None:
        print("")
        return
    httpClient = ApiClient(hostname,"/"+version, APP_ID,plainPort=plainPort,sslPort=sslPort)
    httpClient.active(cookies, md5secret)
    return httpClient
def checkLogin():
    if isLogin()==False:
        print('you need to login first')
        exit(10)
if __name__ == "__main__":
    isLogin()
    #print("这是一个测试＝%s,%s" % ("adfafs", "dafaf",))
    #httpClient = ApiClient("rest.scgrid.cn","/v2", "test")
    #httpClient = ApiClient("localhost","/restapi3", "test",
    #                        plainPort=8080,sslPort=8443)     
    #httpClient.login("caorq", "caorq123")
    #httpClient.login("liuqian", "111111")
    #rbjobs=httpClient.bjobs("ujids=925");
    #print(rbjobs)
    #rjob=httpClient.listJob("925");
    #print(rjob);
    #httpClient.downloadFile("1384876729189654904", '1384876729189654904.xml2');
    #httpClient.logout()
    #rkill=httpClient.killJob('925')
    #print(rkill)
    #rcontent=httpClient.viewFileContent("956", 'stdout', '-5', '2')
    #print(rcontent)
    #rqueues=httpClient.getQueues("gaussian",'120','2')
    #print(rqueues)
    #text='<JobDefinition  xmlns="http://schemas.ggf.org/jsdl/2005/10/jsdl"><JobDescription><JobIdentification><JobName>jobname</JobName></JobIdentification><Application><ApplicationName>VIC</ApplicationName><POSIXApplication><Executable>gaussian</Executable><Argument></Argument><Output>stdout</Output><Error>stderr</Error><WallTimeLimit>10</WallTimeLimit></POSIXApplication></Application><Resources><HostName>any</HostName><CPUCount>2</CPUCount><queue>any</queue></Resources><DataStaging><FileName>autoplus.desktop</FileName><DeleteOnTermination>true</DeleteOnTermination><Source><URI>autoplus.desktop</URI></Source></DataStaging><DataStaging><FileName>stderr</FileName><DeleteOnTermination>true</DeleteOnTermination><Target><URI>stderr</URI></Target></DataStaging></JobDescription></JobDefinition>'
    #filesDict={'autoplus.desktop': '/home/caorongqiang/dataware/python-web/workspace/sceportal/caorq_1415003896305/autoplus.desktop', 'thinkpython.pdf': \
 #'/home/caorongqiang/dataware/python-web/workspace/sceportal/caorq_1415003896305/thinkpython.pdf'}
    #httpClient.putfiles("1415758780214729233", filesDict)
    #rsubmit=httpClient.submit(text)
    #print(rsubmit)
    #strtest="-rw-rw-r--. 1 caorongqiang caorongqiang  2203 10月 30 14:32 AuthAPIQueryData.class"
    #strtest="-rw-rw-r--. 1 caorongqiang caorongqiang 1.1K 10月 30 15:26 AuthDataQueryService$AuthUpdateThread.class"
#     strtest="-rw-rw-r--  1 sce sce  27K  Nov  8  2013 water.log"
#     strArr=strtest.strip().split(" ")
#     newArr=[]
#     for newstr in strArr:
#         newstr2=newstr.strip()
#         if len(newstr2) >0:
#             newArr.append(newstr2)
#     print(len(newArr))
#     print(newArr)
#     pass;
