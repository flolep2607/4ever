#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from IPython.core import ultratb
sys.excepthook = ultratb.FormattedTB(mode='Verbose', color_scheme='Linux', call_pdb=False)
from keepalive import keep_alive
import mysql.connector.pooling
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
import requests
import feedparser
import re
from lxml import html, etree
import zlib
import threading
import os
import time
import ssl
import queue
import socket
import re
import traceback
keep_alive()
import ftplib


if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context
#if os.path.isfile("a"):
#    with open("a", "r") as f:
#        timer = float(f.read())
#        if time.time()-timer < 60:
#            exit()


headers = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 (compatible; AdsBot-Google-Mobile; +http://www.google.com/mobile/adsbot.html)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'deflate',
    'Referer': 'https://api.jeuxvideo.com/',
    'TE': 'trailers',
}
mysql.connector.threading = True

regex = r" \(([0-9]+) réponses\)$"
paging = r"[0-9]+\-[0-9]+\-[0-9]+\-([0-9]+)\-[0-9]+\-[0-9]+\-[0-9]+"
paging2 = r"([0-9]+\-[0-9]+\-[0-9]+)\-[0-9]+(\-[0-9]+\-[0-9]+\-[0-9]+)"
code_extract = r"[0-9]+\-[0-9]+\-([0-9]+)\-[0-9]+\-[0-9]+\-[0-9]+\-[0-9]+"
db_pass = os.environ['db_pass']
db_user = os.environ['db_user']
db_name = os.environ['db_name']
END_RUNNING = False
sql_inser_thread = "INSERT IGNORE INTO threads (code,responses,done,link,title) VALUES (%s, %s, %s, %s, %s)"
sql_inser_msg1 = "INSERT IGNORE INTO responses (code,id,user_name) VALUES (%s, %s, %s)"
sql_inser_msg2 = "INSERT IGNORE INTO html_rep (id,html) VALUES (%s, %s)"
sql_inser_user = "REPLACE user (user_name,user_avatar) VALUES (%s, %s)"
QUEUE_sql_inser_msg = queue.Queue(500000)
QUEUE_sql_inser_msg2 = queue.Queue(500000)
QUEUE_sql_inser_user = queue.Queue(50000)
images_download = queue.Queue(5000000)
works = queue.Queue(50000)
sql_count_msg = "SELECT COUNT(*) from responses where code = %s" # ayao ca peut pas tenir
sql_count_resp = "SELECT responses from threads where code = %s"
sql_update_thread_msgs = "UPDATE threads set responses = %s where code = %s"
sql_update_thread_done = "UPDATE threads set done = %s where code = %s"
sql_update_deleted = "UPDATE threads set deleted = %s, deleted_time = current_timestamp() where code = %s"
sql_isdone = "SELECT responses=done from threads where code = %s"
sql_code2linke = "SELECT link from threads where code = %s limit 1"
sql_code2donee = "SELECT done,responses from threads where code = %s limit 1"
# sql_get2doo = "SELECT code from threads where done < responses and responses > 0 and deleted = 0 order by responses ASC LIMIT 20"
# sql_get2doo = "SELECT code from threads where done < responses and responses > 500 and deleted = 0 ORDER BY created DESC LIMIT 20"
sql_get2doo = "SELECT code from threads where done < responses and responses > 0 and responses < 100 and deleted = 0 ORDER BY updated DESC LIMIT 20"

MAX_threads=int(os.environ["MAX_threads"])
print(MAX_threads)
TIME_LOCK=threading.Lock()
class Database:
    def __init__(self,number=10):
        self.cursors=[
            {
                "cursor":None,
                "LOCK":threading.Lock(),
            } for _ in range(number)
        ]
        self.connections=[]
        self.threads=number
        self.modif=0
        self.params={
            "pool_name": "mypool", 
            "pool_size": self.threads,
            "host":"localhost" if os.environ['host'] in socket.gethostname() else "109.234.164.54",
            "user":db_user,
            "password":db_pass,
            "database":db_name,
            "autocommit":True,
            "charset":'utf8mb4',
            "use_unicode":True
        }
        print("HOST:",self.params["host"])
        # self.pool=mysql.connector.connect(
        #     # pool_name=self.params["pool_name"],
        #     # pool_size=self.params["pool_size"],
        #     host=self.params["host"],
        #     user=self.params["user"],
        #     password=self.params["password"],
        #     database=self.params["database"],
        #     autocommit=self.params["autocommit"]
        # )
        for _ in range(self.threads):
            # self.connections.append(self.pool.get_connection())
            try:
	            self.connections.append(mysql.connector.connect(
	                host=self.params["host"],
	                user=self.params["user"],
	                password=self.params["password"],
	                database=self.params["database"],
	                autocommit=self.params["autocommit"],
                    use_unicode=self.params["use_unicode"]
	            ))
            except:pass
        self.threads=len(self.connections)
        self.reconnecting=[False for _ in range(self.threads)]
        self.connecting=[False for _ in range(self.threads)]
    def _connect(self,i:int):
        if self.connecting[i]==False:
            self.connecting[i]=True
            try:
            	self.connections[i] = mysql.connector.connect(
	                # pool_name=self.params["pool_name"],
	                # pool_size=self.params["pool_size"],
	                host=self.params["host"],
	                user=self.params["user"],
	                password=self.params["password"],
	                database=self.params["database"],
	                autocommit=self.params["autocommit"],
                    use_unicode=self.params["use_unicode"]
	            )
            except Exception as e:
            	print("UwU:",e)
            self.connecting[i]=False
        # print("connected.")
    def pinger(self,i=0,ever=False):
        if ever:
            while True:
                i=(i+1)%self.threads
                if self.cursors[i]["cursor"]:
                    self.execute("SELECT 1;",i=i)
        else:
            self.execute("SELECT 1;",i=i)
    def _reconnect(self,i:int):
        if self.reconnecting[i]==False:
            self.reconnecting[i]=True
            tests=0
            while tests<3 and not self.connections[i].is_connected():
                try:
                    print("reconnecting")
                    self.connections[i].reconnect(attempts=10, delay=0.5)
                    self.reconnecting[i]=False
                    return
                except Exception as err:#mysql.connector.errors.InterfaceError:
                    print(";;",err)
                    time.sleep(2)
                    tests+=1
            self.reconnecting[i]=False
            return Exception("veut plus se reconnecter")
            if not self.connections[i].is_connected():
                try:
                    print("connection")
                    self._connect(i)
                    self.reconnecting[i]=False
                except Exception as err:
                    print("||",err)
                    exit()
        else:
            time.sleep(5)
    def get_cursor(self,i:int):
        while True:
            try:
                if not self.connections[i] or not self.connections[i].is_connected():
                    self._reconnect(i)
                print("uwu",self.connections[i],self.connections[i].is_connected())
                self.cursors[i]["cursor"]=self.connections[i].cursor(buffered=True)
                self.cursors[i]["cursor"].execute('SET NAMES utf8mb4')
                self.cursors[i]["cursor"].execute("SET CHARACTER SET utf8mb4")
                self.cursors[i]["cursor"].execute("SET character_set_connection=utf8mb4")
                break
            except Exception as err:#mysql.connector.errors.InterfaceError:
                print(traceback.format_exc())
                print("!!!",err)
                time.sleep(2)
    def pingpong(self,i:int,*args,**kwargs):
        return self.connections[i].ping(*args,**kwargs)
    def execute(self,*args,**kwargs):
        if 'i' in kwargs:
            i=kwargs['i']
            kwargs.pop('i')
        else:
            i=0
            while self.cursors[i]["LOCK"].locked():
                # print("locked")
                i=(i+1)%self.threads
        with self.cursors[i]["LOCK"]:
            if not self.cursors[i]["cursor"]:
                self.get_cursor(i)
                #print("AAAAAA",*args)
            while True:
                try:
                    self.modif+=1
                    if not self.params["autocommit"] and self.modif%1000==0:
                        print("commit:",self.modif)
                        self.commit()
                    if 'fetchall' in kwargs:
                        #print("FETCHALL")
                        kwargs.pop("fetchall")
                        self.cursors[i]["cursor"].execute(*args,**kwargs)
                        return self.cursors[i]["cursor"].fetchall()
                    elif 'fetchone' in kwargs:
                        #print("FETCHONE")
                        kwargs.pop("fetchone")
                        self.cursors[i]["cursor"].execute(*args,**kwargs)
                        return self.cursors[i]["cursor"].fetchone()
                        self.cursors[i]["cursor"].close()
                        self.get_cursor(i)
                        return tmp
                    elif 'lastrowid' in kwargs:
                        print("LASTROWID")
                        kwargs.pop("lastrowid")
                        self.cursors[i]["cursor"].execute(*args,**kwargs)
                        #self.connections[i].commit()
                        return self.cursors[i]["cursor"].lastrowid
                    elif 'commit' in kwargs:
                        #print("COMMIT")
                        kwargs.pop("commit")
                        if 'connection' in kwargs:
                            kwargs.pop("connection")
                            self.cursors[i]["cursor"].execute(*args,**kwargs)
                            if not self.params["autocommit"]:self.connections[i].commit()
                            return i
                        else:
                            self.cursors[i]["cursor"].execute(*args,**kwargs)
                            if not self.params["autocommit"]:
                                return self.connections[i].commit()
                            return True
                    elif 'rowcount' in kwargs:
                        # print("ROWCOUNT")
                        kwargs.pop("rowcount")
                        self.cursors[i]["cursor"].execute(*args,**kwargs)
                        return self.cursors[i]["cursor"].rowcount
                    else:
                        self.cursors[i]["cursor"].execute(*args,**kwargs)
                        return i
                except mysql.connector.errors.IntegrityError as err:
                    print("integrity error",err)
                    return False
                except Exception as err:#mysql.connector.errors.InterfaceError:
                    print('EE)',err)
                    print(args,kwargs)
                    # self._connect()
                    time.sleep(0.5)
                    self.get_cursor(i)
    def commit(self,i:int=None):
        if self.params["autocommit"]:return
        if i==None:
            for i in range(self.threads):
                self.connections[i].commit()
        else:
            self.connections[i].commit()
    def executemany(self,*args,**kwargs):
        if 'rowcount' in kwargs:
            kwargs.pop("rowcount")
            modif=0
            for row in args[1]:
                modif+=self.execute(args[0],row,rowcount=True)
            return modif
        i=0
        while self.cursors[i]["LOCK"].locked():
            i=(i+1)%self.threads
        with self.cursors[i]["LOCK"]:
            if not self.cursors[i]["cursor"]:
                self.get_cursor(i)
                #print("BBBBBB",*args)
            while True:
                try:
                    self.cursors[i]["cursor"].executemany(*args,**kwargs)
                    if not self.params["autocommit"]:
                        self.connections[i].commit()
                    return True
                except mysql.connector.errors.IntegrityError as err:
                    print("integrity error",err)
                    return False
                except Exception as err:#mysql.connector.errors.InterfaceError:
                    print('EE)',err)
                    print(args,kwargs)
                    # self._connect()
                    time.sleep(2)
                    self.get_cursor(i)
    def sql_get2do(self):
        ROWS=self.execute(sql_get2doo,fetchall=True)
        I=0
        print("LEN:",len(ROWS))
        for row in ROWS:
            # print(row,row[0])
            yield row[0]
    def link2code(self, link):
        return re.findall(code_extract, link)[0]
    def code2link(self, code):
        print("start",code)
        try:
            url=self.execute(sql_code2linke, (code,),fetchall=True)[0][0]
            print("end")
            return url
        except:
            return None
    def code2done(self, code):
        return self.execute(sql_code2donee, (code,),fetchall=True)[0]
    def add(self, link, responses, title):
        code = self.link2code(link)
        # print("code add:", code)
        i=self.execute(sql_inser_thread, (code, responses, 0, link, title))
        if not self.params["autocommit"]:self.commit(i)
        return code
    #def addmsg(self, code, id, html, page):
    #    self.execute(sql_inser_msg, (code, id, html, page))
    def addmsgs1(self, LISTE):
        print("!!addmsgs")
        # edited=self.executemany(sql_inser_msg, LISTE,rowcount=True)
        self.executemany(sql_inser_msg1, LISTE)
    def addmsgs2(self, LISTE):
        print("!!addmsgs")
        # edited=self.executemany(sql_inser_msg, LISTE,rowcount=True)
        self.executemany(sql_inser_msg2, LISTE)
    def updatelen(self, code):
        # code=self.link2code(link)
        length=self.execute(sql_count_msg, (code,),fetchone=True)
        if isinstance(length, list) or isinstance(length,tuple):
            length=length[0]
        length2=self.execute(sql_count_resp, (code,),fetchone=True)[0]
        if isinstance(length2, list) or isinstance(length2,tuple):
            length2=length2[0]
        i=self.execute(sql_update_thread_done, (length, code))
        if length > length2:
            self.execute(sql_update_thread_msgs, (length, code))
        if not self.params["autocommit"]:self.commit(i)
        return length, length2
    def deleted(self, code, reason):
        #code = self.link2code(link)
        if reason == "author":
            i=self.execute(sql_update_deleted, (1,code))
        else:
            i=self.execute(sql_update_deleted, (2,code))
        if not self.params["autocommit"]:self.commit(i)
    def done(self, link, number):
        code = self.link2code(link)
        i=self.execute(sql_update_thread_msgs, (number, code))
        if not self.params["autocommit"]:self.commit(i)        
    def isdone(self, code):
        self.execute(sql_isdone, (code,))
        return self.fetchone()[0]

TIME=time.time()

DB = Database()
# threading.Thread(target=DB.pinger,kwargs={'ever':True}).start()

def threaded_user():
    LISTEE=[]
    while not END_RUNNING or not QUEUE_sql_inser_user.empty():
        user=QUEUE_sql_inser_user.get()
        if not user in LISTEE:
            LISTEE.append(user)
            if len(LISTEE)>500:
                print('\033[91m'+"BOOOOOOOOOOOOOOOOOOOOOOOOOOOOM1"+'\033[0m')
                print('\033[91m'+"BOOOOOOOOOOOOOOOOOOOOOOOOOOOOM2"+'\033[0m')
                print('\033[91m'+"BOOOOOOOOOOOOOOOOOOOOOOOOOOOOM3"+'\033[0m')
                print('\033[91m'+"BOOOOOOOOOOOOOOOOOOOOOOOOOOOOM4"+'\033[0m')
                print('\033[91m'+"BOOOOOOOOOOOOOOOOOOOOOOOOOOOOM5"+'\033[0m')
                DB.executemany(sql_inser_user,LISTEE)
                LISTEE=[]
    DB.executemany(sql_inser_user,LISTEE)

def threaded_msg1():
    LISTEE=[]
    while not END_RUNNING or not QUEUE_sql_inser_msg.empty():
        msg=QUEUE_sql_inser_msg.get()
        LISTEE.append(msg)
        if len(LISTEE)>500:
            codes=list(set([code for code,_,_ in LISTEE]))
            print('\033[91m'+"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAM1"+'\033[0m')
            print('\033[91m'+"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAM2"+'\033[0m')
            print('\033[91m'+"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAM3"+'\033[0m')
            print('\033[91m'+"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAM4"+'\033[0m')
            print('\033[91m'+"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAM5"+'\033[0m')
            DB.executemany(sql_inser_msg1,LISTEE)
            for code in codes:DB.updatelen(code)
            LISTEE=[]
    DB.executemany(sql_inser_msg1,LISTEE)

def threaded_msg2():
    LISTEE=[]
    while not END_RUNNING or not QUEUE_sql_inser_msg2.empty():
        msg=QUEUE_sql_inser_msg2.get()
        LISTEE.append(msg)
        if len(LISTEE)>50:
            print('\033[91m'+"BIIIIIIIIIIIIIIIIIIIIIIIIIIIIM1"+'\033[0m')
            print('\033[91m'+"BIIIIIIIIIIIIIIIIIIIIIIIIIIIIM2"+'\033[0m')
            print('\033[91m'+"BIIIIIIIIIIIIIIIIIIIIIIIIIIIIM3"+'\033[0m')
            print('\033[91m'+"BIIIIIIIIIIIIIIIIIIIIIIIIIIIIM4"+'\033[0m')
            print('\033[91m'+"BIIIIIIIIIIIIIIIIIIIIIIIIIIIIM5"+'\033[0m')
            DB.executemany(sql_inser_msg2,LISTEE)
            LISTEE=[]
    DB.executemany(sql_inser_msg2,LISTEE)


#file = open('kitten.jpg','rb')                  # file to send
#ftp_session.storbinary('STOR kitten.jpg', file)     # send the file
#file.close()                                    # close file and FTP
#ftp_session.quit()

ftp_session=ftplib.FTP(os.environ['ftp_server'],os.environ['ftp_user'],os.environ['ftp_pass'])
ftp_session.set_pasv(True)
ftpfiles=ftp_session.nlst()

def uploadFile(filename):
    thread_ftp = ftplib.FTP(os.environ['ftp_server'],os.environ['ftp_user'],os.environ['ftp_pass'])
    done=False
    while not done:
        if thread_ftp==None:
            thread_ftp=ftplib.FTP(
                    os.environ['ftp_server'],
                    os.environ['ftp_user'],
                    os.environ['ftp_pass']
                )
        with open("tmp/"+filename, "rb") as handle:
            try:
                thread_ftp.storbinary('STOR %s'%filename, handle)
                done=True
            except Exception as e:
                print(e)
                thread_ftp=None
                time.sleep(2)
    os.remove("tmp/"+filename)

#with ThreadPoolExecutor(max_workers=10) as POOOL:
    #for (dirpath, dirnames, filenames) in os.walk("tmp/"):
        #for filename in filenames:
        #    uploadFile(filename)
        #POOOL.map(uploadFile,filenames)
    
    #for filename in filenames:
    #    print(filename)
    #    done=False
    #    while not done:
    #        if ftp_session==None:
    #            ftp_session=ftplib.FTP(
    #                os.environ['ftp_server'],
    #                os.environ['ftp_user'],
    #                os.environ['ftp_pass']
    #            )
    #        with open("tmp/"+filename, "rb") as handle:
    #            try:
    #                ftp_session.storbinary('STOR %s'%filename, handle)
    #                done=True
    #            except:
    #                ftp_session=None
    #                time.sleep(2)
    #    os.remove("tmp/"+filename)

def down_img(url,path,s):
    print(url)
    with s.get(url, headers=headers,stream=True) as response:#
        #print("yup",url,response.status_code)
        with open("tmp/"+path, "wb") as handle:
            for data in response.iter_content():
                handle.write(data)
    uploadFile(path)
    return
    done=False
    while not done:
        if ftp_session==None:
            ftp_session=ftplib.FTP(
                os.environ['ftp_server'],
                os.environ['ftp_user'],
                os.environ['ftp_pass']
            )
        with open("tmp/"+path, "rb") as handle:
            try:
                ftp_session.storbinary('STOR %s'%path, handle)
                done=True
            except:
                ftp_session=None
                time.sleep(2)
    os.remove("tmp/"+path)


def threaded_image():
    global ftpfiles
    with requests.Session() as s:
        with ThreadPoolExecutor(max_workers=3) as executor:
            while not END_RUNNING or not images_download.empty():
                code,url=images_download.get()
                if "%s.png"%code in ftpfiles:continue
                ftpfiles.append("%s.png"%code)
                executor.submit(down_img,url,"%s.png"%code,s)
                #down_img(url,"tmp/%s.png"%code)
                time.sleep(0.2)

threading_user=threading.Thread(target=threaded_user)
threading_user.start()
threading_msg1=threading.Thread(target=threaded_msg1)
threading_msg1.start()
threading_msg2=threading.Thread(target=threaded_msg2)
threading_msg2.start()
#threading_image=threading.Thread(target=threaded_image)
#threading_image.start()

KNOWN={}
def download_image(link):
    query="INSERT INTO `noel2flo` (`name`, `noel_link`) VALUES (MD5(RAND()), %s)"
    i=DB.execute(query,(link,),commit=True,connection=True)
    #print("<=",link)
    query="SELECT `name` FROM `noel2flo` WHERE noel_link=%s limit 1"
    code=DB.execute(query,(link,),fetchall=True,i=i)
    if code and code[0]:
        code=code[0][0]
        #print("CODEEEEEEEEEE:",code)
        images_download.put((code,link))
        return code
    else:
        print("!!!??!!",link,code)
        exit()

def image_modifier(text):
    query="SELECT `name` FROM `noel2flo` WHERE noel_link=%s limit 1"
    regex = r"https?:\/\/image\.noelshack\.com\/[a-z]+\/[0-9]{4}\/[0-9]{2}\/(?:[0-9]\/)?[0-9]{10}-[0-9a-zA-Z\_\-]+\.png"
    founds=list(set(re.findall(regex,text)))
    for found in founds:
        if KNOWN.get(found):
            text=text.replace(found,"https://image.flolep.fr/%s.png"%KNOWN.get(found))
        name=DB.execute(query,(found,),fetchall=True)
        if not name:
            name=download_image(found)
        while found in text:
            text=text.replace(found,"https://image.flolep.fr/%s.png"%name)
    return text
            
    #"https://image.noelshack.com/minis/2018/10/4/1520537807-messirire.png"

YUP=[]
def feedmodifier(feed, DB):
    global YUP
    for entry in feed.get("entries"):
        title = entry.get("title")
        number = int(re.findall(regex, title)[0])
        title = re.sub(regex, "", title, 1)
        entry["title"] = title
        entry["responses"] = number
        if number>100:continue
# print(title,entry.get("link"),number)
        code=DB.add(entry.get("link").replace("www", "api", 1), number, title)
        if not code in YUP:
            works.put(code)
            #YUP.append(code)
            #executor.submit(requestor,code)


def textcompress(string):
    while '\n' in string:
        string=string.replace('\n', '')
    while '  ' in string:
        string=string.replace('  ', ' ')
    while ' class="JvCare' in string:
        toreplace=string.split(' class="JvCare')[1].split('"')[0]
        string=string.replace(' class="JvCare'+toreplace+'"','')
    string=image_modifier(string) ###!!
    return string
    return string.replace(b'\n   ', b'').replace(b'   ', b'').replace(b'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7" data-srcset="', b'')
#	return zlib.compress(string.replace(b'\n   ',b'').replace(b'   ',b''))

def parser(tree):
    user=tree.cssselect('a img.user-avatar-msg')
    if len(user):
        user_img=user[0]
        user_avatar=user_img.get("data-src") or user_img.get("src")
        #user_code=user_img.getparent().get("href").split("=")[1].strip()
        user_name=tree.cssselect("div.who-post a.bloc-pseudo-msg")[0].text.strip()
    else:
        user_img=None
        user_avatar=None
        #user_code=None
        user_name=None
    # post_date=tree.cssselect("div.date-post")[0].text
    if user_avatar=="https://image.jeuxvideo.com/avatar-sm/default.jpg":user_avatar=None
    contenu=etree.tostring(tree.cssselect('div.contenu div.message')[0], method='html').decode().replace('<div class="message  text-enrichi-fmobile  text-crop-fmobile" data-cropfmobile="">','')[::-1].replace(">vid/<","",1)[::-1].strip()
    return {
        "user_avatar":user_avatar,
        "contenu":textcompress(contenu),
        #"user_code":user_code,
        "user_name":user_name
    }

def scrape(response, link, code, page):
    print("=>", link)
    root = html.fromstring(response)
    LISTE=0
    for post in root.cssselect('div.post'):
        id = post.get("id").replace('post_', '')
#		DB.addmsg(code,id,textcompress(etree.tostring(post,method='html')))
        # DB.addmsg(code, id, textcompress(etree.tostring(post, method='html')), page)
        rep=parser(post)
        # (code,id,user_code,user_name,contenu,user_avatar,page)
        # LISTE.append((code, id, rep.get("user_code"),rep.get("user_name"),rep.get("contenu"),rep.get("user_avatar"), page))
        QUEUE_sql_inser_msg.put((code, id, rep.get("user_name")))
        if rep.get("user_name"):
            QUEUE_sql_inser_user.put((rep.get("user_name"),rep.get("user_avatar")))
        QUEUE_sql_inser_msg2.put((id, rep.get("contenu")))
        LISTE+=1
        # QUEUE_sql_inser_msg.put((code, id, textcompress(etree.tostring(post, method='html')), page))
    print('ADDING', LISTE)
    return LISTE
    # DB.addmsgs(LISTE)
    #print("END ADDING")
    # return DB.updatelen(code)


def linkcreator(link, n=1, inc=True):
    print("link modifier:", link, n, inc)
    if inc:number = str(int(re.findall(paging, link)[0])+n)
    else:  number = n
    link = re.sub(paging2, '\\1-%d\\2'%number, link, 1)
    return link


session=requests.Session()
def requestor(code, page=False, done=False, resp=False, url=False):
    #print("requette")
    if url == False:
        #print("url False")
        link = DB.code2link(code)
        if not link:return
        #print("link:", link)
        if done == False:
            done, resp = DB.code2done(code)
        if page == False:
            page = done//20+1
        #print("1LINK:", link, page, done, resp)
        link = linkcreator(link, page, False)
    link=link.replace("https://", "http://")
    while True:
        liste = link.split("-")
        liste[3] = str(page)
        link = "-".join(liste)
        TIME = time.time()
        response = False
        #print("before try")
        while not response:
            # try:
                # response = session.get(link,headers=headers).text
            #print("REQUETTE PROXIED")
            with TIME_LOCK:
                if TIME-time.time()>0:
                    time.sleep(TIME-time.time())
                TIME=time.time()+1/3
            #print('\033[91m'+"AFTER SLEEP"+'\033[0m')
            #print("=>",link)
            response = session.get(link,headers=headers,verify=False).text
            # except Exception as e:
            #     print("<<<",e)
                # requestor(code)
        if '<img class="img-erreur" src="/img/erreurs/e410.png" alt="ERREUR 410">\n' in response:
            if "<strong>Ce topic a été supprimé par son auteur.</strong>" in response:
                print("DELETED author")
                return DB.deleted(code, "author")
            if "<strong>Ce topic a été supprimé suite à une action de modération.</strong>" in response:
                print("DELETED modo")
                return DB.deleted(code, "modo")
        #print("TIME1:", time.time()-TIME)
        scrape(response, link, code, page)
        #print("TIME2:", time.time()-TIME)
        # done, resp = scrape(response, link, code, page)
        if not '<i class="icon-forward">' in response:
            # DB.updatelen(code)
            return
            # requestor(code, page+1, done, resp)
        page += 1


def feedupdate():
    global END_RUNNING
    DB2 = Database()
    #with ThreadPoolExecutor(max_workers=MAX_threads) as executor:
    while not END_RUNNING:
	    try:
	        feed = feedparser.parse('https://www.jeuxvideo.com/rss/forums/51.xml')
	        feedmodifier(feed, DB2)
	    except KeyboardInterrupt:
	        print("ENDING2")
	        END_RUNNING = True
	        exit()
	    except Exception as e:
	        print("!!!",e)
	    time.sleep(1)



threading.Thread(target=feedupdate).start()

def bite(code):
    print("CODE:", code)
    requestor(code)
    DB.updatelen(code)

YUP=[]

def update(executor):
        while True:
            print("yup")
            for i in DB.sql_get2do():
                if i in YUP:continue
                executor.submit(bite,i)
                YUP.append(i)
                if len(YUP)>500:
                    YUP.pop(0)
            time.sleep(5)

def worker():
    while not END_RUNNING:
        print("worker")
        code=works.get()
        requestor(code)

print("BEFORE FETCH")
all=DB.execute("SELECT `name`, `noel_link` FROM `noel2flo`",fetchall=True)
print("AFTER FETCH")
for i in all:
    KNOWN[i[0]]=i[1]
    images_download.put(i)

print("ouff")
threads=[]
for _ in range(MAX_threads):
	threads.append(threading.Thread(target=worker))
	threads[-1].start()

for thread in threads:
	thread.join()

threading_image.join()
threading_msg2.join()
threading_msg1.join()
threading_user.join()
#try:
#    with ThreadPoolExecutor(max_workers=MAX_threads) as executor:
#        while not END_RUNNING:
#            update(executor)
#except KeyboardInterrupt:
#    print("ENDING1")
#    END_RUNNING = True

#PUTE.join()