import tweepy
import json
import MySQLdb 
from dateutil import parser
import time
import pandas as pd
#To Remove the utf-8 error's while parsing twitter data
import sys
reload(sys)
sys.setdefaultencoding('utf8')

#Connection to MySQL
HOST = "localhost"
USER = "root"
PASSWD = "bunny007"
DATABASE = "TESTDB"

#Printclass is to print a list to the respective name.csv
def printclass(data,name):
    prin=[]
    #Converting into List
    for y in data:
        temp=[]
        for z in y:
            temp.append(z)
        prin.append(temp)
    
    df =  pd.DataFrame(prin)
    df.to_csv(str(name)+'.csv', sep=',', header=None, index=None)

#These are the same classes of the api2.py ,can be implemented by import but in confusin whether can we do it,so implemented it again and called print function at end of the each respective function
class filtersearch:
    page=0
    db=MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DATABASE, charset="utf8")
    cursor = db.cursor()
    
    def __init__(self, page):
        self.page = page*10-10
    def printallusers(self):
	print ("select * from twitterdata  LIMIT 10 OFFSET "+str(self.page))
	self.cursor.execute("select * from twitterdata  LIMIT 10 OFFSET "+str(self.page));
	x=self.cursor.fetchall()
	printclass(x,"printallusers")
    def daterange(self,frm,to):
	print (frm,to)
	self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.created_at<='"+to+"' and twitterdata.created_at>='"+frm+"'"+" LIMIT 10 OFFSET "+str(self.page))
	x=self.cursor.fetchall()
	for y in x:
	   print (y)
	printclass(x,"daterange")
    
    def retweetcount(self,cond,va,to=0):
	x=''
	if cond=='<':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count<"+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	if cond=='>':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count>"+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	if cond=='=':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count="+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	if cond=='<=':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count<="+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	if cond=='>=':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count>="+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	if cond=='btw':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count>="+str(va)+" and twitterdata.retweet_count<="+str(to)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	printclass(x,"retweetcount")
    
    def searchtext(self,cond,val):
	x=''
	if cond=="sw":
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.text like '"+val+"%'"+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	if cond=="ew":
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.text like '%"+val+"'"+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	if cond=="has":
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.text like '%"+val+"%'"+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	printclass(x,"searchtext")
    
    def sorttime(self):
	self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id order by twitterdata.created_at desc"+" LIMIT 10 OFFSET "+str(self.page))
        x=self.cursor.fetchall()
        for y in x:
            print (y)
	printclass(x,"sorttime")
    
	 	
#Creating a filtersearch class with page index=2 
new=filtersearch(2)
#Calling one by one function respectively
new.printallusers()	
new.daterange("2018-02-24 20:07:18","2018-02-25 00:04:57")
new.retweetcount('<',0)
new.retweetcount('>',0)
new.retweetcount('<=',0)
new.retweetcount('>=',0)
new.retweetcount('btw',0,1)
new.searchtext('sw',"AI")
new.searchtext('ew',"AI")
new.searchtext('has',"AI")
new.sorttime()

