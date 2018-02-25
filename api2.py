import tweepy
import json
import MySQLdb 
from dateutil import parser
import time

#connection credientials to MySQL 
HOST = "localhost"
USER = "root"
PASSWD = "bunny007"
DATABASE = "TESTDB"
#Filter class for all respective filter outputs
class filtersearch:
    #Declared Page for Pagination
    page=0
    #connect to MySQL Database
    db=MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DATABASE, charset="utf8")
    cursor = db.cursor()
    
    def __init__(self, page):
	#Indexing my offset as page 
        self.page = page*10-10
    #Test function to print all the twitterdata along with pagination 
    def printallusers(self):
	self.cursor.execute("select * from twitterdata LIMIT 10 OFFSET "+str(self.page));
	x=self.cursor.fetchall()
	for y in x:
	    print (y[0])
	return x
    #Function to print all the data between the date range
    def daterange(self,frm,to):
	print (frm,to)
	self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.created_at<='"+to+"' and twitterdata.created_at>='"+frm+"'"+" LIMIT 10 OFFSET "+str(self.page))
	x=self.cursor.fetchall()
	for y in x:
	   print (y)
    #Function to print the data based on retweet_count and query	
    def retweetcount(self,cond,va,to=0):
	x=''
	#For query Less than some value
	if cond=='<':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count<"+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	#For query Greater than some value		
	if cond=='>':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count>"+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	#For query Equal to some value
	if cond=='=':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count="+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	#For query Less than equal to some value	
	if cond=='<=':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count<="+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	#For query Greater than equal to some value
	if cond=='>=':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count>="+str(va)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	#For query for IN betwen  some value's
	if cond=='btw':
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.retweet_count>="+str(va)+" and twitterdata.retweet_count<="+str(to)+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	return x
   #Funtion to search for a text Respectively
    def searchtext(self,cond,val):
	x=''
	#For Seaching a twitterdata which startswith some value
	if cond=="sw":
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.text like '"+val+"%'"+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	#For Seaching a twitterdata which endswith some value
	if cond=="ew":
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.text like '%"+val+"'"+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	#For Seaching a twitterdata has the value
	if cond=="has":
	    self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id where twitterdata.text like '%"+val+"%'"+" LIMIT 10 OFFSET "+str(self.page))
 	    x=self.cursor.fetchall()
	    for y in x:
	        print (y)
	return x
    #Function to get twitter data based on  Time 
    def sorttime(self):
	self.cursor.execute("select * from twitterdata join twitterusers on twitterdata.screen_id=twitterusers.id order by twitterdata.created_at desc"+" LIMIT 10 OFFSET "+str(self.page))
        x=self.cursor.fetchall()
        for y in x:
            print (y)

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

