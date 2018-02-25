import tweepy
import json
import MySQLdb 
from dateutil import parser
import time

#These are the data that you want to search for
WORDS = ['#bigdata', '#AI', '#datascience', '#machinelearning', '#ml', '#iot']
 
#Twitter's Key
CONSUMER_KEY = "nSZa52HCHJ6yLo2EgJl9vy8zv"
CONSUMER_SECRET = "cCBdwy9PFGkNFXzVOSHRAtQU1XyxogijGDmxxRa2zoahHiE4QR"
ACCESS_TOKEN = "819888788671315968-mqCa5ZMrM8WwU8nzHTi9cvFyoqf0RSt"
ACCESS_TOKEN_SECRET = "AURrn1MBNiFQDZ1PV4TacalR6GwS0Jg5WTIPYfbrmwH9H"

#connection credientials to MySQL 
HOST = "localhost"
USER = "root"
PASSWD = "bunny007"
DATABASE = "TESTDB"
 
# This function takes the 'retweet_count', 'text', 'screen_name'  and stores it
# into a MySQL database
def store_data(retweet_count, text, screen_name):
    #conection to MySQL
    db=MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DATABASE, charset="utf8")
    cursor = db.cursor()
    #To know whether the particula twitter screenname is present in my database if yes return his id or null
    cursor.execute("select id from twitterusers where screen_name=%s",(screen_name,))
    x=cursor.fetchall()
    ids=[]
    for y in x:
	ids.append(y[0])
    #to check whether SCreen_name is present or noy
    if len(ids)==0:
	name=(str(screen_name),)
	#As it is not there! I am inserting into the database 
	cursor.execute('''Insert into twitterusers (screen_name) values(%s)''',(str(screen_name),))
	db.commit()	
	#Getting its id in the databse and storing it in id
	cursor.execute("select id from twitterusers where screen_name=%s",(screen_name,))
        wow=cursor.fetchall()
	for x in wow:
		ids.append(x[0])
	#print ids,"ids"
    #Inserting into twitterdata as previous id as a foreign key
    insert_query = "INSERT INTO twitterdata (retweet_count, screen_id, text) VALUES (%s, %s, %s)"     
    cursor.execute(insert_query, (int(retweet_count), int(ids[0]), str(text.encode('utf-8')) ))
    db.commit()
    cursor.close()
    db.close()
    return
 
class StreamListener(tweepy.StreamListener):    
    #This is a class provided by tweepy to access the Twitter Streaming API. 
 
    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
 
    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: asdasd' + repr(status_code))
        return False
 
    def on_data(self, data):
        #This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
           # Decode the JSON from Twitter
            datajson = json.loads(data)
            
            #grab the wanted data from the Tweet
	    
            text = datajson['text']
            screen_name = datajson['user']['screen_name']
 	    retweet_count=datajson['retweet_count']
            #print out a message to the screen that we have collected a tweet
            print("Tweet collected " )
            
            #insert the data into the MySQL database
            store_data(retweet_count, text, screen_name)
        
        except Exception as e:
           print(e)
 
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
#Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True)) 
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
streamer.filter(track=WORDS)
















