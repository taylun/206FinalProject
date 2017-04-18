###### INSTRUCTIONS ###### 

# An outline for preparing your final project assignment is in this file.

# Below, throughout this file, you should put comments that explain exactly what you should do for each step of your project. You should specify variable names and processes to use. For example, "Use dictionary accumulation with the list you just created to create a dictionary called tag_counts, where the keys represent tags on flickr photos and the values represent frequency of times those tags occur in the list."

# You can use second person ("You should...") or first person ("I will...") or whatever is comfortable for you, as long as you are clear about what should be done.

# Some parts of the code should already be filled in when you turn this in:
# - At least 1 function which gets and caches data from 1 of your data sources, and an invocation of each of those functions to show that they work 
# - Tests at the end of your file that accord with those instructions (will test that you completed those instructions correctly!)
# - Code that creates a database file and tables as your project plan explains, such that your program can be run over and over again without error and without duplicate rows in your tables.
# - At least enough code to load data into 1 of your dtabase tables (this should accord with your instructions/tests)

######### END INSTRUCTIONS #########

# Put all import statements you need here.
import requests
import unittest
import tweepy 
import twitter_info
import json
import sqlite3
import itertools
# Begin filling in instructions....

# Tweepy Setup Code- You should create authentication variable names that will hold data necessary to make a request to Tweepy,  and link them to the authentication information in the imported twitter_info document.
consumer_key = twitter_info.consumer_key
consumer_secret = twitter_info.consumer_secret
access_token = twitter_info.access_token
access_token_secret = twitter_info.access_token_secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# You should set up library to grab stuff from twitter with your authentication, and return it in a JSON format 
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

#You should create a Cache file that will hold your requests data and set up a pattern to open this cache file and load its contents into a cache dictionary or create a cache dictionary if one doesnt already exist.
CACHE_FNAME= "SI206_finalproject_cache.json"

try:
	cache_file = open(CACHE_FNAME,'r')
	cache_contents = cache_file.read()
	cache_file.close()
	CACHE_DICTION = json.loads(cache_contents)
except:
	CACHE_DICTION = {}


# Create a function get_tweets that makes a request to Twitter using Tweepy. This function will accept a search term and will cache the data that is returned by Tweepy in regards to the tweets about that term or access previously cached data. 	

def get_tweets(search_term):
	if search_term in CACHE_DICTION:
		print("using cached data for", search_term)
	else:
		print("fetching new web data for", search_term)
		
		search_results= api.search(q= search_term)
		CACHE_DICTION[search_term]= search_results
		f = open(CACHE_FNAME,'w') 
		f.write(json.dumps(CACHE_DICTION)) 
		f.close()

	return CACHE_DICTION[search_term]

#Create a function ger_user_info that makes a request to Twitter using Tweepy. This function will accept a string that represents a twitter handle for a specific user. The Tweepy request should return and cache the data pertaining to this user or access previously cached data.  

def get_user_info(twitter_handle):
	if twitter_handle in CACHE_DICTION:
		print("using cached data for", twitter_handle)
	else:
		print("fetching new web data for", twitter_handle)
		
		search_results= api.get_user(screen_name= twitter_handle)
		
		CACHE_DICTION[twitter_handle]= search_results
		f = open(CACHE_FNAME,'w') 
		f.write(json.dumps(CACHE_DICTION)) 
		f.close()

	return CACHE_DICTION[twitter_handle]
#print(get_user_info("Titanic"))


#Create a function get_movie_data that accepts a string representing a movie title. The term will be used to make a request to the OMDB API for data pertaining to that movie. This function should cache the data returned from the request or access previously cached data from a prior request of the same movie.   
def get_movie_data(movie_title):
	if movie_title in CACHE_DICTION:
		print("using cached data for", movie_title)
	
	else:
		print("fetching new web data for", movie_title)
		
		baseurl= "http://www.omdbapi.com/?"
		params_d= {}
		params_d["t"]= movie_title
		params_d["type"]= "movie"

		r= requests.get(baseurl, params_d)
		movie= json.loads(r.text)
		CACHE_DICTION[movie_title]= movie

		
		f = open(CACHE_FNAME,'w') 
		f.write(json.dumps(CACHE_DICTION)) 
		f.close()
	return CACHE_DICTION[movie_title]


#Define a class, Movie. The class constructor should take a response dictionary from an OMDB request and use this to establish instance variables, title, director, IMDB rating, actors, number of languages and plot. 
#This class should have a string method that access the instance variables to create a one line summary of the details of the movie.

class Movie(object):
	def __init__(self, movie_dict):
		self.title= movie_dict["Title"]
		self.director= movie_dict["Director"]
		self.IMDB_rating= movie_dict["Ratings"][0]["Value"]
		self.actors= movie_dict["Actors"]
		self.num_languages= len(movie_dict["Language"].split())
		self.plot= movie_dict["Plot"]
	def __str__(self):
		return "The movie {}, was directed by {} and stars {}.".format(self.title, self.director, self.actors)


#print(Movie(get_user_info("Titanic")))




#Create a connection to your database

conn= sqlite3.connect('finalproject.db')
cur= conn.cursor()

#Create a Users table within your database. This table should have columns for data user_id (primary key), screen_name, number of faves, and number of followers

cur.execute('DROP TABLE IF EXISTS Users')
table_spec2= "CREATE TABLE IF NOT EXISTS "

table_spec2 += "Users (user_id TEXT PRIMARY KEY, screen_name TEXT, num_favs INTEGER, num_followers INTEGER)"
cur.execute(table_spec2)

#Create a Movies table in your database. This table should have columns for data concerning an id (your primary key), movie title, movie director, number languages, imdb rating, top actor, and plot

cur.execute('DROP TABLE IF EXISTS Movies')
table_spec3= "CREATE TABLE IF NOT EXISTS "

table_spec3+= "Movies (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, director TEXT, num_languages INTEGER, imdb_rating TEXT, top_actor TEXT, plot TEXT)"
cur.execute(table_spec3)

#Create a Tweets table in your database. This table should have columns for data concerning tweet text, user id(a reference to the Users table) , movie title (a reference to the Movies table), retweets, favorites,
cur.execute('DROP TABLE IF EXISTS Tweets')
table_spec = 'CREATE TABLE IF NOT EXISTS '

table_spec += 'Tweets (tweet_id TEXT PRIMARY KEY, '
table_spec += 'text TEXT, user_id TEXT, movie_title TEXT, retweets INTEGER, favorites INTEGER, FOREIGN KEY(user_id) REFERENCES Users(user_id), FOREIGN KEY(movie_title) REFERENCES Movies(title))'
cur.execute(table_spec)


#Write code to insert data pertaining to the movie Titanic into the Movies table in  your database. 

s= "INSERT INTO Movies VALUES (null, ?, ?, ?, ?, ?, ?)"
Titanic= Movie(get_movie_data("Titanic"))
t_data= [(Titanic.title, Titanic.director, Titanic.num_languages, Titanic.IMDB_rating, Titanic.actors.split()[0], Titanic.plot)]

for item in t_data:
	#print(item)
	cur.execute(s, item)



conn.commit()

# Put your tests here, with any edits you now need from when you turned them in with your project plan.


# Remember to invoke your tests so they will run! (Recommend using the verbosity=2 argument.)

















# Write your test cases here.

class CachingTests(unittest.TestCase):
	def test_TheOutsiders_caching(self):
		cache= open("206finalproject_cache.json", "r").read()
		self.assertTrue("The Outsiders" in cache)
		


class Get_Movie_Tweets_test(unittest.TestCase):
	def test_response_type(self):
		test= get_tweets("umich")
		self.assertEqual(type(test), type({}))
	
			



class Database_Tests(unittest.TestCase):
	def test_num_movies(self):
		conn = sqlite3.connect('finalproject.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Movies');
		result = cur.fetchall()
		self.assertTrue(len(result)== 5)
		conn.close()	
	def test_num_tweets(self):
		conn = sqlite3.connect('finalproject.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Tweets');
		result = cur.fetchall()
		self.assertTrue(len(result)>=20)
		conn.close()	


class MovieClassTests(unittest.TestCase):
	def test_type_title(self):
		m= Movie(get_movie_data("Titanic"))
		self.assertEqual(type(m.title), type("hi"))
	def test_type_director(self):
		m= Movie(get_movie_data("Titanic"))
		self.assertEqual(type(m.director), type("hi"))
	def test_type_IMDB_rating(self):
		m= Movie(get_movie_data("Titanic"))
		self.assertEqual(type(m.IMDB_rating), type("hi"))
	def test_type_actors(self):
		m= Movie(get_movie_data("Titanic"))
		self.assertEqual(type(m.actors), type("hi"))

if __name__ == "__main__":
	unittest.main(verbosity=2)
