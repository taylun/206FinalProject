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
import collections
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
	if "Twitter_"+search_term in CACHE_DICTION:
		print("using cached data for", search_term)
	else:
		print("fetching new web data for", search_term)
		
		search_results= api.search(q= search_term)
		CACHE_DICTION["Twitter_"+search_term]= search_results
		f = open(CACHE_FNAME,'w') 
		f.write(json.dumps(CACHE_DICTION)) 
		f.close()

	return CACHE_DICTION["Twitter_"+search_term]

#print(json.dumps(get_tweets("The Notebook")))

#Create a function ger_user_info that makes a request to Twitter using Tweepy. This function will accept a string that represents a twitter handle for a specific user. The Tweepy request should return and cache the data pertaining to this user or access previously cached data.  


def get_user_info(user_id, twitter_handle):
	if twitter_handle in CACHE_DICTION:
		print("using cached data for", twitter_handle)
	else:
		print("fetching new web data for", twitter_handle)
		
		search_results= api.get_user(user_id= user_id, screen_name= twitter_handle)
		
		CACHE_DICTION[twitter_handle]= search_results
		f = open(CACHE_FNAME,'w') 
		f.write(json.dumps(CACHE_DICTION)) 
		f.close()

	return CACHE_DICTION[twitter_handle]








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
	def get_top_actor(self):
		s= self.actors.split(",")
		return s[0]			



#print(Movie(get_movie_data("Titanic")))




#Create a connection to your database
conn= sqlite3.connect('finalproject.db')
cur= conn.cursor()

#Create a Users table within your database. This table should have columns for data user_id (primary key), screen_name, number of faves, and number of followers

cur.execute('DROP TABLE IF EXISTS Users')
table_spec2= "CREATE TABLE IF NOT EXISTS "

table_spec2 += "Users (user_id TEXT PRIMARY KEY, screen_name TEXT, num_favs INTEGER, num_followers INTEGER, location TEXT)"
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


#Write code to insert data pertaining to the movies Titanic, The Notebook, and Goodfellas into the Movies table in your database. 

s= "INSERT INTO Movies VALUES (null, ?, ?, ?, ?, ?, ?)"
Titanic= Movie(get_movie_data("Titanic"))
t_data= [(Titanic.title, Titanic.director, Titanic.num_languages, Titanic.IMDB_rating, Titanic.get_top_actor(), Titanic.plot)]

for item in t_data:
	#print(item)
	cur.execute(s, item)

The_Notebook= Movie(get_movie_data("The Notebook"))
#print(The_Notebook.get_top_actor())

tn_data= [(The_Notebook.title, The_Notebook.director, The_Notebook.num_languages, The_Notebook.IMDB_rating, The_Notebook.get_top_actor(), The_Notebook.plot)]


for item in tn_data:
	cur.execute(s, item)


Goodfellas= Movie(get_movie_data("Goodfellas"))

g_data= [(Goodfellas.title, Goodfellas.director, Goodfellas.num_languages, Goodfellas.IMDB_rating, Goodfellas.get_top_actor(), Goodfellas.plot)]

for item in g_data:
	cur.execute(s, item)


#Write code to insert data into the Tweets table in your database pertaining to the tweets about your 3 movies.
query1= "SELECT title FROM Movies"
cur.execute(query1)
movie_titles= [str(x)[2:-3] for x in cur.fetchall()]

movie_twitter_data= []
for title in movie_titles:
	movie_twitter_data.append(get_tweets(title))


statement= "INSERT INTO Tweets VALUES (?,?, ?, ?, ?, ?)"

tweet_particulars= []
for m_dict in movie_twitter_data:
	movie_title= m_dict["search_metadata"]["query"]
	for tweet in m_dict["statuses"]: 
		tweet_particulars.append((tweet["id_str"], tweet["text"],tweet["user"]["id_str"], movie_title, tweet["retweet_count"], tweet["favorite_count"]))		
for tup in tweet_particulars:
	cur.execute(statement, tup)

#Write code to insert data into the Users table. 

statement2= "INSERT OR IGNORE INTO Users VALUES (?, ?, ?, ?, ?)"
users_dict= []

for m_dict in movie_twitter_data:
	for tweet in m_dict["statuses"]:
		user_info= get_user_info(tweet["user"]["id_str"], tweet["user"]["screen_name"])
		users_dict.append(user_info)
			

user_particulars= []		
for user in users_dict:
	user_particulars.append((user["id_str"], user["screen_name"], user["favourites_count"],user["followers_count"], user["location"]))

for t in user_particulars:
	cur.execute(statement2, t)

user_mention_particulars= []
mentioned_users_dict= []



for m_dict in movie_twitter_data:
	for tweet in m_dict["statuses"]:
		for lst in tweet["entities"]["user_mentions"]:
			foo= get_user_info(lst["id_str"], lst["screen_name"])
			mentioned_users_dict.append(foo)


for user in mentioned_users_dict:
	user_mention_particulars.append((user_info["id_str"], user_info["screen_name"], user_info["favourites_count"], user_info["followers_count"], user_info["description"]))
			

for t in user_mention_particulars:
	cur.execute(statement2, t)

conn.commit()


#Write code to find the most common word in the Tweets about each movie. 

query2= "SELECT text from Tweets INNER JOIN Movies ON Movies.title= Tweets.movie_title WHERE Movies.title== 'Titanic'"
cur.execute(query2)
Titanic_tweets= cur.fetchall()


tweets_not_tups= [x for (x,) in Titanic_tweets]

words= []
words2= []
for item in tweets_not_tups:
	words.append(item.split())
	for item in words:
		for word in item:
			words2.append(word)
	


c= collections.Counter(words2)
Titanic_most_common_words= c.most_common(5)



query3= "SELECT text from Tweets WHERE movie_title= 'The+Notebook'"
cur.execute(query3)
Notebook_tweets= cur.fetchall()


tweets_not_tups_2= [x for (x,) in Notebook_tweets]


wordz= []
wordz2= []
for item in tweets_not_tups_2:
	wordz.append(item.split())
	for item in wordz:
		for word in item:
			wordz2.append(word)
	


c= collections.Counter(wordz2)
Notebook_most_common_word= c.most_common(5)

#print(Notebook_most_common_word)


query4= "SELECT text from Tweets INNER JOIN Movies ON Movies.title= Tweets.movie_title WHERE Movies.title== 'Goodfellas'"
cur.execute(query4)
Goodfellas_tweets= cur.fetchall()


tweets_not_tups_3= [x for (x,) in Goodfellas_tweets]

w= []
w2= []
for item in tweets_not_tups_3:
	w.append(item.split())
	for item in w:
		for word in item:
			w2.append(word)

c= collections.Counter(w2)
Goodfellas_most_common_word= c.most_common(5)


#Write code to find the top 3 locations (of Twitter users who tweeted) of tweets per movie.
query5= "SELECT location FROM Users INNER JOIN Tweets ON Users.user_id= Tweets.user_id WHERE Tweets.movie_title== 'Titanic'"
cur.execute(query5)
Titanic_locations= cur.fetchall()
t_loc_list= [x for (x,) in Titanic_locations]


Titanic_location_dict= {}

for item in t_loc_list:
	if len(item)>= 5:
		try:
			if item in Titanic_location_dict:
				Titanic_location_dict[item]+= 1
			else:
				Titanic_location_dict[item]= 1	
		except:
			pass

t_dict_sort= sorted(Titanic_location_dict, key= lambda x: Titanic_location_dict[x])	


		
query6= "SELECT location FROM Users INNER JOIN Tweets ON Users.user_id= Tweets.user_id WHERE Tweets.movie_title== 'The+Notebook'"
cur.execute(query6)
Notebook_locations= cur.fetchall()
n_loc_list= [x for (x,) in Notebook_locations]


Notebook_location_dict= {}

for item in n_loc_list:
	if len(item)>= 5:
		try:
			if item in Notebook_location_dict:
				Notebook_location_dict[item]+= 1
			else:
				Notebook_location_dict[item]= 1	
		except:
			pass

n_dict_sort= sorted(Notebook_location_dict, key= lambda x: Notebook_location_dict[x])	


query7= "SELECT location FROM Users INNER JOIN Tweets ON Users.user_id= Tweets.user_id WHERE Tweets.movie_title== 'Goodfellas'"
cur.execute(query7)
Goodfellas_locations= cur.fetchall()
g_loc_list= [x for (x,) in Goodfellas_locations]


Goodfellas_location_dict= {}

for item in g_loc_list:
	if len(item)>= 5:
		try:
			if item in Goodfellas_location_dict:
				Goodfellas_location_dict[item]+= 1
			else:
				Goodfellas_location_dict[item]= 1	
		except:
			pass

g_dict_sort= sorted(Goodfellas_location_dict, key= lambda x: Goodfellas_location_dict[x])
#print(g_dict_sort)	

#Write a text file of Summary Statistics about the three movies and their corresponding Tweets/Twitter Users:

filename= "Lundeen_FinalProject_SummaryStats"
o= open(filename, "w")
o.write("Taylor Lundeen's SI 206 Final Project- Summary Statistics:")
o.write("\n")
o.write("\n")
o.write("The three movies this program focuses on are: Titanic, The Notebook, and Goodfellas")
o.write("\n")
o.write("\n")
t= Movie(get_movie_data("Titanic"))
o.write(t.__str__())
o.write("\n")
o.write("\n")
tn= Movie(get_movie_data("The Notebook"))
o.write(tn.__str__())
g= Movie(get_movie_data("Goodfellas"))
o.write("\n")
o.write("\n")
o.write(g.__str__())
o.write("\n")
o.write("\n")
o.write("The most common word found in tweets about Titanic were: " + str([x for (x,y) in Titanic_most_common_words]))
o.write("\n")
o.write("\n")
o.write("The most common word found in tweets about The Notebook were: " + str([x for (x,y) in Notebook_most_common_word]))
o.write("\n")
o.write("\n")
o.write("The most common word found in tweets about Goodfellas were: " + str([x for (x,y) in Goodfellas_most_common_word]))
o.write("\n")
o.write("\n")
o.write("Tweets about Titanic were made by users from: " + str([x for x in t_dict_sort]))
o.write("\n")
o.write("\n")
o.write("Tweets about The Notebook were made by users from: " + str([x for x in n_dict_sort]))
o.write("\n")
o.write("\n")
o.write("Tweets about Goodfellas were made by users from: " + str([x for x in g_dict_sort]))
o.write("\n")
o.write("\n")
o.write("That is all folks! Thanks for a great semester in SI 206!")

o.close()


# Put your tests here, with any edits you now need from when you turned them in with your project plan.


# Remember to invoke your tests so they will run! (Recommend using the verbosity=2 argument.)














conn.close()


# Write your test cases here.

class CachingTests(unittest.TestCase):
	def test_Titanic_caching(self):
		cache= open("SI206_finalproject_cache.json", "r")
		c= cache.read()
		self.assertTrue("Titanic" in c)
		cache.close()
		
	def test_Goodfellas_caching(self):
		cache= open("SI206_finalproject_cache.json", "r")
		c= cache.read()
		self.assertTrue("Goodfellas" in c)
		cache.close()	
	def test_TheNotebook_caching(self):
		cache= open("SI206_finalproject_cache.json", "r")
		c= cache.read()
		self.assertTrue("The Notebook" in c)
		cache.close()	
		


class Get_Tweets_test(unittest.TestCase):
	def test_response_type(self):
		test= get_tweets("umich")
		self.assertEqual(type(test), type({}))
	
			
class get_user_info_test(unittest.TestCase):
	def test_user_response_type(self):
		t= get_user_info("2725444583", "dylanspray5")
		self.assertEqual(type(t), type({}))

class get_movie_data_test(unittest.TestCase):
	def test_movie_response_type(self):
		m= get_movie_data("Titanic")
		self.assertEqual(type(m), type({}))

class Database_Tests(unittest.TestCase):
	def test_num_movies(self):
		conn = sqlite3.connect('finalproject.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Movies');
		result = cur.fetchall()
		self.assertTrue(len(result)== 3)
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
