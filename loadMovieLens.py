import datetime
import csv
import MySQLdb

basePath = '/home/tuyenlv/data/learning/computer-science/data-mining/recommendation-system/datasets/movielens/ml-latest-small';
movies = {}
genres = {}
movies_genres = {}
tags = {}
rattings = {}
taggings = {}
links = {}

dbHost = 'localhost'
dbUser = 'moviesdev'
dbPass = '123456a@'
dbName = 'moviescreed'
db = MySQLdb.connect(dbHost,dbUser,dbPass, dbName)
cursor = db.cursor()

def loadMovies():	
	# content = "";
	with open(basePath + '/movies.csv') as f:
	    reader = csv.DictReader(f, delimiter=',')
	    for row in reader:
	    	movieId = int(row['movieId'])
	    	# content = content + row['movieId'] + "\n";
	        movies.setdefault(movieId, {})
	        movies[movieId]['title'] = row['title'].replace("'", "\\'")
	        sql = "INSERT INTO movies(id,title,poster) VALUES(%d,'%s','%s')" % (movieId,movies[movieId]['title'],movies[movieId]['poster'])	    	
	    	cursor.execute(sql)
	        if '(no genres listed)' in row['genres']:continue

	        genres_arr = row['genres'].split('|')
	        for genre in genres_arr:
	        	if genre not in genres:	        		
	        		genres[genre] = len(genres) + 1
	        		sql = "INSERT INTO genres(id,name) VALUES(%d,'%s')" % (genres[genre], genre)
	        		cursor.execute(sql)
	        	sql = "INSERT INTO movies_genres(movie_id,genre_id) VALUES(%d,%d)" % (movieId,genres[genre])
	    		cursor.execute(sql)	    	
	
	# f1=	open('id.txt', 'w');
	# print >> f1, content
	# f1.close()

def loadTags():
	with open(basePath + '/genome-tags.csv') as f:
	    reader = csv.DictReader(f, delimiter=',')
	    for row in reader:
	    	tags[row['tag']] = int(row['tagId'])
	    	sql = "INSERT INTO tags(id,name) VALUES(%d,'%s')" % (int(row['tagId']), row['tag'].replace("'", "\\'"))
	    	cursor.execute(sql)

def loadLinks():
	with open(basePath + '/links.csv') as f:
	    reader = csv.DictReader(f, delimiter=',')
	    for row in reader:
	    	movieId = int(row['movieId'])
	        movies.setdefault(movieId, {})
	    	movies[movieId]['poster'] = row['imdbId'] + ".jpg"

def loadRatings(): 
	with open(basePath + '/ratings.csv') as f:
		reader = csv.DictReader(f, delimiter=',')
		with open(basePath + '/ratings-editted.csv', 'a') as outcsv:		    
		    writer = csv.writer(outcsv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n') 
		    for row in reader:
		    	userId = int(row['userId'])
		        # rattings.setdefault(userId, {})
		        # rattings[userId]['movieId'] = int(row['movieId'])
		        # rattings[userId]['rating'] = float(row['rating'])
		        # rattings[userId]['time'] = int(row['timestamp'])
		        time = datetime.datetime.fromtimestamp(int(row['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
		        writer.writerow([userId, int(row['movieId']), float(row['rating']), time])
	

def loadTaggings(): 
	with open(basePath + '/tags.csv') as f:
		reader = csv.DictReader(f, delimiter=',')
		with open(basePath + '/taggings-editted.csv', 'a') as outcsv:		    
		    writer = csv.writer(outcsv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n') 
		    for row in reader:		    	
		    	# if row['tag'] not in tags: continue
		    	# print row['tag'] + " " + str(row['tag'] not in tags)
		    	userId = int(row['userId'])
		        # taggings.setdefault(userId, {})
		        # taggings[userId]['movieId'] = int(row['movieId'])
		        # taggings[userId]['tagId'] = tags[row['tag']]	        
		        # taggings[userId]['time'] = int(row['timestamp'])
		        time = datetime.datetime.fromtimestamp(int(row['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
		        writer.writerow([userId, int(row['movieId']), row['tag'], time])


def init():	
	try:
		print "Load links"
		loadLinks()
		print "Load movies"
		loadMovies()
		# print "Load tags"
		# loadTags()
		print "Commit change"
		db.commit()
	except Exception as e:
		print str(e)
		print "rollback"
		db.rollback()		
	print "Load ratings"
	loadRatings()
	print "Load taggings"
	loadTaggings()

