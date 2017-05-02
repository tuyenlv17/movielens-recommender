# -*- coding: utf-8 -*-
import csv
import time
import os
import json
import MySQLdb
import random
import math
import sys
from math import sqrt


dbHost = 'localhost'
dbUser = 'moviesdev'
dbPass = '123456a@'
dbName = 'moviescreed'

basePath = '/home/tuyenlv/common-data/learning/computer-science/data-mining/recommendation-system/datasets/movielens/ml-latest-small';
modalPath = 'modal/modal-small.json'
ratingPath = 'ratings.csv'
maxMovies = 10000


def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 50, fill = '█'):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + ' ' * (length - filledLength)
    sys.stdout.write('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix))
    sys.stdout.flush()
    if iteration == total: 
        print()

def loadJson(path=modalPath):
	return json.loads(open(path).read())

def exportJson(data,path=modalPath):
	str = json.dumps(data)
	f = open(modalPath,'w')
	print >> f,str

# Returns a distance-based similarity score for person1 and person2
def sim_distance(prefs,person1,person2):
	# Get the list of shared_items
	si={}
	for item in prefs[person1]:
		if item in prefs[person2]:
			si[item]=1
			# if they have no ratings in common, return 0
	if len(si)==0: return 0
			# Add up the squares of all the differences
	sum_of_squares=sum([pow(prefs[person1][item]-prefs[person2][item],2) for item in si])
	return 1/(1+sum_of_squares)

def sim_pearson(prefs,p1,p2):
	# Get the list of mutually rated items
	si={}
	for item in prefs[p1]:
		if item in prefs[p2]: si[item]=1
	# Find the number of elements
	n=len(si)
	# if they are no ratings in common, return 0
	if n==0: return 0
	# Add up all the preferences
	sum1=sum([prefs[p1][it] for it in si])
	sum2=sum([prefs[p2][it] for it in si])
	# Sum up the squares
	sum1Sq=sum([pow(prefs[p1][it],2) for it in si])
	sum2Sq=sum([pow(prefs[p2][it],2) for it in si])
	# Sum up the products
	pSum=sum([prefs[p1][it]*prefs[p2][it] for it in si])
	# Calculate Pearson score
	num=pSum-(sum1*sum2/n)
	den=sqrt((sum1Sq-pow(sum1,2)/n)*(sum2Sq-pow(sum2,2)/n))
	if den==0: return 0
	r=num/den
	return r

# Returns the best matches for person from the prefs dictionary.
# Number of results and similarity function are optional params.
def topMatches(prefs,person,n=10,similarity=sim_pearson):
	scores=[(similarity(prefs,person,other),other) for other in prefs if other!=person]
	# Sort the list so the highest scores appear at the top
	scores.sort( )
	scores.reverse( )
	return scores[0:n]

# Gets recommendations for a person by using a weighted average
# of every other user's rankings
def getRecommendations(prefs,person,similarity=sim_pearson):
	totals={}
	simSums={}
	for other in prefs:
		# don't compare me to myself
		if other==person: continue
		sim=similarity(prefs,person,other)
		# ignore scores of zero or lower
		if sim<=0: continue
		for item in prefs[other]:
			# only score movies I haven't seen yet
			if item not in prefs[person] or prefs[person][item]==0:
				# Similarity * Score
				totals.setdefault(item,0)
				totals[item]+=prefs[other][item]*sim
				# Sum of similarities
				simSums.setdefault(item,0)
				simSums[item]+=sim
	# Create the normalized list
	rankings=[(total/simSums[item],item) for item,total in totals.items( )]
	# Return the sorted list
	rankings.sort( )
	rankings.reverse( )
	return rankings


def transformPrefs(prefs):
	result={}
	for person in prefs:
		for item in prefs[person]:
			result.setdefault(item,{})
			# Flip item and person
			result[item][person]=prefs[person][item]
	return result

def calculateSimilarItems(ratings,n=10,similarity=sim_distance):
	# Create a dictionary of items showing which other items they
	# are most similar to.
	topSimItems = {};
	# Invert the preference matrix to be item-centric
	ratings=transformPrefs(ratings)
	# print ratings
	c=0
	for itemId, itemRatings in ratings.items():
	# Status updates for large datasets
		c+=1
		printProgressBar(c, len(ratings), prefix = 'Trainning:', suffix = 'Complete', length = 50)

		# Find the most similar items to this one
		curTopSim=topMatches(ratings,itemId,n=n,similarity=sim_distance)
		topSimItems[itemId] = {}
		for item in curTopSim:
			topSimItems[itemId][item[1]] = item[0];
	return topSimItems


def getRecommendedItems(userRatings, topSimItems):
	scores={}
	totalSim={}
	# Loop over items rated by this user
	for (item,rating) in userRatings.items( ):
		# Loop over items similar to this one
		for (item2, similarity) in topSimItems[item].items():
			# Ignore if this user has already rated this item
			if item2 in userRatings: continue
			# Weighted sum of rating times similarity
			scores.setdefault(item2,0)
			scores[item2]+=similarity*rating
			# Sum of all the similarities
			totalSim.setdefault(item2,0)
			totalSim[item2]+=similarity
	# Divide each total score by total weighting to get an average
	ct = 0
	rankings = []
	for item,score in scores.items():
		ct+=1
		if totalSim[item] == 0:
			print (" " + item + " \n")
		else: 
			rankings.append((score/totalSim[item],item))
	#rankings=[(score/totalSim[item],item) for item,score in scores.items( )]
	# Return the rankings from highest to lowest
	rankings.sort( )
	rankings.reverse( )
	return rankings

def loadRatingsFromFile(path=basePath+"/"+ratingPath):
	ratings={}
	trainRatings={}
	testRatings={}
	with open(path) as f:
	    reader = csv.DictReader(f, delimiter=',')
	    for row in reader:
	    	userId = str(row['userId'])
	    	movieId = str(row['movieId'])
	    	rating = float(row['rating'])
	    	ratings.setdefault(userId,{})
	    	ratings[userId][movieId]=rating
	for user in ratings:
		totalTest = len(ratings[user]) / 10;
		testRatings[user] = dict([(movie,rating) for movie, rating in random.sample(ratings[user].items(),totalTest)])
		trainRatings[user] = dict([(movie,rating) for movie, rating in ratings[user].items() if movie not in testRatings[user].keys()])
	return ratings, testRatings, trainRatings

def loadRatings(userId=-1):	
	db = MySQLdb.connect(dbHost,dbUser,dbPass, dbName)
	cursor = db.cursor()
	ratings={}
	try:
		sql = "SELECT user_id, movie_id, rating FROM ratings";
		if userId != -1: sql += " WHERE user_id = %d" % userId
		cursor.execute(sql)
		# print sql
		resultSet = cursor.fetchall()		
		for row in resultSet:
			userId = str(row[0])
			movieId = str(row[1])
			rating = float(row[2])
			ratings.setdefault(userId,{})
			ratings[userId][movieId]=rating
	except Exception as e:
		print (e)
		db.rollback()
	return ratings

def saveTopSimItems(topSimItems):
	db = MySQLdb.connect(dbHost,dbUser,dbPass, dbName)
	cursor = db.cursor()
	try:
		cursor.execute('TRUNCATE sim_movies')
		for itemId,curTopSim in	topSimItems.items():
			for simId,coefficient in curTopSim.items():
				sql = "INSERT INTO sim_movies(movie_id, sim_id,coefficient) VALUES(%s,%s,%f)" % (itemId,simId,coefficient)
				# print sql
				cursor.execute(sql)
		db.commit()
	except Exception as e:
		print (e)
		db.rollback()

def saveUserRecommendation(ratings, topSimItems):
	db = MySQLdb.connect(dbHost,dbUser,dbPass, dbName)
	cursor = db.cursor()
	try:		
		for userId,userRating in ratings.items():
			cursor.execute("DELETE FROM recommendations WHERE user_id = %s" % (userId))			
			recommendedItems = getRecommendedItems(userRating, topSimItems)
			# print "DELETE FROM recommendations WHERE user_id = %s" % (userId)
			for item in recommendedItems:
				sql = "INSERT INTO recommendations(user_id,movie_id,rating) VALUES(%s,%s,%f)" % (userId,item[1],item[0])
				# print sql
				cursor.execute(sql)
		db.commit()
	except Exception as e:
		print (e)
		db.rollback()

def prepareUserRecommendations(userId=-1):
	topSimItems = loadJson(modalPath)
	ratings = loadRatings(userId)
	saveUserRecommendation(ratings, topSimItems)

def predict(userRatings, predictMovie, topSimItems):
	simSum = 0
	total = 0
	for ratedMovie in userRatings.keys():
		# print "%s %s %f" % (ratedMovie, predictMovie, userRatings[ratedMovie])
		topSimItems[ratedMovie].setdefault(predictMovie, 0);
		total += topSimItems[ratedMovie][predictMovie] * userRatings[ratedMovie]
		simSum += topSimItems[ratedMovie][predictMovie] 
	if simSum == 0: return -1
	return total / simSum

#90% for train, 10% for test
def validate():
	print ("prepare 90% dataset for trainning...")
	ratings, testRatings, trainRatings = loadRatingsFromFile()
	# maxMovies = 500
	topSimItems = calculateSimilarItems(trainRatings, maxMovies, sim_distance)
	totalTest = 0
	s = 0	
	sSquare = 0
	totalZero = 0
	print ("\nevaluating...")
	for user in testRatings.keys():
		for movie in testRatings[user].keys():
			# print "userid %s, movie %s" % (user, movie)
			predictedRating = predict(trainRatings[user], movie, topSimItems)
			if predictedRating < 0:
				totalZero += 1
				predictedRating = 2.5
			realRating = testRatings[user][movie]
			totalTest += 1
			s += (realRating - predictedRating)
			sSquare += (realRating - predictedRating) * (realRating - predictedRating)
			if predictedRating > 5:
				print ("%s %s %f %f" % (user, movie, realRating, predictedRating))			
	print ("Total test %d, Total sim sum zero %d" % (totalTest, totalZero))
	print ("MAE  %f" % (s / totalTest))
	print ("RMSE %f" % (math.sqrt(sSquare / totalTest)))

def calRecommendations():
	# print "Loading rating from file..."	
	
	ratings, testRatings, trainRatings = loadRatingsFromFile()
	# print "Calculating modal..."
	# # topSimItems = loadJson(modalPath)		
	topSimItems = calculateSimilarItems(ratings, 50)
	# os.rename(modalPath, modalPath + "-" + str(time.time()))
	# exportJson(topSimItems,modalPath)
	# print "Save recommendations items to db..."
	# saveUserRecommendation(ratings, topSimItems)
	# print "Save similar items to db..."
	# saveTopSimItems(topSimItems)



validate()