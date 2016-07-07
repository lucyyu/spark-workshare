import pyspark
import json
import time 

import wrapper

sc = pyspark.SparkContext(appName="FlexibleStreaming")
sc.setLogLevel("ERROR")




def defaultSparkWithoutCaching(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])

	transform_rdd = text_rdd.map(lambda x: len(x))

	start_time = time.time()
	
	job1 = transform_rdd.count()
	job2 = transform_rdd.reduce(max)
	job3 = transform_rdd.reduce(min)

	job4 = transform_rdd.count()
	job5 = transform_rdd.reduce(max)
	job6 = transform_rdd.reduce(min)
	job7 = transform_rdd.reduce(min)

	print "%s tweets" % job1
	print "%s max length" % job2
	print "%s min length" % job3

	print "%s tweets" % job4
	print "%s max length" % job5
	print "%s min length" % job6
	print "%s min length" % job7

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)

def defaultSparkWithCaching(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])

	transform_rdd = text_rdd.map(lambda x: len(x))
	transform_rdd.cache()

	start_time = time.time()
	
	job1 = transform_rdd.count()
	job2 = transform_rdd.reduce(max)
	job3 = transform_rdd.reduce(min)

	job4 = transform_rdd.count()
	job5 = transform_rdd.reduce(max)
	job6 = transform_rdd.reduce(min)
	job7 = transform_rdd.reduce(min)

	print "%s tweets" % job1
	print "%s max length" % job2
	print "%s min length" % job3

	print "%s tweets" % job4
	print "%s max length" % job5
	print "%s min length" % job6
	print "%s min length" % job7

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)

def wrapped(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])

	transform_rdd = text_rdd.map(lambda x: len(x))

	wrapped = wrapper.AggregateWrapper(transform_rdd)
	start_time = time.time()

	job1 = wrapped.aggregate(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job2 = wrapped.aggregate(0, lambda x,y: max(x,y), lambda x,y: max(x,y))
	job3 = wrapped.aggregate(float('inf'), lambda x,y: min(x,y), lambda x,y: min(x,y))

	job4 = wrapped.aggregate(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job5 = wrapped.aggregate(0, lambda x,y: max(x,y), lambda x,y: max(x,y))
	job6 = wrapped.aggregate(float('inf'), lambda x,y: min(x,y), lambda x,y: min(x,y))
	job7 = wrapped.aggregate(float('inf'), lambda x,y: min(x,y), lambda x,y: min(x,y))

	print "%s tweets" % job1.__eval__()
	print "%s max" % job2.__eval__()
	print "%s min" % job3.__eval__()

	print "%s tweets" % job4.__eval__()
	print "%s max length" % job5.__eval__()
	print "%s min length" % job6.__eval__()
	print "%s min length" % job7.__eval__()

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)

def defaultSparkWithoutCachingAggregateByKey(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])

	transform_rdd = text_rdd.map(lambda x: (len(x)%6, len(x)))

	start_time = time.time()
	
	job1 = transform_rdd.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job2 = transform_rdd.reduceByKey(max)
	job3 = transform_rdd.reduceByKey(min)

	job4 = transform_rdd.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job5 = transform_rdd.reduceByKey(max)
	job6 = transform_rdd.reduceByKey(min)
	job7 = transform_rdd.reduceByKey(min)

	print "%s tweets" % job1.take(10)
	print "%s max" % job2.take(10)
	print "%s min" % job3.take(10)

	print "%s tweets" % job4.take(10)
	print "%s max" % job5.take(10)
	print "%s min" % job6.take(10)
	print "%s min" % job6.take(10)

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)

def defaultSparkWithCachingAggregateByKey(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])

	transform_rdd = text_rdd.map(lambda x: (len(x)%6, len(x)))
	transform_rdd.cache()

	start_time = time.time()
	
	job1 = transform_rdd.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job2 = transform_rdd.reduceByKey(max)
	job3 = transform_rdd.reduceByKey(min)

	job4 = transform_rdd.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job5 = transform_rdd.reduceByKey(max)
	job6 = transform_rdd.reduceByKey(min)
	job7 = transform_rdd.reduceByKey(min)

	print "%s tweets" % job1.take(10)
	print "%s max" % job2.take(10)
	print "%s min" % job3.take(10)

	print "%s tweets" % job4.take(10)
	print "%s max" % job5.take(10)
	print "%s min" % job6.take(10)
	print "%s min" % job6.take(10)

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)

def wrappedAggregateByKey(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])

	transform_rdd = text_rdd.map(lambda x: (len(x)%6, len(x)))

	wrapped = wrapper.AggregateWrapper(transform_rdd)
	start_time = time.time()

	job1 = wrapped.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job2 = wrapped.aggregateByKey(0, lambda x,y: max(x,y), lambda x,y: max(x,y))
	job3 = wrapped.aggregateByKey(float('inf'), lambda x,y: min(x,y), lambda x,y: min(x,y))

	job4 = wrapped.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job5 = wrapped.aggregateByKey(0, lambda x,y: max(x,y), lambda x,y: max(x,y))
	job6 = wrapped.aggregateByKey(float('inf'), lambda x,y: min(x,y), lambda x,y: min(x,y))
	job7 = wrapped.aggregateByKey(float('inf'), lambda x,y: min(x,y), lambda x,y: min(x,y))

	print "%s tweets" % job1.__eval__().take(10)
	print "%s max" % job2.__eval__().take(10)
	print "%s min" % job3.__eval__().take(10)

	print "%s tweets" % job4.__eval__().take(10)
	print "%s max" % job5.__eval__().take(10)
	print "%s min" % job6.__eval__().take(10)
	print "%s min" % job7.__eval__().take(10)

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)

def filterScratch():
	name = "filter"

	def f1(x):
		a = 2
		return "hi" in x

	args = [f1]
	kwargs = {}
	print wrapper.make_hashkey(name, args, kwargs)

	name2 = "filter"

	def f2(x):
		a = 1
		return "look" in x
	args2 = [f2]
	kwargs2 = {}
	print wrapper.make_hashkey(name2, args2, kwargs2)

def complexWithoutCaching(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])

	transform_rdd = text_rdd.map(lambda x: (len(x)%6, len(x)))

	start_time = time.time()

	job1 = transform_rdd.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	def f1(x):
		return x > 3
	job2 = job1.filter(f1).aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job3 = transform_rdd.reduceByKey(max)
	job4 = job1.filter(f1).reduceByKey(min)

	print "%s tweets" % job1.take(10)
	print "%s tweets > 3 mod 6" % job2.take(10)
	print "%s max" % job3.take(10)
	print "%s min after filter" % job4.take(10)

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)

def complexWithCaching(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])

	transform_rdd = text_rdd.map(lambda x: (len(x)%6, len(x)))
	transform_rdd.cache()

	start_time = time.time()

	job1 = transform_rdd.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	def f1(x):
		return x > 3
	filtered = job1.filter(f1)
	filtered.cache()

	job2 = filtered.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)
	job3 = transform_rdd.reduceByKey(max)
	job4 = filtered.reduceByKey(min)

	print "%s tweets" % job1.take(10)
	print "%s tweets > 3 mod 6" % job2.take(10)
	print "%s max" % job3.take(10)
	print "%s min after filter" % job4.take(10)

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)

def complexWrapped(f):
	rdd = sc.textFile(f)

	json_rdd = rdd.map(lambda x: json.loads(x))
	text_rdd = json_rdd.filter(lambda json_keys: 'text' in json_keys).map(lambda x: x['text'])
	transform_rdd = text_rdd.map(lambda x: (len(x)%6, len(x)))
	wrapped = wrapper.AggregateWrapper(transform_rdd)

	start_time = time.time()

	job1 = wrapped.aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)

	def f1(x):
		return x > 3

	job2 = job1.filter(f1).aggregateByKey(0, lambda acc, _: acc + 1, lambda a, b: a + b)

	job3 = wrapped.reduceByKey(lambda x,y: max(x,y))

	job4 = job1.filter(f1).reduceByKey(lambda x,y: min(x,y))

	print "%s tweets" % job1.__eval__().take(10)
	print "%s tweets > 3 mod 6" % job2.__eval__().take(10)
	print "%s max" % job3.__eval__().take(10)
	print "%s min after filter" % job4.__eval__().take(10)

	end_time = time.time()
	print "--- %s seconds ---" % (end_time - start_time)


if __name__ == "__main__":
	practice = "/Users/lucy/flexible-spark-curr/data/tweets-1450352666530.txt"
	large = "/Users/lucy/flexible-spark-curr/largedata/large.txt" #287 MB
	largest = "/Users/lucy/flexible-spark-curr/largedata/largest.txt" #476 MB
	complexWithoutCaching(large)
	complexWithCaching(large)
	complexWrapped(large)