import collections
import os
import time

import pyspark

import dirwatcher
import queryparser
import wrapper

NORMAL_EXIT = "java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext"

class TestObj(object):
	def __init__(self, _id):
		self.id = _id


def main():
	testObj = TestObj(1)

	query1 = queryparser.SimpleQuery("1", {'agg': 'count', 'field': '*'}, {'text': {'_contains': 'happy'}})

	lines = wrapper.AggregateWrapper(testObj)

	print lines.id

	print "test"

if __name__ == "__main__":
    main()
