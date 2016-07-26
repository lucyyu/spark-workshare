import inspect
import pyspark
import wrapper

# test wrapper calculates correct basic reduce(max) and reduce(min) 
def testReduceCorrectness():
    rdd = sc.parallelize([1,2,3,4,5,6,7])
    x = wrapper.AggregateWrapper(rdd)
    y = x.reduce(max)
    z = x.reduce(min)

    result = y.__eval__()
    print "result of y.__eval__(): ", result, "\n"

    result2 = z.__eval__()
    print "result of z.__eval__(): ", result2, "\n"

    print "parent's _tasks: ", x._tasks, "\n"
    print "parent's _results: ", x._results, "\n"

# test that wrapper calculates correct basic aggregate
def testBasicAggregateCorrectness():
    rdd = sc.parallelize([1,2,3,4,5,6,7])
    x = wrapper.AggregateWrapper(rdd)

    a1 = x.aggregate(0, lambda x,y:x+y, lambda x,y:x+y)
    a2 = x.aggregate(0, lambda x,y:max(x,y), lambda x,y: max(x,y))

    result = a1.__eval__()
    print "result of a1.__eval__(): ", result, "\n"

    result2 = a2.__eval__()
    print "result of a2.__eval__(): ", result2, "\n"

    print "parent's _tasks: ", x._tasks, "\n"
    print "parent's _results: ", x._results, "\n"    

    a3 = x.aggregate(10, lambda x,y:min(x,y), lambda x,y: min(x,y))

    result3 = a3.__eval__()
    print "result of a3.__eval__(): ", result3, "\n"

# test wrapper calculates correct basic aggregateByKey
def testAggregateByKeyCorrectness():
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])

    # the result the rdd computes
    result = rdd.aggregateByKey(0, lambda x,y:x+y, lambda x,y:x+y)
    print "\nresult of sum on rdd: ", result.take(10)

    result2 = rdd.aggregateByKey(0, max, max)
    print "\nresult of max on rdd: ", result2.take(10)

    print "\n - - - - - - - - - - \n"

    # the result the wrapper computes
    x = wrapper.AggregateWrapper(rdd)
    y = x.aggregateByKey(0, lambda x,y:x+y, lambda x,y:x+y)
    z = x.aggregateByKey(0, lambda x,y: max(x,y), lambda x,y: max(x,y))

    result = y.__eval__()
    print "eval result of sum on aggWrapper: ", result.take(10)

    result2 = z.__eval__()
    print "eval result of max on aggWrapper: ", result2.take(10)

# test wrapper calculates correct basic filter
def testBasicFilterCorrectness():
    rdd = sc.parallelize([1,2,3,4,5,6,7])
    x = wrapper.AggregateWrapper(rdd)

    y = x.filter(lambda x: True)
    print "y object (hopefully is RDD): ", y
    print "y._deferred: ", y._deferred
    result = y.__eval__()
    print "result of y.__eval__(): ", result.take(10)

def testBasicFilterCorrectness2():
    rdd = sc.parallelize([1,2,3,4,5,6,7])
    x = wrapper.AggregateWrapper(rdd)

    y = x.filter(lambda x: True)
    print "y object (hopefully is RDD): ", y
    result = y.take(10)
    print "result of y.take(10).__eval__(): ", result.__eval__()


# test wrapper calculates correct filter results when multiple filters applied
def testComplexFilterCorrectness():
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10,11,12,13,14])
    x = wrapper.AggregateWrapper(rdd)

    x_f1 = x.filter(lambda x: x % 2 == 1)
    x_f2 = x.filter(lambda x: x % 2 == 0)

    print "x._tasks: ", x._tasks

    result1 = x_f1.__eval__()
    print "x_f1._eval__(): ", result1.take(10)
    result2 = x_f2.__eval__()
    print "x_f2.__eval__(): ", result2.take(10)

    f1_f3 = x_f1.filter(lambda x: x % 3 == 0)
    print "x_f1._tasks: ", x_f1._tasks
    result3 = f1_f3.__eval__()
    print "f1_f3.__eval__(): ", result3.take(10)

    x_f4 = x.filter(lambda x: x > 10)
    print "x._tasks: ", x._tasks

    result4 = x_f4.__eval__()
    print "x_f4.__eval__(): ", result4.take(10)


# test my multiFilter(f) in rdd.py
def testMultiFilterCorrectness():
    rdd = sc.parallelize([1,2,3,4,5,6,7])
    def f1(x):
        return x%2==1
    def f2(x):
        return x%2==0
    fa = [f1, f2]
    z = rdd.multiFilter(fa)
    print z[0].take(10)
    print z[1].take(10)
    print z[0]
    print z[1]

# test function wrapping logic used in multiFilter([f]) and multiMap([f]) in rdd.py
def testFunctionCreationInLoop():
    def f1(x):
        return 2*x
    def f2(x):
        return 3*x
    def f3(x):
        return 4*x
    fa = [f1, f2, f3]
    print "fa: ", fa
    result = []
    for f in fa:
        print "this function:", f
        def func(p):
            def subfunc(x):
                return p(x)
            return subfunc
        print "func: ", func(f)
        result.append(func(f))

    print "result: ", result

    print result[0](3)
    print result[1](3)
    print result[2](3)

sc = pyspark.SparkContext(appName="FlexibleStreaming")
sc.setLogLevel("ERROR")

if __name__ == "__main__":
    testBasicFilterCorrectness2()
