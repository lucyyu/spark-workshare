import inspect

LOG = 0

def log(s):
    if LOG == 1:
        print s

def make_hashkey(name, args, kwargs):
    hn = name
    ha = []
    for arg in args:
        ha.append(arg)
        """
        if hasattr(arg, "__call__"):
            c = [x.cell_contents for x in arg.__closure__] if arg.__closure__ else ()
            key = (arg.__code__.co_code, tuple(c))
            ha.append(key)
        else:
            ha.append(arg)
            """
    ha = tuple(ha)
    hk = frozenset(kwargs.items())
    return (hn, ha, hk)


class Wrapper(object):

    def __init__(self, wrapped, deferred=None):
        """
        Wraps an object for the purpose of lazy evaluation and optimization. To
        create a wrapper:

            x = Wrapper(real_x)
            y = x.map(foo, bar)

        Here, y is another Wrapper object that causes evaluation of real_x.map
        to be deferred. To force evaluation:

            real_y = y.__eval__()

        Which results in real_y = real_x.map(foo, bar).

        It's a little unclear what to do about attribute accesses like x.baz,
        since they're not really used in Spark. If this ever comes up, Wrapper
        will print a warning so we can figure out what the right thing to do is.

        ---

        The deferred argument (internal use only!) represents the deferred
        method call used to transform one wrapper into the next. For instance,
        the above y would be created as follows:

            y = Wrapper(x, deferred=('map', [foo, bar], {}))

        deferred is a tuple of (name, args, kwargs). Only Wrapper objects that
        were created by calling a function on another Wrapper object have
        deferred set. User-created Wrappers do not.

        Here, x is the parent Wrapper and y is its child. By recursively
        following ._wrapper, we can trace the lineage of y to x to real_x (its
        ancestors).
        """
        self._wrapped = wrapped
        self._deferred = deferred

    def __getattr__(self, name):
        """
        Intercepts accesses of nonexistent attributes.

        Accessing real attributes skips this method:

            self._wrapped
            self._deferred
            self.__eval__()

        But we can use this to synthesize "fake" attributes:

            self.map()
            ...

        """
        # http://stackoverflow.com/a/2704528
        if name == "__getnewargs__":
            # http://stackoverflow.com/a/6416080
            raise ValueError("You're trying to pickle a Wrapper. Don't do that!")

        # This logic is probably buggy
        attr = getattr(self._wrapped, name)
        if hasattr(attr, "__call__"):
            return self.__getcall__(name)
        else:
            print("WARNING: raw attribute access")
            return attr

    def __getcall__(self, name):
        """
        Helper function for constructing deferred functions for __getattr__.
        When __getattr__('map') is called, it must return a *function* that
        returns the child Wrapper.

        Note: we use self.__class__ to ensure that this still works when Wrapper
        is subclassed. Children will have the same class as their parent, so
        CachingWrappers will produce more CachingWrappers, etc.
        """
        def fn(*args, **kwargs):
            deferred = (name, args, kwargs)
            return self.__class__(self, deferred)
        return fn

    def __eval__(self):
        """
        Recursively force evaluation of this wrapper and its ancestors,
        returning the real object with all method calls applied to it.
        """
        if not self._deferred:
            # no deferred action, just pass through the object
            return self._wrapped
        else:
            # evaluate all ancestors of this object
            parent = self._wrapped.__eval__()
            # then apply the deferred action
            name, args, kwargs = self._deferred
            return getattr(parent, name)(*args, **kwargs)

class CachingWrapper(Wrapper):

    def __init__(self, *args, **kwargs):
        """
        Wraps an object, and caches the result of __eval__() so that the
        computation isn't duplicated. It's possible that this optimization is
        something that Spark already gives us, though.
        """
        super(CachingWrapper, self).__init__(*args, **kwargs)

        # The cached result of __eval__()
        self._cached = None

        # False until the result of __eval__() has been computed and cached,
        # then True.
        self._cache_present = False

    def __eval__(self):
        if not self._cache_present:
            self._cached = super(CachingWrapper, self).__eval__()
            self._cache_present = True
        return self._cached

class CommonSubqueryWrapper(CachingWrapper):

    def __init__(self, *args, **kwargs):
        """
        Wraps an object and collapses the lineage of duplicate computations by
        making wrappers singletons with respect to the deferred actions.

            x = CommonSubqueryWrapper(real_x)
            y = x.filter(f)
            z = x.filter(f)

        Here, y and z will refer to the same Wrapper object. In conjunction with
        __eval__() caching, above, this will eliminate unnecessary recomputation
        of the filter.

        To determine whether or not two actions are identical, we use Python's
        built-in hashing. Arguments must be hashable types. Lambdas and
        functions are hashable, but note that changing variable names can change
        the hash.
        """
        super(CommonSubqueryWrapper, self).__init__(*args, **kwargs)

        # A dictionary of hashkey => child, where hashkey is a hashable version
        # of deferred (see below) and child is the resulting
        # CommonSubqueryWrapper.
        self._call_cache = {}

    def __getcall__(self, name):
        def fn(*args, **kwargs):
            # Like deferred, hashkey represents a method call performed on the
            # parent object. Unlike deferred, hashkey is hashable.
            deferred = (name, args, kwargs)
            hashkey = make_hashkey(name, args, kwargs)
            if hashkey not in self._call_cache:
                self._call_cache[hashkey] = self.__class__(self, deferred)
            else:
                print "hashkey already in self._call_cache!"
            return self._call_cache[hashkey]
        return fn

class ScanSharingWrapper(CommonSubqueryWrapper):

    def __init__(self, *args, **kwargs):
        """
        Wraps an object and performs scan sharing on a limited set of queries,
        e.g. map. The wrapper records all deferred map actions called on it. At
        evaluation time, the first child performs a mega-map consisting of the
        union of all these map actions. Then each child runs their individual
        map on this result, which should be an efficiency gain.
        """
        super(ScanSharingWrapper, self).__init__(*args, **kwargs)

        # For each optimized action, a list of arguments to be computed, e.g.
        # self._tasks["map"] is a list of map actions to run.
        self._tasks = {
            "map": [],
            "filter": [],
            "aggregate": [],
            "aggregateByKey": []
        }

        # For each optimized action, the "megaresult" produced by running the
        # megaquery with the actions in the list above.
        self._results = {}

    def __getcall__(self, name):
        fn = super(ScanSharingWrapper, self).__getcall__(name)
        def ffn(*args, **kwargs):
            if name in self._tasks:
                if name == "aggregate" or name == "aggregateByKey":
                    if len(args) != 3:
                        raise ValueError("%s only takes three arguments" % name)
                    v = args
                else: #map and filter
                    if len(args) != 1:
                        raise ValueError("%s only takes one argument" % name)
                    v = args[0]
                if len(kwargs) != 0:
                    raise ValueError("%s does not take keyword arguments" % name)
                self._tasks[name].append(v)
            return fn(*args, **kwargs)
        return ffn

    def __eval__(self):
        log("\n>>> - - ENTERING EVAL. self._deferred = %s - - >>> \n" % (self._deferred,))
        if self._cache_present:
            log("+ Cache present.")
            pass
        elif not self._deferred:
            log("+ No cache present. Root reached.")
            self._cached = self._wrapped
            self._cached.cache()
        else:
            name, args, kwargs = self._deferred
            log("+ No cache present. Evaluating _wrapped:")
            parent = self._wrapped.__eval__()

            # Bind to a local variable to prevent Spark from trying to pickle self.
            tasks = self._wrapped._tasks.get(name)

            if name == "filter":
                log("+ Computing megaresult for FILTER. tasks: %s" % (tasks,))

                megaresult = self.__getmegaresult__(name, parent, tasks)
                log("+ Megaresult: %s" % megaresult)

                index = self._wrapped._tasks[name].index(args[0])
                log("+ Index: %s" % index)

                self._cached = megaresult[index]

            elif name == "oldfilter":
                log("+ Computing megaresult for OLDFILTER. tasks: %s" % (tasks,))

                megaresult = self.__oldgetmegaresult__("filter", parent, lambda item: any(task(item) for task in tasks))
                log("+ Megaresult: %s" % megaresult)

                self._cached = megaresult.filter(*args, **kwargs)

            elif name == "map":
                log("+ Computing megaresult for MAP. tasks: %s" % (tasks,))

                megaresult = self.__getmegaresult__(name, parent, tasks)
                log("+ Megaresult: %s" % megaresult)

                index = self._wrapped._tasks[name].index(args[0])
                log("+ Index: %s" % index)

                self._cached = megaresult[index] 

            elif name == "aggregate":
                log("+ Computing megaresult for AGGREGATE. tasks: %s" % (tasks,))

                megaresult = self.__getmegaresult__(name, parent, tasks)
                log("+ Megaresult: %s" % megaresult)

                index = self._wrapped._tasks[name].index(args)
                log("+ Index: %s" % index)

                self._cached = megaresult[index]

            elif name == "aggregateByKey":
                log("+ Computing megaresult for AGGREGATEBYKEY. tasks: %s" % (tasks,))

                megaresult = self.__getmegaresult__(name, parent, tasks)
                log("+ Megaresult: %s" % megaresult)

                index = self._wrapped._tasks[name].index(args)
                log("+ Index: %s" % index)

                thisResult = megaresult.map(lambda x: (x[0], x[1][index]))
                self._cached = thisResult

            else:
                self._cached = getattr(parent, name)(*args, **kwargs)
        
        log("\n<<< - - EXITING EVAL - - <<< \n")
        self._cache_present = True
        return self._cached

    def __oldgetmegaresult__(self, name, parent, megaquery):
        if name not in self._wrapped._results:
            log("\tMegaresult not yet computed:")
            self._wrapped._results[name]=getattr(parent,name)(megaquery)
        else:
            pass
        return self._wrapped._results[name]

    def __getmegaresult__(self, name, parent, tasks):
        if name not in self._wrapped._results:
            log("\tMegaresult not yet computed:")

            if name == "filter":
                self.__computeMultiFilter__(parent, tasks)
            elif name == "map":
                self.__computeMultiMap__(parent, tasks)
            elif name == "aggregate":
                self.__computeMultiAggregate__(parent, tasks)    
            elif name == "aggregateByKey":
                self.__computeMultiAggregateByKey__(parent, tasks)                
        else:
            if name == "aggregateByKey":
                if len(self._wrapped._results[name].first()[1]) == len(tasks):
                    log("\tMegaresult already previously computed. name: %s" % name)
                    pass
                else:
                    log("\tMegaresult must be recomputed due to addition of another task:")
                    self.__computeMultiAggregateByKey__(parent, tasks)                                        
            elif len(self._wrapped._results[name]) == len(tasks):
                log("\tMegaresult already previously computed. name: %s" % name)
                pass
            else:
                log("\tMegaresult must be recomputed due to addition of another task:")
                if name == "filter":
                    self.__computeMultiFilter__(parent, tasks)
                elif name == "map":
                    self.__computeMultiMap__(parent, tasks)
                elif name == "aggregate":
                    self.__computeMultiAggregate__(parent, tasks)  
        return self._wrapped._results[name]

    def __computeMultiFilter__(self, parent, tasks):
        log("\tComputing multiFilter: %s" % (tasks,))
        self._wrapped._results["filter"] = parent.multiFilter(tasks)

    def __computeMultiMap__(self, parent, tasks):
        log("\tComputing multiMap: %s" % (tasks,))
        self._wrapped._results["map"] = parent.multiMap(tasks)

    def __computeMultiAggregate__(self, parent, tasks):
        log("\tComputing multiAggregate: %s" % (tasks,))
        zeroValues = [v[0] for v in tasks]
        def seqOp(accumulatedVal, newVal):
            result = [None] * len(tasks)
            for i in xrange(len(tasks)):
                result[i] = tasks[i][1](accumulatedVal[i], newVal)
            return result
        def combOp(result1, result2):
            result = [None] * len(tasks)
            for i in xrange(len(tasks)):
                result[i] = tasks[i][2](result1[i], result2[i])
            return result
        self._wrapped._results["aggregate"] = parent.aggregate(zeroValues, seqOp, combOp)  

    def __computeMultiAggregateByKey__(self, parent, tasks):
        log("\tComputing multiAggregateByKey: %s" % (tasks,))
        zeroValues = [v[0] for v in tasks]
        def seqOp(accumulatedVal, newVal):
            result = [None] * len(tasks)
            for i in xrange(len(tasks)):
                result[i] = tasks[i][1](accumulatedVal[i], newVal)
            return result
        def combOp(result1, result2):
            result = [None] * len(tasks)
            for i in xrange(len(tasks)):
                result[i] = tasks[i][2](result1[i], result2[i])
            return result
        self._wrapped._results["aggregateByKey"] = parent.aggregateByKey(zeroValues, seqOp, combOp)        


class AggregateWrapper(ScanSharingWrapper):

    def __getcall__(self, name):
        if name == "reduce":
            fn = super(AggregateWrapper, self).__getcall__("aggregate")
            def ffn(*args, **kwargs):
                if len(args) != 1:
                    raise ValueError("%s takes one argument" % name)
                if len(kwargs) != 0:
                    raise ValueError("%s does not take keyword arguments" % name)
                f = args[0]
                def combOp(a, b):
                    if a is None and b is None:
                        return None
                    if a is None:
                        return b
                    if b is None:
                        return a
                    return f(a, b)
                def seqOp(acc, val):
                    if acc is None:
                        return val
                    return f(acc, val)
                return fn(None, seqOp, combOp)
            return ffn

        elif name == "reduceByKey":
            fn = super(AggregateWrapper, self).__getcall__("aggregateByKey")
            def ffn(*args, **kwargs):
                if len(args) != 1:
                    raise ValueError("%s takes one argument" % name)
                if len(kwargs) != 0:
                    raise ValueError("%s does not take keyword arguments" % name)
                f = args[0]
                def combOp(a, b):
                    if a is None and b is None:
                        return None
                    if a is None:
                        return b
                    if b is None:
                        return a
                    return f(a, b)
                def seqOp(acc, val):
                    if acc is None:
                        return val
                    return f(acc, val)
                return fn(None, seqOp, combOp)
            return ffn

        elif name == "fold":
            fn = super(AggregateWrapper, self).__getcall__("aggregate")
            def ffn(*args, **kwargs):
                if len(args) != 2:
                    raise ValueError("%s takes two arguments" % name)
                if len(kwargs) != 0:
                    raise ValueError("%s does not take keyword arguments" % name)
                zeroValue, op = args
                return fn(zeroValue, op, op)
            return ffn
        else:
            return super(AggregateWrapper, self).__getcall__(name)
