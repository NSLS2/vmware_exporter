'''
Helpers for writing efficient twisted code, optimized for coroutine scheduling efficiency
'''
# autopep8'd

from twisted.internet import defer
from twisted.python import failure


class BranchingDeferred(defer.Deferred):

    '''
    This is meant for code where you are doing something like this:

    content = yield self.get_connection_content()
    results = yield defer.DeferredList([
        self.get_hosts(content),
        self.get_datastores(content),
    ])

    This allows get_hosts and get_datastores to run in parallel, which is good.
    But what if you don't want the whole of get_hosts to wait for
    get_connection_content() to be complete?

    We have a bunch of places where it would be better for scheduling if we did this:

    content = self.get_connection_content()
    results = yield defer.DeferredList([
        self.get_hosts(content),
        self.get_datastores(content),
    ])

    Now we don't have to wait for content to be finished before get_hosts etc
    starts running. It is up to get_hosts to block on the content deferred itself.

    (Thats a contrived example, the real win is allowing host_labels and
    vm_inventory to run in parallel).

    Unfortunately you can't have parallel branches blocking on the same deferred
    like this with a standard Twisted deferred.

    This is a deferred that enables the parallel branching use case.
    '''

    def __init__(self):
        super().__init__()
        self._branches = []
        self._completed = False
        self.result = None

    def _branch(self):
        if not self._completed:
            branch = defer.Deferred()
            self._branches.append(branch)
            return branch

        if isinstance(self.result, failure.Failure):
            return defer.fail(self.result)

        return defer.succeed(self.result)

    def callback(self, result):
        if self._completed:
            raise defer.AlreadyCalledError
        self.called = True
        self._completed = True
        self.result = result
        while self._branches:
            self._branches.pop(0).callback(result)

    def errback(self, err=None):
        if self._completed:
            raise defer.AlreadyCalledError
        if err is None:
            err = failure.Failure()
        elif not isinstance(err, failure.Failure):
            err = failure.Failure(err)
        self.called = True
        self._completed = True
        self.result = err
        while self._branches:
            self._branches.pop(0).errback(err)

    def addCallbacks(self, *args, **kwargs):
        return self._branch().addCallbacks(*args, **kwargs)

    def addCallback(self, *args, **kwargs):
        return self._branch().addCallback(*args, **kwargs)

    def addErrback(self, *args, **kwargs):
        return self._branch().addErrback(*args, **kwargs)

    def addBoth(self, *args, **kwargs):
        return self._branch().addBoth(*args, **kwargs)


class run_once_property(object):

    '''
    This is a property descriptor that caches the first result it retrieves. It
    does this by setting keys in self.__dict__ on the parent class instance.
    This is fast - python won't even bother running our descriptor next time
    because attributes in self.__dict__ on a class instance trump descriptors
    on the class.

    This is intended to be used with the Collector class which has a request
    bound lifecycle (this isn't going to cache stuff forever and cause memory
    leaks).
    '''

    def __init__(self, callable):
        self.callable = callable

    def __get__(self, obj, cls):
        if obj is None:
            return self
        result = obj.__dict__[self.callable.__name__] = BranchingDeferred()
        self.callable(obj).chainDeferred(result)
        return result


@defer.inlineCallbacks
def parallelize(*args):
    results = yield defer.DeferredList(
        args,
        fireOnOneErrback=True,
        consumeErrors=True,
    )
    return tuple(r[1] for r in results)
