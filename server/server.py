from zope.interface import Interface, implementer
from twisted.internet.interfaces import IProtocol
from twisted.web.iweb import IAgent, IResponse
from twisted.web.client import Agent, RedirectAgent, ResponseDone, readBody
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.resource import Resource, NoResource
from twisted.web.http_headers import Headers
from twisted.internet.defer import Deferred
from twisted.internet import reactor
from twisted.python import log
from twisted.python.failure import Failure
from twisted.python.components import proxyForInterface
from BeautifulSoup import BeautifulSoup
import time, sys, hashlib, json, re, os

FANART_URL = 'https://gist.githubusercontent.com/adammw/8662672/raw/fanart.json'
EPISODES_URL = 'http://tenplay.com.au/Handlers/GenericUserControlRenderer.ashx?path=~/UserControls/Browse/Episodes.ascx&props=ParentID,{%s}'
CACHE_EXPIRY = 60

log.startLogging(sys.stdout)

class ICacheStore(Interface):
    def cleanup(self):
        """
        Cleanup the cache by removing expired entries.

        All implementations may not support this method.
        """

    def clear(self):
        """
        Clear the entire cache. Be careful with this method since it could affect other processes if shared cache is being used.

        All implementations may not support this method.
        """

    def delete(self, key):
        """
        Deletes an entry in the cache. Returns true if an entry is deleted.
        """

    def read(self, key):
        """
        Fetches data from the cache, using the given key. If there is data in the cache with the given key, then that data is returned. Otherwise, None is returned.
        """

    def write(self, key, value):
        """
        Writes the value to the cache, with the key.
        """

@implementer(ICacheStore)
class MemoryStore(object):
    def __init__(self, expiry=None):
        self._defaultExpiry = expiry
        self.clear()

    def __contains__(self, key):
        if key in self._store:
            if key in self._expiryTimes and self._expiryTimes[key] < time.time():
                self.delete(key)
                return False
            else:
                return True
        else:
            return False

    def cleanup(self):
        for key in self._expiryTimes:
            if self._expiryTimes[key] < time.time():
                self.delete(key)

    def clear(self):
        self._store = dict()
        self._expiryTimes = dict()

    def read(self, key):
        log.msg('Cache read: %s' % key, system='MemoryStore')
        if key in self._expiryTimes and self._expiryTimes[key] < time.time():
            self.delete(key)
            return None
        else:
            return self._store.get(key)

    def write(self, key, value, expiry=None):
        log.msg('Cache write: %s' % key, system='MemoryStore')
        self._store[key] = value
        expiry = expiry or self._defaultExpiry
        if expiry:
            self._expiryTimes[key] = time.time() + expiry

    def delete(self, key):
        if 'key' in self._store:
            del self._store[key]
            del self._expiryTimes[key]
            return True
        return False

class CachingResponse(proxyForInterface(IResponse)):
    @staticmethod
    def cacheKey(method, uri, *args):
        h = hashlib.new('sha256')
        h.update(uri)
        keyParams = [method, h.hexdigest()]
        keyParams.extend(args)
        return '/'.join(keyParams)

    def __init__(self, response, cache):
        self.original = response
        self.cache = cache
        self.cache.write(self._cacheKey(), self) # TODO make this more generic, work with non-memory stores
        self._protocol = None

    def _cacheKey(self, *args):
        return CachingResponse.cacheKey(self.request.method, self.request.absoluteURI, *args)

    def deliverBody(self, protocol):
        def _returnCachedData(cachedBody):
            protocol.dataReceived(cachedBody)
            protocol.connectionLost(Failure(ResponseDone()))

        def _returnFailure(failure, self): # retry
            self._protocol = CachingProtocol(protocol, self.cache, cacheKey)
            self.original.deliverBody(self._protocol)

        cacheKey = self._cacheKey('body')
        cachedBody = self.cache.read(cacheKey)
        if cachedBody:
            reactor.callLater(0, _returnCachedData, cachedBody)
        else:
            if self._protocol:
                self._protocol.deferred.addCallback(_returnCachedData)
                self._protocol.deferred.addErrback(_returnFailure, self)
            else:
                self._protocol = CachingProtocol(protocol, self.cache, cacheKey)
                self.original.deliverBody(self._protocol)

class CachingProtocol(proxyForInterface(IProtocol)):
    def __init__(self, protocol, cache, cacheKey):
        self.original = protocol
        self.cache = cache
        self.cacheKey = cacheKey
        self.dataBuffer = []
        self.deferred = Deferred()

    def dataReceived(self, data):
        self.dataBuffer.append(data)
        self.original.dataReceived(data)

    def connectionLost(self, reason):
        if reason.check(ResponseDone):
            data = b''.join(self.dataBuffer)
            self.cache.write(self.cacheKey, data)
            self.deferred.callback(data)
        else:
            self.deferred.errback(reason)
        self.original.connectionLost(reason)

@implementer(IAgent)
class CachingAgent(object):
    def __init__(self, agent, cache):
        self._agent = agent
        self._cache = cache

    def request(self, method, uri, headers=None, bodyProducer=None):
        def _cacheResponse(response):
            return CachingResponse(response, self._cache)

        if method == 'GET':
            cacheKey = CachingResponse.cacheKey(method, uri)
            cachedResponse = self._cache.read(cacheKey)
            if cachedResponse:
                d = Deferred()
                reactor.callLater(0, d.callback, cachedResponse)
                return d
            else:
                d = self._agent.request(method, uri, headers, bodyProducer)
                d.addCallback(_cacheResponse)
        else:
            d = self._agent.request(method, uri, headers, bodyProducer)
        return d

cache = MemoryStore(expiry = CACHE_EXPIRY)
agent = CachingAgent(RedirectAgent(Agent(reactor)), cache)

class Fanart(Resource):
    isLeaf = True

    def _handleResponse(self, response, request):
        d = readBody(response)
        d.addCallback(self._handleResponseBody, response, request)
        d.addErrback(self._handleError, request)

    def _handleResponseBody(self, body, response, request):
        fanartData = json.loads(body)
        for id in fanartData:
            cache.write("fanart/%s" % id, str(fanartData[id]))
        cache.write("fanart-bootstrap-loaded", True)

        showId = request.args['id'][0]
        if showId in fanartData:
            request.setResponseCode(301)
            request.setHeader("location", str(fanartData[showId]))
            request.finish()
        else:
            channel = request.args['channel'][0]
            title = request.args['title'][0]
            self._handleNotInBootstrap(request, showId, channel, title)

    def _handleNotInBootstrapStage3(self, body, response, request, showId, channel, title):
        print "_handleNotInBootstrapStage3()"

        # try to determine correct show url
        episode_list_html = BeautifulSoup(body)
        urls = episode_list_html.findAll('a', {'href': True})
        for url in urls:
            url = url['href']
            if url.startswith('/' + channel):
                show_name = url.split('/')[2]
                break

        # find the fanart on the show url
        if len(channel) == 0 or len(show_name) == 0:
            return self._handleNotFound(request)
        else:
            self._handleNotInBootstrapStage4(request, showId, channel, show_name, episode_list_html)
            return None

    def _handleNotInBootstrapStage4(self, request, showId, channel, show_name, episode_list_html):
        print "_handleNotInBootstrapStage4(%s, %s)" %(repr(channel), repr(show_name))
        print 'http://tenplay.com.au/' + channel + '/' + show_name
        d = agent.request('GET', 'http://tenplay.com.au/' + channel + '/' + show_name, Headers({'User-Agent': ['Mozilla/5.0 fanart-fetcher/0.0.1']}), None)
        d.addCallback(self._handleNotInBootstrapStage5, request, showId, channel, show_name, episode_list_html)
        d.addErrback(self._handleNotInBootstrapStage5Error, request, showId, channel, show_name, episode_list_html)

    def _handleNotInBootstrapStage5(self, response, request, showId, channel, show_name, episode_list_html):
        print "_handleNotInBootstrapStage5()"
        d = readBody(response)
        d.addCallback(self._handleNotInBootstrapStage6, response, request, showId, channel, show_name, episode_list_html)
        d.addErrback(self._handleNotInBootstrapStage5Error, request, showId, channel, show_name, episode_list_html)

    def _handleNotInBootstrapStage6(self, body, response, request, showId, channel, show_name, episode_list_html):
        print "_handleNotInBootstrapStage6()"
        show_page_html = BeautifulSoup(body)
        images = show_page_html.find('div', 'marquee-container').findAll('div', 'marquee-image')
        def findImage(images):
            for image in images:
                url = image.find('div', {'data-src': True})['data-src']
                if url.startswith('//'):
                    url = 'http:' + url
                if url.startswith('http://iprx.ten.com.au/ImageHandler.ashx') or len(url) == 0:
                    continue
                return url.split('?')[0]
        url = findImage(images)
        if url:
            url = str(url)
            cache.write("fanart/%s" % showId, url)
            request.setResponseCode(301)
            request.setHeader("location", url)
            request.finish()
            return

        self._handleNotInBootstrapStage7(request, showId, channel, show_name, episode_list_html)

    def _handleNotInBootstrapStage7(self, request, showId, channel, show_name, episode_list_html):
        if episode_list_html:
            images = episode_list_html.findAll('img', {'src': True})
            for image in images:
                if len(image['src']) != 0:
                    url = str(image['src'])
                    print "Using fanart from episode list: %s" % url
                    cache.write("fanart/%s" % showId, url)
                    request.setResponseCode(302)
                    request.setHeader("location", url)
                    request.finish()

        self._handleNotFound(request)

    def _handleNotInBootstrapError(self, err, request, showId, channel, title):
        self._handleNotInBootstrapStage4(request, showId, channel, title, None)

    def _handleNotInBootstrapStage5Error(self, err, request, showId, channel, title, episode_list_html):
        print "_handleNotInBootstrapStage5Error()"
        log.err(err)
        self._handleNotInBootstrapStage7(request, showId, channel, title, episode_list_html)

    def _handleNotInBootstrapStage2(self, response, request, showId, channel, title):
        print "_handleNotInBootstrapStage2()"
        d = readBody(response)
        d.addCallback(self._handleNotInBootstrapStage3, response, request, showId, channel, title)
        d.addErrback(self._handleNotInBootstrapError, request, showId, channel, title)

    def _handleNotInBootstrap(self, request, showId, channel, title):
        print "_handleNotInBootstrap(%s, %s)" %(repr(channel), repr(title))
        # educated guesses
        channel = re.sub(r'[^a-z0-9A-Z-]+','', channel.lower().replace(' ','-'))
        show_name = re.sub(r'[^a-z0-9A-Z-]+','', title.lower().replace(' ','-'))

        d = agent.request('GET', EPISODES_URL % showId, Headers({'User-Agent': ['fanart-fetcher/0.0.1']}), None)
        d.addCallback(self._handleNotInBootstrapStage2, request, showId, channel, show_name)
        d.addErrback(self._handleNotInBootstrapError, request, showId, channel, show_name)

    def _handleNotFound(self, request):
        request.setResponseCode(404)
        request.setHeader("content-type", "text/plain")
        request.write("fanart not found")
        request.finish()

    def _handleError(self, err, request):
        log.err(err)
        request.setResponseCode(500)
        request.write(err)
        request.finish()

    def render_GET(self, request):
        cachedFanart = cache.read("fanart/%s" % request.args['id'][0])
        if cachedFanart:
            request.setResponseCode(301)
            request.setHeader("location", cachedFanart)
            return b''
        elif cache.read("fanart-bootstrap-loaded"):

            return NOT_DONE_YET
        else:
            d = agent.request('GET', FANART_URL, Headers({'User-Agent': ['fanart-fetcher/0.0.1']}), None)
            d.addCallback(self._handleResponse, request)
            d.addErrback(self._handleError, request)
            return NOT_DONE_YET

root = Resource()
root.putChild("fanart", Fanart())

factory = Site(root)
reactor.listenTCP(int(os.getenv('PORT', 8880)), factory)
reactor.run()
