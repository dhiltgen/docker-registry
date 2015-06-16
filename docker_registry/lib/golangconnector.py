# -*- coding: utf-8 -*-

import functools
import json
import logging
import time
from docker_registry.core import lru

logger = logging.getLogger(__name__)


def timeit(f):
    @functools.wraps(f)
    def wrapper(*args):
        start = time.time()
        content = f(*args)
        end = time.time()
        logger.info('Done in %s' % (end - start))
        return content
    return wrapper


class Connector(object):

    def __init__(self, driver):
        logger.debug('Entering gconnector')
        self.driver = driver
        self._imageshard = 'g2ci_4'
        self._layershard = 'g2cl_3'

    def _blobpath(self, digest):
        digest = digest.split(':')
        return '/registry-v2/docker/registry/v2/blobs/%s/%s/%s/data' % (
            digest[0], digest[1][:2], digest[1]
        )

    def _manifestbase(self, user, name):
        return '/registry-v2/docker/registry/v2/repositories/%s/%s/_manifests/tags' % (user, name)

    def _manifestpath(self, user, name, tag):
        return '%s/%s/current/link' % (self._manifestbase(user, name), tag)

    @timeit
    @lru.get
    def _lookup_layer(self, shard):
        logger.info('No go layer with that id in the cache: %s' % shard)
        # print(lru.redis_conn.get(shard))
        return None

    # Getting a golang image by digest
    # - look it up in the lru, return a raw manifest blob content if it's there
    # (one lru call)
    # - if it's not there, means we need to go over each layer so:
    #  * look up the digest on the golang blob store
    #  * process every layer and create lru entries {gopath: , legacy: } to lru
    #  (one s3 read, n+1 lru write)
    @timeit
    # @lru.get
    def image_by_digest(self, key):
        logger.info('Looking up image by digest %s' % key)
        digest = key.split('/')[1]
        storepath = self._blobpath(digest)
        # jcontent = self.driver.get_json(storepath)
        jcontent = json.loads(self.get_no_cache(storepath))
        stack = []
        for idx, legacy in enumerate(jcontent['history']):
            stack.append(json.loads(legacy['v1Compatibility'])['id'])

        for idx, goid in enumerate(jcontent['fsLayers']):
            logger.info('Registering layer %s' % goid['blobSum'])
            phypath = self._blobpath(goid['blobSum'])
            phycontent = json.loads(jcontent['history'][idx]['v1Compatibility'])
            # phycontent['id'] =
            tolru = {
                "gopath": phypath,
                "legacy": phycontent,
                "ancestry": stack[idx:]
            }
            logger.info('With phyid: %s' % phycontent['id'])

            lru.redis_conn.set(
                lru.cache_key('%s/%s' % (self._layershard, phycontent['id'])),
                json.dumps(tolru))
            # print('%s/%s' % (self._layershard, phycontent['id']))
            # print(lru.redis_conn.get('%s/%s' % (self._layershard, phycontent['id'])))
        return stack[0]
        # json.dumps(jcontent)

    def get_no_cache(self, path):
        path = self.driver._init_path(path)
        if hasattr(self.driver, 'makeKey'):
            key = self.driver.makeKey(path)
            if not key.exists():
                raise Exception('%s is not there' % path)
            return key.get_contents_as_string()
        else:
            return self.driver.get_content(path)

    # Looking up a specific tag, no caching
    # - if it's not there, fail and move on (one read on s3)
    # - if it's there, move to the digest part of it
    #   (one s3 read, on lru write, one lru delete)
    def image(self, user, name, tag):
        mainkey = self._manifestpath(user, name, tag)
        logger.info('Looking up go image: %s' % (mainkey))
        try:
            # Access the main manifest entry point
            content = self.get_no_cache(mainkey)
            logger.info('Go image is here')
            # Don't store this one
            # lru.redis_conn.delete(mainkey)
            # Return the digest version
            return self.image_by_digest('%s/%s' % (self._imageshard, content))
        except Exception as e:
            logger.info('No go image, or something wrong %s' % e)

    def delete(self, user, name, tag):
        self.driver.remove(self._manifestpath(user, name, tag))

    # Getting layer infos from golang cache:
    # - just look it up in the lru, return a {gopath: , legacy: } object
    # - if it's not there, it's not there
    def layer(self, id):
        return self._lookup_layer('%s/%s' % (self._layershard, id))

    def tags(self, user, name):
        alltags = self.driver.list_directory(self._manifestbase(user, name))
        for i in alltags:
            shorttag = i.split('/').pop()
            oldid = self.image(user, name, shorttag)
            if oldid:
                yield (shorttag, oldid)
