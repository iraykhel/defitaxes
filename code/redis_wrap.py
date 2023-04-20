import redis
from .util import log, log_error
# from .chain import Chain
import time

class Redis:
    def __init__(self,address):
        # log("init redis",address)
        self.R = redis.StrictRedis(host='localhost',decode_responses=True)
        self.address = address
        self.queue = 'queue'
        self.pushed=False
        # address, chain_name, uid = get_session_vars()
        # log("session vars for redis", address, uid)
        # if uid is not None:
        #     self.uid = uid
        # else:
        self.uid = self.address

    def enq(self,reset=False):
        if self.pushed:
            return
        R = self.R
        self.set('last_update', int(time.time()))
        if reset:
            R.lrem(self.queue, 0, self.uid)
        else:
            redis_len = R.llen(self.queue)
            for idx in range(redis_len):
                if R.lindex(self.queue, idx) == self.uid:
                    return
        self.R.rpush(self.queue, self.uid)
        self.pushed = True

    def waitself(self, sleep=1):
        rv = False

        running = self.get('running')

        if running is not None:
            rv = True
            if self.qpos() is not None:
                self.wait()

        while running is not None:
            # top = R.lindex(self.queue, 0)
            # if top != self.uid:
            #     top_progress = min(100, round(float(R.get(top + "_progress")), 2))
            #     self.set('progress_entry', R.get(top+"_progress_entry"))
            #     self.set('progress', R.get(top+"_progress"))

            time.sleep(sleep)
            running = self.get('running')
        return rv

    def wait(self,sleep=1,pb=0):
        # if self.address.lower() != '0xd603a49886c9B500f96C0d798aed10068D73bF7C'.lower():
        #     return

        R = self.R
        wait_start = int(time.time())
        last_update_change = wait_start
        prev_top_last_update = None
        error_recorded = False

        top = R.lindex(self.queue, 0)
        while top != self.uid:
            qpos = self.qpos()
            if qpos > 1:
                top_progress = min(100,round(float(R.get(top+"_progress")),2))
                top_last_update = R.get(top+"_last_update")
                self.set('progress_entry', 'Waiting for other users, your queue position:' + str(qpos)+', current user\'s progress:'+str(top_progress)+'%')
                if pb is not None:
                    self.set('progress',pb)
                self.set('last_update',int(time.time()))

                if top_last_update is None or time.time() - float(top_last_update) > 30: #assume it crashed
                    log('assuming other guy crashed',time.time(),top_last_update)
                    R.lrem(self.queue, 0, top)

                if top_last_update != prev_top_last_update:
                    last_update_change = int(time.time())
                    prev_top_last_update = top_last_update

                if not error_recorded and time.time() - last_update_change > 600:
                    log_error("WAITING TOO LONG", 'address',self.address,'redis top',top,'top_last_update',top_last_update,'time diff',time.time() - float(top_last_update))
                    error_recorded = True

            log('redis check in loop', top, self.uid)
            time.sleep(sleep)
            top = R.lindex(self.queue, 0)

    def qpos(self):
        R = self.R
        redis_len = R.llen(self.queue)
        for idx in range(redis_len):
            if R.lindex(self.queue, idx) == self.uid:
                return idx +1
        return None

    def deq(self):
        R = self.R
        R.lrem(self.queue, 0, self.uid)


    def set(self,key,val):
        R = self.R
        key = self.uid+"_"+str(key)
        # log("setting redis", key, val)
        R.set(key,val)

    def get(self, key):
        R = self.R
        key = self.uid + "_" + str(key)
        val = R.get(key)
        # log("getting redis", key, val)
        return val

    def unset(self,key):
        R = self.R
        key = self.uid + "_" + str(key)
        R.delete(key)
        # try:
        #     R.delete(key)
        # except:
        #     pass

    def start(self):
        self.set('running','1')
        self.unset('progress')
        self.unset('progress_entry')
        self.unset('last_update')

    def finish(self):
        self.unset('running')

    def cleanup(self):
        R = self.R
        t = int(time.time())
        keys = R.keys("*_last_update")
        to_delete = []
        to_stay_queued = []
        for key in keys:
            log('key',key)
            last_update = int(R.get(key))
            uid, _ = key.split("_", 1)
            if t > last_update + 1800:
                to_delete.extend([key,uid+"_progress",uid+"_progress_entry",uid+"_running"])
            else:
                to_stay_queued.append(uid)
        log("deleting redis keys",to_delete)
        if len(to_delete):
            R.delete(*to_delete)

        els = R.lrange("queue", 0, -1)
        for el in els:
            if el not in to_stay_queued:
                log("unqueueing ",el)
                R.lrem("queue",0,el)

        # chains = Chain.list()
        # for chain in chains:
        #     els = R.lrange("queue_"+chain,0,-1)
        #     for el in els:
        #         if el not in to_stay_queued:
        #             log("unqueueing ",el,"from",chain,"queue")
        #             R.lrem("queue_"+chain,0,el)



