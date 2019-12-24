import logging
import etcd
import uuid

_log = logging.getLogger(__name__)

class Lock(object):
    """
    Locking recipe for etcd, inspired by the kazoo recipe for zookeeper
    """

    def __init__(self, client, lock_name):
        self.client = client
        self.name = lock_name
        # props to Netflix Curator for this trick. It is possible for our
        # create request to succeed on the server, but for a failure to
        # prevent us from getting back the full path name. We prefix our
        # lock name with a uuid and can check for its presence on retry.
        self._uuid = uuid.uuid4().hex
        self.path = "{}/{}".format(client.lock_prefix, lock_name) 
        self.is_taken = False
        self._sequence = None
        _log.debug("Initiating lock for %s with uuid %s", self.path, self._uuid)

    @property
    def uuid(self):
        """
        The unique id of the lock
        """
        return self._uuid

    @uuid.setter
    def uuid(self, value):
        old_uuid = self._uuid
        self._uuid = value
        if not self._find_lock():
            _log.warn("The hand-set uuid was not found, refusing")
            self._uuid = old_uuid
            raise ValueError("Inexistent UUID")

    @property
    def is_acquired(self):
        """
        tells us if the lock is acquired
        """
        if not self.is_taken:
            _log.debug("Lock not taken")
            return False
        try:
            self.client.read(self.lock_key)
            return True
        except etcd.EtcdKeyNotFound:
            _log.warn("Lock was supposedly taken, but we cannot find it")
            self.is_taken = False
            return False

    def acquire(self, blocking=True, lock_ttl=3600, timeout=0):
        """
        Acquire the lock.

        :param blocking Block until the lock is obtained, or timeout is reached
        :param lock_ttl The duration of the lock we acquired, set to None for eternal locks
        :param timeout The time to wait before giving up on getting a lock
        """
        # First of all try to write, if our lock is not present.
        if not self._find_lock():
            _log.debug("Lock not found, writing it to %s", self.path)
            res = self.client.write(self.path, self.uuid, ttl=lock_ttl, append=True)
            self._set_sequence(res.key)
            _log.debug("Lock key %s written, sequence is %s", res.key, self._sequence)
        elif lock_ttl:
            # Renew our lock if already here!
            self.client.write(self.lock_key, self.uuid, ttl=lock_ttl)

        # now get the owner of the lock, and the next lowest sequence
        return self._acquired(blocking=blocking, timeout=timeout)

    def release(self):
        """
        Release the lock
        """
        if not self._sequence:
            self._find_lock()
        try:
            _log.debug("Releasing existing lock %s", self.lock_key)
            self.client.delete(self.lock_key)
        except etcd.EtcdKeyNotFound:
            _log.info("Lock %s not found, nothing to release", self.lock_key)
            pass
        finally:
            self.is_taken = False

    def __enter__(self):
        """
        You can use the lock as a contextmanager
        """
        self.acquire(blocking=True, lock_ttl=None)
        return self

    def __exit__(self, type, value, traceback):
        self.release()
        return False

    def _acquired(self, blocking=True, timeout=0):
        locker, nearest = self._get_locker()
        self.is_taken = False
        if self.lock_key == locker:
            _log.debug("Lock acquired!")
            # We own the lock, yay!
            self.is_taken = True
            return True
        else:
            self.is_taken = False
            if not blocking:
                return False
            # Let's look for the lock
            watch_key = nearest.key
            _log.debug("Lock not acquired, now watching %s", watch_key)
            t = max(0, timeout)
            while True:
                try:
                    r = self.client.watch(watch_key, timeout=t, index=nearest.modifiedIndex + 1)
                    _log.debug("Detected variation for %s: %s", r.key, r.action)
                    return self._acquired(blocking=True, timeout=timeout)
                except etcd.EtcdKeyNotFound:
                    _log.debug("Key %s not present anymore, moving on", watch_key)
                    return self._acquired(blocking=True, timeout=timeout)
                except etcd.EtcdLockExpired as e:
                    raise e
                except etcd.EtcdException:
                    _log.exception("Unexpected exception")

    @property
    def lock_key(self):
        if not self._sequence:
            raise ValueError("No sequence present.")
        return self.path + '/' + str(self._sequence)

    def _set_sequence(self, key):
        self._sequence = key.replace(self.path, '').lstrip('/')

    def _find_lock(self):
        if self._sequence:
            try:
                res = self.client.read(self.lock_key)
                self._uuid = res.value
                return True
            except etcd.EtcdKeyNotFound:
                return False
        elif self._uuid:
            try:
                for r in self.client.read(self.path, recursive=True).leaves:
                    if r.value == self._uuid:
                        self._set_sequence(r.key)
                        return True
            except etcd.EtcdKeyNotFound:
                pass
        return False

    def _get_locker(self):
        results = [res for res in
                   self.client.read(self.path, recursive=True).leaves]
        if not self._sequence:
            self._find_lock()
        l = sorted([r.key for r in results])
        _log.debug("Lock keys found: %s", l)
        try:
            i = l.index(self.lock_key)
            if i == 0:
                _log.debug("No key before our one, we are the locker")
                return (l[0], None)
            else:
                _log.debug("Locker: %s, key to watch: %s", l[0], l[i-1])
                return (l[0], next(x for x in results if x.key == l[i-1]))
        except ValueError:
            # Something very wrong is going on, most probably
            # our lock has expired
            raise etcd.EtcdLockExpired(u"Lock not found")
