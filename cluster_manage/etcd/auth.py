import json

import logging
import etcd

_log = logging.getLogger(__name__)


class EtcdAuthBase(object):
    entity = 'example'

    def __init__(self, client, name):
        self.client = client
        self.name = name
        self.uri = "{}/auth/{}s/{}".format(self.client.version_prefix,
                                           self.entity, self.name)

    @property
    def names(self):
        key = "{}s".format(self.entity)
        uri = "{}/auth/{}".format(self.client.version_prefix, key)
        response = self.client.api_execute(uri, self.client._MGET)
        return json.loads(response.data.decode('utf-8'))[key]

    def read(self):
        try:
            response = self.client.api_execute(self.uri, self.client._MGET)
        except etcd.EtcdInsufficientPermissions as e:
            _log.error("Any action on the authorization requires the root role")
            raise
        except etcd.EtcdKeyNotFound:
            _log.info("%s '%s' not found", self.entity, self.name)
            raise
        except Exception as e:
            _log.error("Failed to fetch %s in %s%s: %r",
                       self.entity, self.client._base_uri,
                       self.client.version_prefix, e)
            raise etcd.EtcdException(
                "Could not fetch {} '{}'".format(self.entity, self.name))

        self._from_net(response.data)

    def write(self):
        try:
            r = self.__class__(self.client, self.name)
            r.read()
        except etcd.EtcdKeyNotFound:
            r = None
        try:
            for payload in self._to_net(r):
                response = self.client.api_execute_json(self.uri,
                                                        self.client._MPUT,
                                                        params=payload)
                # This will fail if the response is an error
                self._from_net(response.data)
        except etcd.EtcdInsufficientPermissions as e:
            _log.error("Any action on the authorization requires the root role")
            raise
        except Exception as e:
            _log.error("Failed to write %s '%s'", self.entity, self.name)
            # TODO: fine-grained exception handling
            raise etcd.EtcdException(
                "Could not write {} '{}': {}".format(self.entity,
                                                     self.name, e))

    def delete(self):
        try:
            _ = self.client.api_execute(self.uri, self.client._MDELETE)
        except etcd.EtcdInsufficientPermissions as e:
            _log.error("Any action on the authorization requires the root role")
            raise
        except etcd.EtcdKeyNotFound:
            _log.info("%s '%s' not found", self.entity, self.name)
            raise
        except Exception as e:
            _log.error("Failed to delete %s in %s%s: %r",
                       self.entity, self._base_uri, self.version_prefix, e)
            raise etcd.EtcdException(
                "Could not delete {} '{}'".format(self.entity, self.name))

    def _from_net(self, data):
        raise NotImplementedError()

    def _to_net(self, old=None):
        raise NotImplementedError()

    @classmethod
    def new(cls, client, data):
        c = cls(client, data[cls.entity])
        c._from_net(data)
        return c


class EtcdUser(EtcdAuthBase):
    """Class to manage in a orm-like way etcd users"""
    entity = 'user'

    def __init__(self, client, name):
        super(EtcdUser, self).__init__(client, name)
        self._roles = set()
        self._password = None

    def _from_net(self, data):
        d = json.loads(data.decode('utf-8'))
        self.roles = d.get('roles', [])
        self.name = d.get('user')

    def _to_net(self, prevobj=None):
        if prevobj is None:
            retval = [{"user": self.name, "password": self._password,
                       "roles": list(self.roles)}]
        else:
            retval = []
            if self._password:
                retval.append({"user": self.name, "password": self._password})
            to_grant = list(self.roles - prevobj.roles)
            to_revoke = list(prevobj.roles - self.roles)
            if to_grant:
                retval.append({"user": self.name, "grant": to_grant})
            if to_revoke:
                retval.append({"user": self.name, "revoke": to_revoke})
        # Let's blank the password now
        # Even if the user can't be written we don't want it to leak anymore.
        self._password = None
        return retval

    @property
    def roles(self):
        return self._roles

    @roles.setter
    def roles(self, val):
        self._roles = set(val)

    @property
    def password(self):
        """Empty property for password."""
        return None

    @password.setter
    def password(self, new_password):
        """Change user's password."""
        self._password = new_password

    def __str__(self):
        return json.dumps(self._to_net()[0])



class EtcdRole(EtcdAuthBase):
    entity = 'role'

    def __init__(self, client, name):
        super(EtcdRole, self).__init__(client, name)
        self._read_paths = set()
        self._write_paths = set()

    def _from_net(self, data):
        d = json.loads(data.decode('utf-8'))
        self.name = d.get('role')

        try:
            kv = d["permissions"]["kv"]
        except:
            self._read_paths = set()
            self._write_paths = set()
            return

        self._read_paths = set(kv.get('read', []))
        self._write_paths = set(kv.get('write', []))

    def _to_net(self, prevobj=None):
        retval = []
        if prevobj is None:
            retval.append({
                "role": self.name,
                "permissions":
                {
                    "kv":
                    {
                        "read": list(self._read_paths),
                        "write": list(self._write_paths)
                    }
                }
            })
        else:
            to_grant = {
                'read': list(self._read_paths - prevobj._read_paths),
                'write': list(self._write_paths - prevobj._write_paths)
            }
            to_revoke = {
                'read': list(prevobj._read_paths - self._read_paths),
                'write': list(prevobj._write_paths - self._write_paths)
            }
            if [path for sublist in to_revoke.values() for path in sublist]:
                retval.append({'role': self.name, 'revoke': {'kv': to_revoke}})
            if [path for sublist in to_grant.values() for path in sublist]:
                retval.append({'role': self.name, 'grant': {'kv': to_grant}})
        return retval

    def grant(self, path, permission):
        if permission.upper().find('R') >= 0:
            self._read_paths.add(path)
        if permission.upper().find('W') >= 0:
            self._write_paths.add(path)

    def revoke(self, path, permission):
        if permission.upper().find('R') >= 0 and \
           path in self._read_paths:
            self._read_paths.remove(path)
        if permission.upper().find('W') >= 0 and \
           path in self._write_paths:
            self._write_paths.remove(path)

    @property
    def acls(self):
        perms = {}
        try:
            for path in self._read_paths:
                perms[path] = 'R'
            for path in self._write_paths:
                if path in perms:
                    perms[path] += 'W'
                else:
                    perms[path] = 'W'
        except:
            pass
        return perms

    @acls.setter
    def acls(self, acls):
        self._read_paths = set()
        self._write_paths = set()
        for path, permission in acls.items():
            self.grant(path, permission)

    def __str__(self):
        return json.dumps({"role": self.name, 'acls': self.acls})


class Auth(object):
    def __init__(self, client):
        self.client = client
        self.uri = "{}/auth/enable".format(self.client.version_prefix)

    @property
    def active(self):
        resp = self.client.api_execute(self.uri, self.client._MGET)
        return json.loads(resp.data.decode('utf-8'))['enabled']

    @active.setter
    def active(self, value):
        if value != self.active:
            method = value and self.client._MPUT or self.client._MDELETE
            self.client.api_execute(self.uri, method)
