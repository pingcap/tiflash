#!/usr/bin/python3
import errno
import os
import socket
import time
import fcntl
import logging

import requests


def wrap_run_time(func):
    def wrap_func(*args, **kwargs):
        bg = time.time()
        r = func(*args, **kwargs)
        print('time cost {}'.format(time.time() - bg))
        return r

    return wrap_func


class FLOCK(object):
    def __init__(self, name):
        self.obj = open(name, 'w')
        self.fd = self.obj.fileno()

    def lock(self):
        try:
            fcntl.lockf(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            logging.error(
                'Cannot lock file {}. Another instance in same directory is already running'.format(self.obj.name))
            return False


def curl_http(uri, params=None):
    if params is None:
        params = {}
    import conf
    if conf.flash_conf.enable_tls and conf.flash_conf.ca_path != "":
        r = requests.get('https://{}'.format(uri), params, timeout=conf.flash_conf.update_rule_interval, verify=conf.flash_conf.ca_path, cert=(conf.flash_conf.cert_path, conf.flash_conf.key_path))
    else:
        r = requests.get('http://{}'.format(uri), params, timeout=conf.flash_conf.update_rule_interval)
    return r


def check_status_code(r):
    if r.status_code != 200:
        raise Exception('unexpected status code {} from {}'.format(r.status_code, r.url))


def try_get_json(r):
    check_status_code(r)
    return r.json()


def post_http(uri, params):
    import conf
    if conf.flash_conf.enable_tls and conf.flash_conf.ca_path != "":
        r = requests.post('https://{}'.format(uri), json=params, timeout=conf.flash_conf.update_rule_interval, verify=conf.flash_conf.ca_path, cert=(conf.flash_conf.cert_path, conf.flash_conf.key_path))
    else:
        r = requests.post('http://{}'.format(uri), json=params, timeout=conf.flash_conf.update_rule_interval)
    return r


def delete_http(uri):
    import conf
    if conf.flash_conf.enable_tls and conf.flash_conf.ca_path != "":
        r = requests.delete('https://{}'.format(uri), timeout=conf.flash_conf.update_rule_interval, verify=conf.flash_conf.ca_path, cert=(conf.flash_conf.cert_path, conf.flash_conf.key_path))
    else:
        r = requests.delete('http://{}'.format(uri), timeout=conf.flash_conf.update_rule_interval)
    return r


def obj_2_dict(obj):
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if not name.startswith('_') and not callable(value):
            pr[name] = value
    return pr


def make_compare_pd_key(key):
    return (1, key) if key else (0, '')


def net_is_used(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = s.connect_ex((ip, port))
    s.close()
    return result == 0


def pid_exists(pid):
    """Check whether pid exists in the current process table.
    UNIX only.
    """
    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True


def pid_exists2(pid):
    if pid == 0:
        return True
    return pid_exists(pid)


def compute_addr_list(addrs):
    return [e.strip() for e in addrs.split(',') if e]


def main():
    pass


if __name__ == '__main__':
    main()
