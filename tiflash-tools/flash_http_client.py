#!/usr/bin/python3

import util


def curl_flash(address, params):
    if type(params) != dict:
        params = {'query': params}
    r = util.curl_http(address, params)
    return r


def get_compress_info(tiflash_status_address):
    res = util.curl_http(
        '{}/tiflash/tzg-compress'.format(tiflash_status_address))
    util.check_status_code(res)
    data = res.text
    data = [e.strip() for e in data.split(',')]
    res = {}
    for e in data:
        a, b = e.split(":")
        res[a.strip()] = b.strip()
    return res


def get_stream_info(tiflash_status_address):
    res = util.curl_http(
        '{}/tiflash/get-tzg-compress-stream-cnt'.format(tiflash_status_address))
    util.check_status_code(res)
    return res.text


def get_codec_info(tiflash_status_address):
    res = util.curl_http(
        '{}/tiflash/get-tzg-encode-info'.format(tiflash_status_address))
    util.check_status_code(res)
    return res.text


def set_compress_method(tiflash_status_address, method):
    res = util.curl_http(
        '{}/tiflash/set-tzg-compress-method/{}'.format(tiflash_status_address, method))
    util.check_status_code(res)
    return res.text


def clean_compress_info(tiflash_status_address, ):
    res = util.curl_http(
        '{}/tiflash/tzg-clean-compress'.format(tiflash_status_address, ))
    util.check_status_code(res)
    return res.text
