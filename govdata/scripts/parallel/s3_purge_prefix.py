#!/usr/bin/env python3
"""Delete every object under <schema>/ in an S3-compatible bucket (SigV4).

usage: s3_purge_prefix.py <endpoint> <bucket> <schema> [timeout_seconds] [attempts]

Every request carries an explicit socket timeout and is retried a bounded number of
times. On exhaustion the script exits 1 naming the failing operation and key.
Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
"""
import datetime
import hashlib
import hmac
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

NS = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}


def sign(k, m):
    return hmac.new(k, m.encode(), hashlib.sha256).digest()


def fail(what, key, err):
    print(f'S3 purge failed: {what} {key}: {err}', file=sys.stderr)
    sys.exit(1)


def main():
    ep, bucket, schema = sys.argv[1:4]
    timeout = float(sys.argv[4]) if len(sys.argv) > 4 else 30.0
    attempts = int(sys.argv[5]) if len(sys.argv) > 5 else 3
    kid = os.environ['AWS_ACCESS_KEY_ID']
    secret = os.environ['AWS_SECRET_ACCESS_KEY']
    if not ep:
        return
    host = ep.replace('http://', '').replace('https://', '')

    def request(method, path, qs_dict, what, key):
        last = None
        for _ in range(attempts):
            now = datetime.datetime.now(datetime.timezone.utc)
            d, ts = now.strftime('%Y%m%d'), now.strftime('%Y%m%dT%H%M%SZ')
            ph = hashlib.sha256(b'').hexdigest()
            qs = '&'.join(f'{urllib.parse.quote(k, safe="")}={urllib.parse.quote(str(v), safe="")}'
                          for k, v in sorted(qs_dict.items()))
            hdr = f'host:{host}\nx-amz-content-sha256:{ph}\nx-amz-date:{ts}\n'
            sh = 'host;x-amz-content-sha256;x-amz-date'
            cr = f'{method}\n/{bucket}{path}\n{qs}\n{hdr}\n{sh}\n{ph}'
            sts = (f'AWS4-HMAC-SHA256\n{ts}\n{d}/us-east-1/s3/aws4_request\n'
                   f'{hashlib.sha256(cr.encode()).hexdigest()}')
            k = sign(sign(sign(sign(('AWS4' + secret).encode(), d), 'us-east-1'), 's3'), 'aws4_request')
            sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
            auth = (f'AWS4-HMAC-SHA256 Credential={kid}/{d}/us-east-1/s3/aws4_request,'
                    f'SignedHeaders={sh},Signature={sig}')
            url = f'{ep}/{bucket}{path}' + (f'?{qs}' if qs else '')
            req = urllib.request.Request(url, method=method)
            for h, v in [('Host', host), ('x-amz-date', ts), ('x-amz-content-sha256', ph),
                         ('Authorization', auth)]:
                req.add_header(h, v)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return r.read().decode()
            except urllib.error.HTTPError as e:
                if e.code < 500:
                    fail(what, key, f'HTTP {e.code}')
                last = e
            except (urllib.error.URLError, OSError) as e:
                last = e
        fail(what, key, f'{attempts} attempts failed, last error: {last}')

    total, token = 0, None
    while True:
        params = {'list-type': '2', 'prefix': f'{schema}/', 'max-keys': '1000'}
        if token:
            params['continuation-token'] = token
        resp = request('GET', '/', params, 'LIST', f'{schema}/')
        root = ET.fromstring(resp)
        keys = [c.find('s:Key', NS).text for c in root.findall('.//s:Contents', NS)]
        for key in keys:
            enc = '/'.join(urllib.parse.quote(p, safe='') for p in key.split('/'))
            request('DELETE', f'/{enc}', {}, 'DELETE', key)
            total += 1
        nxt = root.find('s:NextContinuationToken', NS)
        if nxt is None:
            break
        token = nxt.text
    print(f'Purged {total} objects for schema={schema} from {bucket}')


if __name__ == '__main__':
    main()
