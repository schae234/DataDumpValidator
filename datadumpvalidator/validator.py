import asyncio
import hashlib
import backoff
import aioboto3
import contextlib
import boto3

from pathlib import Path
from tinydb import TinyDB, where
from ssl import SSLCertVerificationError

BUCKET = 'mccue-lab'
CHUNK_SIZE = 69 * 1024
   
db = TinyDB('checksums.json')

async def fetch_keys():
    tasks = []
    sem = asyncio.Semaphore(5)
    print('Fetching Keys',flush=True)    

    conn = boto3.client(
        service_name='s3',
        aws_access_key_id='',
        aws_secret_access_key='', 
        endpoint_url='https://s3.msi.umn.edu',
    )

    pages = conn.get_paginator('list_objects')
    prev_keys = {k['Key']:k for k in db.table().all()}

    for i,page in enumerate(pages.paginate(Bucket=BUCKET)):

        for k in page['Contents']:
            # Get previous
            k['LastModified'] = k['LastModified'].timestamp()
            try:
                prev = prev_keys[k["Key"]]
                if 'ibiodatatransfer' not in k['Key'] and not k['Key'].endswith('/'):
                    pass
                elif prev is not None and k['LastModified'] <= prev['LastModified']:
                    pass
                else:
                    tasks.append(asyncio.create_task(worker(k,sem)))
            except KeyError:
                pass

    print(f'have got {len(tasks)} tasks',flush=True)
    await asyncio.gather(*tasks)

@contextlib.contextmanager
def suppress_ssl_exception_report():
    loop = asyncio.get_event_loop()
    old_handler = loop.get_exception_handler()
    old_handler_fn = old_handler or (lambda _loop, ctx: loop.default_exception_handler(ctx))
    def ignore_exc(_loop, ctx):
        exc = ctx.get('exception')
        if isinstance(exc, SSLCertVerificationError):
            return
        old_handler_fn(loop, ctx)
    loop.set_exception_handler(ignore_exc)
    try:
        yield
    finally:
        loop.set_exception_handler(old_handler)

def backoff_hdlr(details):
    print(
        "Backing off {wait:0.1f} seconds afters {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details), flush=True
    )

@backoff.on_exception(
    backoff.expo, Exception, max_tries=8, on_backoff=backoff_hdlr
)
async def worker(key,sem,dry_run=True):
    with suppress_ssl_exception_report():
        async with aioboto3.client(
            service_name='s3',
            aws_access_key_id='',
            aws_secret_access_key='', 
            endpoint_url='https://s3.msi.umn.edu',
            verify=False
        ) as conn:
            md5 = hashlib.md5()
            async with sem:
                obj = await conn.get_object(Bucket='mccue-lab',Key=key['Key'])
                print(f"Working on {key['Key']}",flush=True)
                if dry_run == 'True':
                    pass
                else:
                    async with obj["Body"] as stream :
                        data = await stream.read(CHUNK_SIZE)
                        while data:
                            md5.update(data)
                            data = await stream.read(CHUNK_SIZE)
                    print(f'The checksum for {key["Key"]} is {md5.hexdigest()}',flush=True)
                    key['md5sum']  = md5.hexdigest()      
                    db.upsert(key,where('Key') == key["Key"])

if __name__ == '__main__':
    asyncio.run(fetch_keys())
