import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import boto3
from botocore.exceptions import ClientError

from mmmeta import mmmeta

DATA_DIR = os.path.abspath('./data')


def json_encoder(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError('Could not encode to %s JSON' % obj)


def sync_meta_s3(bucket_name, s3_dir):
    resource = boto3.resource('s3', endpoint_url=os.environ["ARCHIVE_ENDPOINT_URL"])
    bucket = resource.Bucket(bucket_name)
    prefix = '{}/_mmmeta'.format(s3_dir)
    for obj in bucket.objects.filter(Prefix=prefix):
        print('Syncing', obj.key)
        path = os.path.join(DATA_DIR, obj.key)
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        with open(path, 'wb') as f:
            bucket.download_fileobj(obj.key, f)

    path = os.path.join(DATA_DIR, s3_dir)
    # return dir with _mmmeta dir inside
    return path


def get_new_files(m):
    print('Listing unimported files')
    for file in m.files.find(imported=None, __deleted=None):
        yield file


def get_key(file):
    return file.remote.s3_key


def download_file(bucket, file, target_dir):
    ext = 'pdf'
    key = get_key(file)
    print('Downloading key', key)

    resource = boto3.resource('s3', endpoint_url=os.environ["ARCHIVE_ENDPOINT_URL"])
    bucket = resource.Bucket(bucket)

    path = os.path.abspath(
        target_dir / '{}.{}'.format(file['content_hash'], ext)
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        # File already present
        return path
    try:
        with open(path, 'wb') as f:
            bucket.download_fileobj(key, f)
        return path
    except ClientError:
        print('404 for', key)
        if os.path.exists(path):
            os.remove(path)
    return None


def ellipse(s, n=500):
    if len(s) > n:
        return s[:(n-1)] + '…'
    return s


def process_file(collection: str, bucket: str, file_row, target_dir: Path, tag=None):
    pdf_path = download_file(bucket, file_row, target_dir)
    if pdf_path is None:
        return
    metadata = file_row._data

    meta_path = pdf_path.replace('.pdf', '.json')

    fc_meta = {
        'content_hash': metadata['content_hash'],
        'title': ellipse(metadata['title']) or '',
        'description': metadata.get('keywords', '') or '',
        'published_at': metadata['published_at'],
        'language': 'de',
        'allow_annotation': True,
        'tags': [tag] if tag else [],
        'properties': {
            'title': metadata['title'],
            'foreign_id': metadata['foreign_id'],
            'url': metadata['url'],
            'publisher': metadata['publisher:name'],
            'publisher_url': metadata['publisher:url'],
            'reference': metadata['reference'],
        },
        'data': {
            'category': metadata.get('category'),
            'publisher': metadata['publisher:jurisdiction:id'],
            'document_type': metadata['document_type'],
            'legislative_term': metadata.get('legislative_term'),
            # 'jurisdiction': metadata['publisher']['jurisdiction']['id'],
        },
        'portal': collection
    }

    with open(meta_path, 'w') as f:
        json.dump(fc_meta, f, default=json_encoder)

    return [pdf_path, meta_path]


def call_import(command, target_dir):
    if not target_dir.exists():
        return
    call_args = command.split(' ')
    print('Running import')
    command = subprocess.run(call_args, capture_output=True)
    sys.stdout.buffer.write(command.stdout)
    sys.stderr.buffer.write(command.stderr)
    if command.returncode == 0:
        shutil.rmtree(target_dir)
        os.makedirs(target_dir)
    else:
        raise Exception


def run_update(bucket, s3_dir):
    print('Syncing...')
    path = sync_meta_s3(bucket, s3_dir)
    print(path)
    m = mmmeta(path)
    print('Updating local state')
    m.update()
    print('create column')
    m.files.create_column_by_example('imported', False)
    return m


def mark_imported(m, batch):
    m.files.update_many(
        [{"content_hash": c, "imported": 1} for c in batch],
        ['content_hash']
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", type=str)
    parser.add_argument("--bucket", type=str)
    parser.add_argument("--dir", type=str)
    parser.add_argument("--tag", default='', type=str, nargs='?', const='')
    parser.add_argument("--target", type=Path)
    parser.add_argument("--command", type=str)
    args = parser.parse_args()
    collection = args.collection
    s3_bucket = args.bucket
    s3_dir = args.dir
    target_dir = args.target
    command = args.command

    m = run_update(s3_bucket, s3_dir)

    BATCH_SIZE = 100
    batch = []
    for file_row in get_new_files(m):
        result = process_file(
            collection, s3_bucket, file_row, target_dir, tag=args.tag
        )
        if result is None:
            continue
        batch.append(file_row['content_hash'])
        if len(batch) == BATCH_SIZE:
            call_import(command, target_dir)
            mark_imported(m, batch)
            batch = []
    if batch:
        call_import(command, target_dir)
        mark_imported(m, batch)


if __name__ == "__main__":
    main()
