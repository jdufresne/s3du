import csv
import sys
import argparse
import boto3
import shutil


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('buckets', nargs='*', metavar='BUCKET')
    return parser.parse_args()


def human_readable(size):
    suffix = ['', 'k', 'M', 'G', 'T']
    i = 0
    while size >= 1000 and i < len(suffix):
        i += 1
        size = size / 1000
    return f'{size:0.2f} {suffix[i]}B'


def restart_line():
    terminal_size = shutil.get_terminal_size()
    blank = terminal_size.columns * ' '
    sys.stdout.write(f'\r{blank}\r')


def handle_bucket(client, bucket_name):
    print(bucket_name)
    underline = '-' * len(bucket_name)
    sys.stdout.write(f'{underline}\n')

    count = 0
    total = 0

    paginator = client.get_paginator('list_objects_v2')
    it = paginator.paginate(Bucket=bucket_name)
    for page in it:
        if page["KeyCount"]:
            count += len(page['Contents'])
            total += sum(obj["Size"] for obj in page['Contents'])

            restart_line()
            sys.stdout.write(f'{human_readable(total)} ({count} files)')
            sys.stdout.flush()

    restart_line()
    print(f'{human_readable(total)} ({count} files)')
    print()

    return count, total


def handle_buckets(client, bucket_names):
    rows = []

    for bucket_name in bucket_names:
        count, total = handle_bucket(client, bucket_name)
        rows.append((bucket_name, count, total))

    writer = csv.writer(sys.stdout)
    writer.writerow(['name', 'file count', 'size'])
    for row in rows:
        writer.writerow(row)


def main():
    args = parse_args()

    client = boto3.client('s3')
    if args.buckets:
        handle_buckets(client, args.buckets)
    else:
        response = client.list_buckets()
        buckets = [obj['Name'] for obj in response['Buckets']]
        handle_buckets(client, buckets)


if __name__ == '__main__':
    main()
