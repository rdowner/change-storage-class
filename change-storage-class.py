#!/usr/bin/env python3

import csv
import boto3

s3 = boto3.client('s3')

with open('../query.csv', newline='', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
        print(f'{row["actual_storage_class"]} -> {row["recommended_storage_class"]} {row["key"]}')
        copy_source = {
                'Bucket': 'BUCKETNAME-GOES-HERE',
                'Key': row["key"]
        }
        s3.copy(copy_source, copy_source['Bucket'], copy_source['Key'],
                ExtraArgs = {
                    'StorageClass': row["recommended_storage_class"],
                    'MetadataDirective': 'COPY'
                    })

        line_count += 1
    print(f'Processed {line_count} lines.')
