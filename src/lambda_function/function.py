import boto3
import json
import logging.config
import os
import psycopg2
import re

from boto3.dynamodb.types import TypeDeserializer
from jsonpointer import resolve_pointer

boto3.resource('dynamodb')
deserializer = TypeDeserializer()
dynamo_redshift_etl = json.loads(os.environ['DYNAMO_REDSHIFT_ETL'])
dynamo_table_re = re.compile('^arn:aws:dynamodb:[a-z]{2}-[a-z]*-[0-9]:[0-9]*:table/(.+?)/')
redshift_connection_string = "host='" + \
                             os.environ['REDSHIFT_HOST'] + \
                             "' dbname='" + \
                             os.environ['REDSHIFT_DB_NAME'] + \
                             "' port='" + \
                             os.environ.get('REDSHIFT_PORT', '5439') + \
                             "' user='" + \
                             os.environ['REDSHIFT_USER'] + \
                             "' password='" + \
                             os.environ['REDSHIFT_PASSWORD']


def upsert(logger, redshift_connection, redshift_table, primary_key, fields):
    with redshift_connection.cursor() as cursor:
        cursor.execute(
            'SELECT * FROM {} WHERE {} = %s'.format(
                redshift_table,
                primary_key
            ),
            (fields[primary_key], )
        )
        if cursor.fetchone():
            primary_key_value = fields.get(primary_key)
            logger.info(
                'Updating {} record {} with {}'.format(
                    redshift_table,
                    primary_key_value,
                    json.dumps(fields)
                )
            )
            updates = []
            for field in fields:
                if primary_key != field[0]:
                    updates.append('{0} = {%({0})s}'.format(field[0]))
            cursor.execute(
                'UPDATE {0} SET {1} WHERE {2} = %{{2})s'.format(
                    redshift_table,
                    ', '.join(updates),
                    primary_key
                ),
                fields
            )
            logger.info(
                'Updated {} record {}'.format(
                    redshift_table,
                    primary_key_value
                )
            )
        else:
            logger.info(
                'Inserting {} into {}'.format(
                    json.dumps(fields),
                    redshift_table
                )
            )
            cursor.execute(
                'INSERT INTO {0}({1}) VALUES ({})'.format(
                    redshift_table,
                    ', '.join(fields.keys()),
                    '%({})s, '.join(fields.keys())
                ),
                fields
            )
            logger.info(
                'Inserted {} into {}'.format(
                    fields[primary_key],
                    redshift_table
                )
            )


def handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.info('Processing event {}'.format(json.dumps(event)))

    if 'Records' in event and len(event['Records']):
        logger.debug('Connecting to Redshift with connection string: {}'.format(redshift_connection_string))
        with psycopg2.connect(redshift_connection_string) as redshift_connection:
            logger.info('Connected to redshift')
            for record in filter(lambda x: x['eventName'] == 'INSERT', event['Records']):
                dynamodb_table = dynamo_table_re.search(record['eventSourceARN']).group(1)
                etl = dynamo_redshift_etl.get(dynamodb_table, None)
                if etl:
                    new_image = {k: deserializer.deserialize(v) for k, v in record['dynamodb']['NewImage'].items()}
                    fields = {}
                    for item in etl['fields'].items():
                        value = resolve_pointer(new_image, item[1])
                        if value:
                            fields[item[0]] = value
                    if len(fields) > 1:
                        upsert(
                            logger,
                            redshift_connection,
                            etl['table'],
                            etl['primaryKey'],
                            fields
                        )
                    else:
                        logger.warn('Nothing to upsert for {}'.format(dynamodb_table))
                else:
                    logger.error('Unable to find etl for {}'.format(dynamodb_table))
            logger.info('Processing complete')
    return event
