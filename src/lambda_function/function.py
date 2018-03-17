import boto3
import json
import logging.config
import os
import psycopg2
import re

from boto3.dynamodb.types import TypeDeserializer
from datetime import datetime
from jsonpointer import resolve_pointer

base_redshift_connection_string = "host='" + \
                                  os.environ['REDSHIFT_HOST'] + \
                                  "' dbname='" + \
                                  os.environ['REDSHIFT_DB_NAME'] + \
                                  "' port='" + \
                                  os.environ.get('REDSHIFT_PORT', '5439') + \
                                  "'"
boto3.resource('dynamodb')
cluster_credentials = {}
if 'REDSHIFT_PASSWORD' in os.environ:
    cluster_credentials['DbUser'] = os.environ['REDSHIFT_USER']
    cluster_credentials['DbPassword'] = os.environ['REDSHIFT_PASSWORD']
    cluster_credentials['Expiration'] = datetime.max
deserializer = TypeDeserializer()
dynamo_redshift_etl = json.loads(os.environ['DYNAMO_REDSHIFT_ETL'])
dynamo_table_re = re.compile('^arn:aws:dynamodb:[a-z]{2}-[a-z]*-[0-9]:[0-9]*:table/(.+?)/')
redshift = boto3.client('redshift')


def get_connection_string():
    if 'Expiration' not in cluster_credentials or (cluster_credentials['Expiration'] - datetime.utcnow()).total_seconds() <= 30:
        temp_credentials = redshift.get_cluster_credentials(
            ClusterIdentifier=os.environ['REDSHIFT_CLUSTER_IDENTIFIER'],
            DbName=os.environ['REDSHIFT_DB_NAME'],
            DbUser=os.environ['REDSHIFT_USER'],
            DurationSeconds=3600
        )
        cluster_credentials['DbUser'] = temp_credentials['DbUser']
        cluster_credentials['DbPassword'] = temp_credentials['DbPassword']
        cluster_credentials['Expiration'] = temp_credentials['Expiration'].replace(tzinfo=None)
    return base_redshift_connection_string + \
           " user='" + \
           cluster_credentials['DbUser'] + \
           "' password='" + \
           cluster_credentials['DbPassword'] + \
           "'"


def upsert(logger, redshift_connection, redshift_table, primary_key, fields):
    with redshift_connection.cursor() as cursor:
        logger.debug('Checking if {} exist. '.format(fields[primary_key]))
        cursor.execute(
            'SELECT * FROM {} WHERE {} = %s'.format(
                redshift_table,
                primary_key
            ),
            (fields[primary_key],)
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
            logger.debug(', '.join('{0}=%({0})s'.format(k) for k in fields.keys()))
            cursor.execute(
                'UPDATE {0} SET {1} WHERE {2} = %({2})s'.format(
                    redshift_table,
                    ', '.join('{0}=%({0})s'.format(k) for k in fields.keys()),
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
                'INSERT INTO {0} ({1}) VALUES ({2})'.format(
                    redshift_table,
                    ', '.join(fields.keys()),
                    ', '.join('%({0})s'.format(k) for k in fields.keys())),
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
        connection_string = get_connection_string()
        logger.debug('Connecting to Redshift with connection string: {}'.format(connection_string))
        with psycopg2.connect(connection_string) as redshift_connection:
            logger.info('Connected to redshift')
            for record in filter(lambda x: x['eventName'] == 'INSERT' or x['eventName'] == 'MODIFY', event['Records']):
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
