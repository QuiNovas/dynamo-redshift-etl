import boto3
import logging.config
import os

from boto3.dynamodb.conditions import Key


def handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    return event
