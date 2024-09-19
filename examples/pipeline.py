#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import time
import logging

import boto3
from botocore.exceptions import ClientError

import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from sqs_pyio.io import WriteToSqs

QUEUE_NAME = "sqs-pyio-test"


def get_queue_url(queue_name):
    client = boto3.client("sqs")
    try:
        return client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except ClientError as error:
        if error.response["Error"]["QueryErrorCode"] == "QueueDoesNotExist":
            client.create_queue(QueueName=queue_name)
            return client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        else:
            raise error


def purge_queue(queue_name):
    client = boto3.client("sqs")
    queue_url = get_queue_url(queue_name)
    try:
        client.purge_queue(QueueUrl=queue_url)
    except ClientError as error:
        if error.response["Error"]["QueryErrorCode"] != "PurgeQueueInProgress":
            raise error


def check_number_of_messages(queue_name):
    client = boto3.client("sqs")
    queue_url = get_queue_url(queue_name)
    resp = client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return f'{resp["Attributes"]["ApproximateNumberOfMessages"]} messages are found approximately!'


def mask_secrets(d: dict):
    return {k: (v if k.find("aws") < 0 else "x" * len(v)) for k, v in d.items()}


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--queue_name", default=QUEUE_NAME, type=str, help="SQS queue name"
    )
    parser.add_argument(
        "--num_records", default="100", type=int, help="Number of records"
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known_args - {known_args}")
    print(f"pipeline options - {mask_secrets(pipeline_options.display_data())}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateElements"
            >> beam.Create(
                [
                    {"Id": str(i), "MessageBody": str(i)}
                    for i in range(known_args.num_records)
                ]
            )
            | "BatchElements" >> BatchElements(min_batch_size=10, max_batch_size=10)
            | "WriteToSqs"
            >> WriteToSqs(
                queue_name=known_args.queue_name,
                max_trials=3,
                append_error=True,
                failed_output="my-failed-output",
            )
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    # check if queue exists
    get_queue_url(QUEUE_NAME)
    print(">> start pipeline...")
    run()
    time.sleep(1)
    print(check_number_of_messages(QUEUE_NAME))
    print(">> purge existing messages...")
    purge_queue(QUEUE_NAME)
