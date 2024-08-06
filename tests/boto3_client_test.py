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


import unittest
from moto import mock_aws

from sqs_pyio.boto3_client import SqsClient, SqsClientError


def create_queue(sqs_client, **kwargs):
    sqs_client.client.create_queue(**kwargs)


def receive_message(sqs_client, **kwargs):
    queue_url = sqs_client.client.get_queue_url(QueueName=kwargs["queue_name"])[
        "QueueUrl"
    ]
    return sqs_client.client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=kwargs["max_msgs"]
    )


@mock_aws
class TestBoto3Client(unittest.TestCase):
    queue_name = "test-sqs-queue"

    def setUp(self):
        options = {
            "aws_access_key_id": "testing",
            "aws_secret_access_key": "testing",
            "region_name": "us-east-1",
        }

        self.sqs_client = SqsClient(options)
        create_queue(self.sqs_client, QueueName=self.queue_name)

    def test_get_queue_url(self):
        queue_url = self.sqs_client.get_queue_url(self.queue_name)
        assert self.queue_name in queue_url

    def test_get_queue_url_with_non_existing_queue(self):
        self.assertRaises(
            SqsClientError, self.sqs_client.get_queue_url, "non-existing-queue-name"
        )

    def test_send_message_batch(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
        resp = self.sqs_client.send_message_batch(records, self.queue_name)
        assert [r["Id"] for r in records] == [r["Id"] for r in resp["Successful"]]

        messages = receive_message(
            self.sqs_client, queue_name=self.queue_name, max_msgs=3
        )["Messages"]
        assert set([r["MessageBody"] for r in records]) == set(
            [m["Body"] for m in messages]
        )

    def test_send_message_batch_with_unsupported_data_types(self):
        # Id should be a string
        records = [{"Id": i, "MessageBody": str(i)} for i in range(3)]
        self.assertRaises(
            SqsClientError, self.sqs_client.send_message_batch, records, self.queue_name
        )
        # MessageBody should be a string
        records = [{"Id": str(i), "MessageBody": i} for i in range(3)]
        self.assertRaises(
            SqsClientError, self.sqs_client.send_message_batch, records, self.queue_name
        )

    def test_send_message_batch_with_unsupported_record_types(self):
        # only the list type is supported!
        self.assertRaises(
            SqsClientError, self.sqs_client.send_message_batch, {}, self.queue_name
        )

        self.assertRaises(
            SqsClientError, self.sqs_client.send_message_batch, "abc", self.queue_name
        )

    def test_send_message_batch_without_mandatory_attributes(self):
        # Id and MessageBody are mandatory message attributes
        without_ids = [{"MessageBody": str(i)} for i in range(3)]
        self.assertRaises(
            SqsClientError,
            self.sqs_client.send_message_batch,
            without_ids,
            self.queue_name,
        )

        without_bodies = [{"Id": str(i)} for i in range(3)]
        self.assertRaises(
            SqsClientError,
            self.sqs_client.send_message_batch,
            without_bodies,
            self.queue_name,
        )

    def test_send_message_batch_exceeding_max_batch_size(self):
        # Id and MessageBody are mandatory message attributes
        without_ids = [{"MessageBody": str(i)} for i in range(11)]
        self.assertRaises(
            SqsClientError,
            self.sqs_client.send_message_batch,
            without_ids,
            self.queue_name,
        )
