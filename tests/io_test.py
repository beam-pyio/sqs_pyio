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

import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.transforms.util import BatchElements
from apache_beam import GroupIntoBatches
from apache_beam.options import pipeline_options
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from sqs_pyio.boto3_client import SqsClient, SqsClientError
from sqs_pyio.io import WriteToSqs, _SqsWriteFn


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
class TestWriteToSqs(unittest.TestCase):
    queue_name = "test-sqs-queue"

    def setUp(self):
        options = {
            "aws_access_key_id": "testing",
            "aws_secret_access_key": "testing",
            "region_name": "us-east-1",
        }

        self.sqs_client = SqsClient(options)
        create_queue(self.sqs_client, QueueName=self.queue_name)

        self.pipeline_opts = pipeline_options.PipelineOptions(
            [
                "--aws_access_key_id",
                "testing",
                "--aws_secret_access_key",
                "testing",
                "--region_name",
                "us-east-1",
            ]
        )

    def test_write_to_sqs_with_non_existing_queue(self):
        with self.assertRaises(SqsClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (
                    p
                    | beam.Create([[{"Id": "id", "MessageBody": "body"}]])
                    | WriteToSqs(queue_name="non-existing-queue-name")
                )

    def test_write_to_sqs_with_unsupported_record_type(self):
        # only the list type is supported!
        # fails because individual elements (dictionary) are sent to the process function
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
        with self.assertRaises(SqsClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (p | beam.Create(records) | WriteToSqs(queue_name=self.queue_name))

    def test_write_to_sqs_with_list_element(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
        with TestPipeline(options=self.pipeline_opts) as p:
            output = p | beam.Create([records]) | WriteToSqs(queue_name=self.queue_name)
            assert_that(output, equal_to([]))

        messages = receive_message(
            self.sqs_client, queue_name=self.queue_name, max_msgs=3
        )["Messages"]
        assert set([r["MessageBody"] for r in records]) == set(
            [m["Body"] for m in messages]
        )

    def test_write_to_sqs_with_tuple_element(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create([("key", records)])
                | WriteToSqs(queue_name=self.queue_name)
            )
            assert_that(output, equal_to([]))

        messages = receive_message(
            self.sqs_client, queue_name=self.queue_name, max_msgs=3
        )["Messages"]
        assert set([r["MessageBody"] for r in records]) == set(
            [m["Body"] for m in messages]
        )

    def test_write_to_sqs_with_incorrect_message_data_type(self):
        # Id should be string
        records = [{"Id": i, "MessageBody": str(i)} for i in range(3)]
        with self.assertRaises(SqsClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (p | beam.Create([records]) | WriteToSqs(queue_name=self.queue_name))

        # MessageBody should be string
        records = [{"Id": str(i), "MessageBody": i} for i in range(3)]
        with self.assertRaises(SqsClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (p | beam.Create([records]) | WriteToSqs(queue_name=self.queue_name))

    def test_write_to_sqs_with_batch_elements(self):
        # BatchElements groups unkeyed elements into a list
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=2, max_batch_size=2)
                | WriteToSqs(queue_name=self.queue_name)
            )
            assert_that(output, equal_to([]))

        messages = receive_message(
            self.sqs_client, queue_name=self.queue_name, max_msgs=3
        )["Messages"]
        assert set([r["MessageBody"] for r in records]) == set(
            [m["Body"] for m in messages]
        )

    def test_write_to_sqs_with_group_into_batches(self):
        # GroupIntoBatches groups keyed elements into a list
        records = [(i % 2, {"Id": str(i), "MessageBody": str(i)}) for i in range(3)]
        with TestPipeline(options=self.pipeline_opts) as p:
            output = (
                p
                | beam.Create(records)
                | GroupIntoBatches(batch_size=2)
                | WriteToSqs(queue_name=self.queue_name)
            )
            assert_that(output, equal_to([]))

        messages = receive_message(
            self.sqs_client, queue_name=self.queue_name, max_msgs=3
        )["Messages"]
        assert set([r[1]["MessageBody"] for r in records]) == set(
            [m["Body"] for m in messages]
        )


class TestRetryLogic(unittest.TestCase):
    def test_write_to_sqs_retry_no_failed_element(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(4)]
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=4)
                | WriteToSqs(
                    queue_name="test-sqs-queue",
                    max_trials=3,
                    append_error=True,
                    fake_config={"num_success": 2},
                )
            )
            assert_that(output, equal_to([]))

    def test_write_to_sqs_retry_failed_element_without_appending_error(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(4)]
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=4)
                | WriteToSqs(
                    queue_name="test-sqs-queue",
                    max_trials=3,
                    append_error=False,
                    fake_config={"num_success": 1},
                )
            )
            assert_that(
                output,
                equal_to([{"Id": "3", "MessageBody": "3"}]),
            )

    def test_write_to_sqs_retry_failed_element_with_appending_error(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(4)]
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=4)
                | WriteToSqs(
                    queue_name="test-sqs-queue",
                    max_trials=3,
                    append_error=True,
                    fake_config={"num_success": 1},
                )
            )
            assert_that(
                output,
                equal_to(
                    [
                        {
                            "Id": "3",
                            "MessageBody": "3",
                            "error": {
                                "Id": "3",
                                "SenderFault": False,
                                "Code": "error-code",
                                "Message": "error-message",
                            },
                        }
                    ]
                ),
            )


class TestMetrics(unittest.TestCase):
    def test_metrics_with_no_failed_element(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(4)]

        pipeline = TestPipeline()
        output = (
            pipeline
            | beam.Create(records)
            | BatchElements(min_batch_size=4)
            | WriteToSqs(
                queue_name="test-sqs-queue",
                max_trials=3,
                append_error=True,
                fake_config={"num_success": 2},
            )
        )
        assert_that(output, equal_to([]))

        res = pipeline.run()
        res.wait_until_finish()

        ## verify total_elements_count
        metric_results = res.metrics().query(
            MetricsFilter().with_metric(_SqsWriteFn.total_elements_count)
        )
        total_elements_count = metric_results["counters"][0]
        self.assertEqual(total_elements_count.key.metric.name, "total_elements_count")
        self.assertEqual(total_elements_count.committed, 4)

        ## verify succeeded_elements_count
        metric_results = res.metrics().query(
            MetricsFilter().with_metric(_SqsWriteFn.succeeded_elements_count)
        )
        succeeded_elements_count = metric_results["counters"][0]
        self.assertEqual(
            succeeded_elements_count.key.metric.name, "succeeded_elements_count"
        )
        self.assertEqual(succeeded_elements_count.committed, 4)

        ## verify failed_elements_count
        metric_results = res.metrics().query(
            MetricsFilter().with_metric(_SqsWriteFn.failed_elements_count)
        )
        failed_elements_count = metric_results["counters"][0]
        self.assertEqual(failed_elements_count.key.metric.name, "failed_elements_count")
        self.assertEqual(failed_elements_count.committed, 0)

    def test_metrics_with_failed_element(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(4)]

        pipeline = TestPipeline()
        output = (
            pipeline
            | beam.Create(records)
            | BatchElements(min_batch_size=4)
            | WriteToSqs(
                queue_name="test-sqs-queue",
                max_trials=3,
                append_error=False,
                fake_config={"num_success": 1},
            )
        )
        assert_that(
            output,
            equal_to([{"Id": "3", "MessageBody": "3"}]),
        )

        res = pipeline.run()
        res.wait_until_finish()

        ## verify total_elements_count
        metric_results = res.metrics().query(
            MetricsFilter().with_metric(_SqsWriteFn.total_elements_count)
        )
        total_elements_count = metric_results["counters"][0]
        self.assertEqual(total_elements_count.key.metric.name, "total_elements_count")
        self.assertEqual(total_elements_count.committed, 4)

        ## verify succeeded_elements_count
        metric_results = res.metrics().query(
            MetricsFilter().with_metric(_SqsWriteFn.succeeded_elements_count)
        )
        succeeded_elements_count = metric_results["counters"][0]
        self.assertEqual(
            succeeded_elements_count.key.metric.name, "succeeded_elements_count"
        )
        self.assertEqual(succeeded_elements_count.committed, 3)

        ## verify failed_elements_count
        metric_results = res.metrics().query(
            MetricsFilter().with_metric(_SqsWriteFn.failed_elements_count)
        )
        failed_elements_count = metric_results["counters"][0]
        self.assertEqual(failed_elements_count.key.metric.name, "failed_elements_count")
        self.assertEqual(failed_elements_count.committed, 1)
