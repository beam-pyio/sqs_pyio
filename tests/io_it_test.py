import unittest
import pytest
import docker
import boto3
import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam import GroupIntoBatches
from apache_beam.options import pipeline_options
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from localstack_utils.localstack import startup_localstack, stop_localstack

from sqs_pyio.io import WriteToSqs


def create_client(service_name):
    return boto3.client(
        service_name=service_name,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        endpoint_url="http://localhost:4566",
    )


def create_queue(**kwargs):
    sqs_client = create_client("sqs")
    sqs_client.create_queue(**kwargs)


def receive_message(**kwargs):
    sqs_client = create_client("sqs")
    queue_url = sqs_client.get_queue_url(QueueName=kwargs["queue_name"])["QueueUrl"]
    return sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=kwargs["max_msgs"]
    )


@pytest.mark.integration
class TestWriteToSqs(unittest.TestCase):
    queue_name = "test-sqs-queue"

    def setUp(self):
        startup_localstack()
        ## create resources
        create_queue(QueueName=self.queue_name)

        self.pipeline_opts = pipeline_options.PipelineOptions(
            [
                "--runner",
                "FlinkRunner",
                "--parallelism",
                "1",
                "--aws_access_key_id",
                "testing",
                "--aws_secret_access_key",
                "testing",
                "--aws_access_key_id",
                "testing",
                "--region_name",
                "us-east-1",
                "--endpoint_url",
                "http://localhost:4566",
            ]
        )

    def tearDown(self):
        stop_localstack()
        docker_client = docker.from_env()
        docker_client.containers.prune()
        return super().tearDown()

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
            assert_that(output[None], equal_to([]))

        messages = receive_message(queue_name=self.queue_name, max_msgs=3)["Messages"]
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
            assert_that(output[None], equal_to([]))

        messages = receive_message(queue_name=self.queue_name, max_msgs=3)["Messages"]
        assert set([r[1]["MessageBody"] for r in records]) == set(
            [m["Body"] for m in messages]
        )

    def test_write_to_sqs_with_too_many_elements(self):
        # BatchElements groups unkeyed elements into a list
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(100)]
        with self.assertRaises(RuntimeError):  # RuntimeError with a portable runner!
            with TestPipeline(options=self.pipeline_opts) as p:
                (
                    p
                    | beam.Create(records)
                    | BatchElements(min_batch_size=50)
                    | WriteToSqs(queue_name=self.queue_name)
                )
