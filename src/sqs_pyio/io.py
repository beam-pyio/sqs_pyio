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

import logging
import typing
import apache_beam as beam
from apache_beam import metrics
from apache_beam.pvalue import PCollection, TaggedOutput

from sqs_pyio.boto3_client import SqsClient, FakeSqsClient
from sqs_pyio.options import SqsOptions

__all__ = ["WriteToSqs"]


class _SqsWriteFn(beam.DoFn):
    """Create the connector can send messages in batch to an Amazon SQS queue.

    Args:
        records (list): Records to send into an Amazon SQS queue.
        queue_name (str): Queue name whose URL must be fetched.
        owner_acc_id (str): AWS account ID where the queue is created.
        max_trials (int): Maximum number of trials to put failed records.
        append_error (bool): Whether to append error details to failed records.
        failed_output (str): A tagged output name where failed records are written to.
        options (Union[SqsOptions, dict]): Options to create a boto3 SQS client.
        fake_config (dict, optional): Config parameters when using FakeSqsClient for testing.
    """

    total_elements_count = metrics.Metrics.counter(
        "_SqsWriteFn", "total_elements_count"
    )
    succeeded_elements_count = metrics.Metrics.counter(
        "_SqsWriteFn", "succeeded_elements_count"
    )
    failed_elements_count = metrics.Metrics.counter(
        "_SqsWriteFn", "failed_elements_count"
    )

    def __init__(
        self,
        queue_name: str,
        owner_acc_id: str,
        max_trials: int,
        append_error: bool,
        failed_output: str,
        options: typing.Union[SqsOptions, dict],
        fake_config: dict,
    ):
        """Constructor of _SqsWriteFn

        Args:
            records (list): Records to send into an Amazon SQS queue.
            queue_name (str): Queue name whose URL must be fetched.
            owner_acc_id (str): AWS account ID where the queue is created.
            max_trials (int): Maximum number of trials to put failed records.
            append_error (bool): Whether to append error details to failed records.
            failed_output (str): A tagged output name where failed records are written to.
            options (Union[SqsOptions, dict]): Options to create a boto3 SQS client.
            fake_config (dict): Config parameters when using FakeSqsClient for testing.
        """
        super().__init__()
        self.queue_name = queue_name
        self.owner_acc_id = owner_acc_id
        self.max_trials = max_trials
        self.append_error = append_error
        self.failed_output = failed_output
        self.options = options
        self.fake_config = fake_config

    def start_bundle(self):
        if not self.fake_config:
            self.client = SqsClient(self.options)
        else:
            self.client = FakeSqsClient(self.fake_config)

    def process(self, element):
        if isinstance(element, tuple):
            element = element[1]
        loop, total, failed = 0, len(element), []
        while loop < self.max_trials:
            response = self.client.send_message_batch(
                element, self.queue_name, self.owner_acc_id
            )
            failed = response.get("Failed", [])
            if len(failed) == 0 or (self.max_trials - loop == 1):
                break
            element = [e for e in element if e["Id"] in [r["Id"] for r in failed]]
            failed = []
            loop += 1
        self.total_elements_count.inc(total)
        self.succeeded_elements_count.inc(total - len(failed))
        self.failed_elements_count.inc(len(failed))
        logging.info(
            f"total {total}, succeeded {total - len(failed)}, failed {len(failed)}..."
        )
        if len(failed) > 0:
            failed_records = [
                e for e in element if e["Id"] in [r["Id"] for r in failed]
            ]
            if self.append_error:
                failed_records = zip(failed_records, failed)
            for r in failed_records:
                yield TaggedOutput(self.failed_output, r)

    def finish_bundle(self):
        self.client.close()


class WriteToSqs(beam.PTransform):
    """A transform that sends messages in batch into an Amazon SQS queue.

    Takes an input PCollection and put them in batch using the boto3 package.
    For more information, visit the `Boto3 Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message_batch.html>`__.

    Note that, if the PCollection element is a tuple (i.e. keyed stream), only the value is used to send messages in batch.

    Args:
        queue_name (str): Amazon SQS queue name.
        owner_acc_id (str, optional): AWS account ID where the queue is created. Defaults to None.
        max_trials (int, optional): Maximum number of trials to put failed records. Defaults to 3.
        append_error (bool, optional): Whether to append error details to failed records. Defaults to True.
        failed_output (str, optional): A tagged output name where failed records are written to. Defaults to 'write-to-sqs-failed-output'.
        fake_config (dict, optional): Config parameters when using FakeFirehoseClient for testing. Defaults to {}.
    """

    def __init__(
        self,
        queue_name: str,
        owner_acc_id: str = None,
        max_trials: int = 3,
        append_error: bool = True,
        failed_output: str = "write-to-sqs-failed-output",
        fake_config: dict = {},
    ):
        """Constructor of the transform that puts records into an Amazon Firehose delivery stream

        Args:
            queue_name (str): Amazon SQS queue name.
            owner_acc_id (str, optional): AWS account ID where the queue is created. Defaults to None.
            max_trials (int, optional): Maximum number of trials to put failed records. Defaults to 3.
            append_error (bool, optional): Whether to append error details to failed records. Defaults to True.
            failed_output (str, optional): A tagged output name where failed records are written to. Defaults to 'write-to-sqs-failed-output'.
            fake_config (dict, optional): Config parameters when using FakeFirehoseClient for testing. Defaults to {}.
        """
        super().__init__()
        self.queue_name = queue_name
        self.owner_acc_id = owner_acc_id
        self.max_trials = max_trials
        self.append_error = append_error
        self.failed_output = failed_output
        self.fake_config = fake_config

    def expand(self, pcoll: PCollection):
        options = pcoll.pipeline.options.view_as(SqsOptions)
        return pcoll | beam.ParDo(
            _SqsWriteFn(
                self.queue_name,
                self.owner_acc_id,
                self.max_trials,
                self.append_error,
                self.failed_output,
                options,
                self.fake_config,
            )
        ).with_outputs(self.failed_output, main=None)
