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

import typing
import apache_beam as beam
from apache_beam.pvalue import PCollection

from sqs_pyio.boto3_client import SqsClient
from sqs_pyio.options import SqsOptions

__all__ = ["WriteToSqs"]


class _SqsWriteFn(beam.DoFn):
    """Create the connector can send messages in batch to an Amazon SQS queue.

    Args:
        records (list): Records to send into an Amazon SQS queue.
        queue_name (str): Queue name whose URL must be fetched.
        owner_acc_id (str): AWS account ID where the queue is created.
        max_trials (int): Maximum number of trials to put failed records.
        options (Union[FirehoseOptions, dict]): Options to create a boto3 Firehose client.
    """

    def __init__(
        self,
        queue_name: str,
        owner_acc_id: str,
        max_trials: int,
        options: typing.Union[SqsOptions, dict],
    ):
        """Constructor of _SqsWriteFn

        Args:
            records (list): Records to send into an Amazon SQS queue.
            queue_name (str): Queue name whose URL must be fetched.
            owner_acc_id (str): AWS account ID where the queue is created.
            max_trials (int): Maximum number of trials to put failed records.
            options (Union[FirehoseOptions, dict]): Options to create a boto3 Firehose client.
        """
        super().__init__()
        self.queue_name = queue_name
        self.owner_acc_id = owner_acc_id
        self.max_trials = max_trials
        self.options = options

    def start_bundle(self):
        self.client = SqsClient(self.options)

    def process(self, element):
        if isinstance(element, tuple):
            element = element[1]
        responses = self.client.send_message_batch(
            element, self.queue_name, self.owner_acc_id
        )
        return responses["Failed"]

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
        max_trials (int): Maximum number of trials to put failed records. Defaults to 3.
    """

    def __init__(self, queue_name: str, owner_acc_id: str = None, max_trials: int = 3):
        """Constructor of the transform that puts records into an Amazon Firehose delivery stream

        Args:
            queue_name (str): Amazon SQS queue name.
            owner_acc_id (str, optional): AWS account ID where the queue is created. Defaults to None.
            max_trials (int): Maximum number of trials to put failed records. Defaults to 3.
        """
        super().__init__()
        self.queue_name = queue_name
        self.owner_acc_id = owner_acc_id
        self.max_trials = max_trials

    def expand(self, pcoll: PCollection):
        options = pcoll.pipeline.options.view_as(SqsOptions)
        return pcoll | beam.ParDo(
            _SqsWriteFn(self.queue_name, self.owner_acc_id, self.max_trials, options)
        )
