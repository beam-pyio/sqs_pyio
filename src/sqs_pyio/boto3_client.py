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
import boto3
from apache_beam.options import pipeline_options

from sqs_pyio.options import SqsOptions


__all__ = ["SqsClient", "SqsClientError"]


def get_http_error_code(exc):
    if hasattr(exc, "response"):
        return exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return None


class SqsClientError(Exception):
    def __init__(self, message=None, code=None):
        self.message = message
        self.code = code


class SqsClient(object):
    """
    Wrapper for boto3 library.
    """

    def __init__(self, options: typing.Union[SqsOptions, dict]):
        """Constructor of the SqsClient.

        Args:
            options (Union[SqsOptions, dict]): Options to create a boto3 SQS client.
        """
        assert boto3 is not None, "Missing boto3 requirement"
        if isinstance(options, pipeline_options.PipelineOptions):
            options = options.view_as(SqsOptions)
            access_key_id = options.aws_access_key_id
            secret_access_key = options.aws_secret_access_key
            session_token = options.aws_session_token
            endpoint_url = options.endpoint_url
            use_ssl = not options.disable_ssl
            region_name = options.region_name
            api_version = options.api_version
            verify = options.verify
        else:
            access_key_id = options.get("aws_access_key_id")
            secret_access_key = options.get("aws_secret_access_key")
            session_token = options.get("aws_session_token")
            endpoint_url = options.get("endpoint_url")
            use_ssl = not options.get("disable_ssl", False)
            region_name = options.get("region_name")
            api_version = options.get("api_version")
            verify = options.get("verify")

        session = boto3.session.Session()
        self.client = session.client(
            service_name="sqs",
            region_name=region_name,
            api_version=api_version,
            use_ssl=use_ssl,
            verify=verify,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
        )

    def get_queue_url(self, queue_name: str, owner_acc_id: str = None):
        """Returns the URL of an Amazon SQS queue

        Args:
            queue_name (str): Queue name whose URL must be fetched.
            owner_acc_id (str, optional): AWS account ID where the queue is created. Defaults to None.

        Raises:
            SqsClientError: SQS client error.

        Returns:
            (str): URL of the queue.
        """
        try:
            params = {"QueueName": queue_name}
            if owner_acc_id is not None:
                params["QueueOwnerAWSAccountId"] = owner_acc_id
            boto_response = self.client.get_queue_url(**params)
            return boto_response["QueueUrl"]
        except Exception as e:
            raise SqsClientError(str(e), get_http_error_code(e))

    def send_message_batch(
        self, records: list, queue_name: str, owner_acc_id: str = None
    ):
        """Send messages to an Amazon SQS queue in batch.

        Args:
            records (list): Records to send into an Amazon SQS queue.
            queue_name (str): Queue name whose URL must be fetched.
            owner_acc_id (str, optional): AWS account ID where the queue is created. Defaults to None.

        Raises:
            SqsClientError: SQS client error.

        Returns:
            (Object): Boto3 response message.
        """

        if not isinstance(records, list):
            raise SqsClientError("Records should be a list.")
        try:
            queue_url = self.get_queue_url(queue_name, owner_acc_id)
            boto_response = self.client.send_message_batch(
                QueueUrl=queue_url, Entries=records
            )
            return boto_response
        except Exception as e:
            raise SqsClientError(str(e), get_http_error_code(e))

    def close(self):
        """Closes underlying endpoint connections."""
        self.client.close()


class FakeSqsClient:
    def __init__(self, fake_config: dict):
        self.num_success = fake_config.get("num_success", 0)

    def get_queue_url(self, queue_name: str, owner_acc_id: str = None):
        return "fake-queue-url"

    def send_message_batch(
        self, records: list, queue_name: str, owner_acc_id: str = None
    ):
        if not isinstance(records, list):
            raise SqsClientError("Records should be a list.")

        successful, failed = [], []
        for index, record in enumerate(records):
            if index < self.num_success:
                successful.append({"Id": record["Id"]})
            else:
                failed.append(
                    {
                        "Id": record["Id"],
                        "SenderFault": False,
                        "Code": "error-code",
                        "Message": "error-message",
                    }
                )
        return {"Successful": successful, "Failed": failed}

    def close(self):
        return
