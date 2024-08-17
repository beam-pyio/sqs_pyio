# sqs_pyio

![doc](https://github.com/beam-pyio/sqs_pyio/workflows/doc/badge.svg)
![test](https://github.com/beam-pyio/sqs_pyio/workflows/test/badge.svg)
[![release](https://img.shields.io/github/release/beam-pyio/sqs_pyio.svg)](https://github.com/beam-pyio/sqs_pyio/releases)
![pypi](https://img.shields.io/pypi/v/sqs_pyio)
![python](https://img.shields.io/pypi/pyversions/sqs_pyio)

[Amazon Simple Queue Service (Amazon SQS)](https://aws.amazon.com/sqs/) offers a secure, durable, and available hosted queue that lets you integrate and decouple distributed software systems and components. The Apache Beam Python I/O connector for Amazon SQS (`sqs_pyio`) aims to integrate with the queue service by supporting a source and sink connectors. Currently, a sink connector is available.

## Installation

The connector can be installed from PyPI.

```bash
pip install sqs_pyio
```

## Usage

### Sink Connector

It has the main composite transform ([`WriteToSqs`](https://beam-pyio.github.io/sqs_pyio/autoapi/sqs_pyio/io/index.html#sqs_pyio.io.WriteToSqs)), and it expects a list or tuple _PCollection_ element. If the element is a tuple, the tuple's first element is taken. If the element is not of the accepted types, you can apply the [`GroupIntoBatches`](https://beam.apache.org/documentation/transforms/python/aggregation/groupintobatches/) or [`BatchElements`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements) transform beforehand. Then, the element is sent into a SQS queue using the [`send_message_batch`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message_batch.html) method of the boto3 package. Note that the above batch transforms can also be useful to overcome the API limitation listed below.

- Each `SendMessageBatch` request supports up to 10 messages. The maximum allowed individual message size and the maximum total payload size (the sum of the individual lengths of all the batched messages) are both 256 KiB (262,144 bytes).

The transform also has options that handle failed records as listed below.

- _max_trials_ - The maximum number of trials when there is one or more failed records - it defaults to 3. Note that failed records after all trials are returned by a tagged output, which allows users to determine how to handle them subsequently.
- _append_error_ - Whether to append error details to failed records. Defaults to True.

As mentioned earlier, failed elements are returned by a tagged output where it is named as `write-to-sqs-failed-output` by default. You can change the name by specifying a different name using the `failed_output` argument.

#### Sink Connector Example

If a _PCollection_ element is key-value pair (i.e. keyed stream), it can be batched in group using the `GroupIntoBatches` transform before it is connected into the main transform.

```python
import apache_beam as beam
from apache_beam import GroupIntoBatches
from sqs_pyio.io import WriteToSqs

records = [(i % 2, {"Id": str(i), "MessageBody": str(i)}) for i in range(3)]

with beam.Pipeline() as p:
    (
        p
        | beam.Create(records)
        | GroupIntoBatches(batch_size=2)
        | WriteToSqs(queue_name=self.queue_name)
    )
```

For a list element (i.e. unkeyed stream), we can apply the `BatchElements` transform instead.

```python
import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from sqs_pyio.io import WriteToSqs

records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]

with beam.Pipeline() as p:
    (
        p
        | beam.Create(records)
        | BatchElements(min_batch_size=2, max_batch_size=2)
        | WriteToSqs(queue_name=self.queue_name)
    )
```

See [this post](https://beam-pyio.github.io/blog/2024/sqs-pyio-intro/) for more examples.

## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a Code of Conduct. By contributing to this project, you agree to abide by its terms.

## License

`sqs_pyio` was created as part of the [Apache Beam Python I/O Connectors](https://github.com/beam-pyio) project. It is licensed under the terms of the Apache License 2.0 license.

## Credits

`sqs_pyio` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `pyio-cookiecutter` [template](https://github.com/beam-pyio/pyio-cookiecutter).
