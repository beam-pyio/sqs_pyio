# Changelog

<!--next-version-placeholder-->

## v0.1.0 (22/08/2024)

✨NEW

- Add a composite transform (`WriteToSqs`) that sends messages to a SQS queue in batch, using the [`send_message_batch`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message_batch.html) method of the boto3 package.
- Provide options that handle failed records.
  - _max_trials_ - The maximum number of trials when there is one or more failed records.
  - _append_error_ - Whether to append error details to failed records.
- Return failed elements by a tagged output, which allows users to determine how to handle them subsequently.
- Create a dedicated pipeline option (`SqsOptions`) that reads AWS related values (e.g. `aws_access_key_id`) from pipeline arguments.
- Implement metric objects that record the total, succeeded and failed elements counts.
- Add unit and integration testing cases. The [moto](https://github.com/getmoto/moto) and [localstack-utils](https://docs.localstack.cloud/user-guide/tools/testing-utils/) are used for unit and integration testing respectively. Also, a custom test client is created for testing retry behavior, which is not supported by the moto package.
- Integrate with GitHub Actions by adding workflows for testing, documentation and release management.
