## Background

Currently, local-mode does not work for `PySparkProcessor` due to YARN not being configured correctly for local setups. To enable local development, we created an enhanced version of the `PySparkProcessor` which overrides the underlying functionality of the SageMaker SDK, and runs Spark in local mode rather than using YARN. This enhanced version also preserves the interface that exists with the original `PySparkProcessor`. It's important to note that this project should serve only as a stop-gap solution (until local-mode is natively supported in SageMaker SDK).

## Getting Started

To install:

```bash
pip install git+https://github.com/aws-samples/enhanced-pyspark-processor
```

Please refer to the [notebook example](examples/example.ipynb) for usage patterns.

## Compatability

The following versions have been tested for compatibility.

| SageMaker SDK                    | Spark | Compatible?        |
| -------------------------------- | ----- | ------------------ |
| `sagemaker >= 2.22.0, <= 2.61.0` | `2.4` | :heavy_check_mark: |
| `sagemaker >= 2.22.0, <= 2.61.0` | `3.0` | :heavy_check_mark: |

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
