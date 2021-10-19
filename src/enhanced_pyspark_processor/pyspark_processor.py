import os

import sagemaker
from sagemaker.local import image


class PySparkProcessor(sagemaker.spark.processing.PySparkProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance_type == "local" and self.instance_count > 1:
            raise ValueError("Instance count must equal to 1 for local mode")

    def run(self, *args, **kwargs):

        if self.instance_type == "local":
            # monkey patch _generate_compose_file with our own function
            orig_generate_compose_file = (
                image._SageMakerContainer._generate_compose_file
            )
            image._SageMakerContainer._generate_compose_file = (
                self._generate_compose_file
            )
            try:
                super().run(*args, **kwargs)
            except Exception as e:
                raise (e)
            finally:
                # undo the patch
                image._SageMakerContainer._generate_compose_file = (
                    orig_generate_compose_file
                )
        else:
            super().run(*args, **kwargs)

    @staticmethod
    def _generate_compose_file(
        self, command, additional_volumes=None, additional_env_vars=None
    ):

        boto_session = self.sagemaker_session.boto_session
        additional_volumes = additional_volumes or []
        additional_env_vars = additional_env_vars or {}
        environment = []
        optml_dirs = set()

        aws_creds = image._aws_credentials(boto_session)
        if aws_creds is not None:
            environment.extend(aws_creds)

        additional_env_var_list = [
            "{}={}".format(k, v) for k, v in additional_env_vars.items()
        ]
        environment.extend(additional_env_var_list)

        if os.environ.get(image.DOCKER_COMPOSE_HTTP_TIMEOUT_ENV) is None:
            os.environ[
                image.DOCKER_COMPOSE_HTTP_TIMEOUT_ENV
            ] = image.DOCKER_COMPOSE_HTTP_TIMEOUT

        if command == "train":
            optml_dirs = {"output", "output/data", "input"}
        elif command == "process":
            optml_dirs = {"output", "config"}

        ####################################################
        # Override entrypoint
        ####################################################
        new_container_entrypoint = [
            "spark-submit",
            "--jars",
            "/usr/lib/hadoop/hadoop-aws.jar,/usr/share/aws/aws-java-sdk/*,/opt/ml/processing/input/jars/*",
            self.container_entrypoint[-1],
        ]
        self.container_entrypoint = new_container_entrypoint
        ####################################################
        ####################################################

        services = {
            h: self._create_docker_host(
                h, environment, optml_dirs, command, additional_volumes
            )
            for h in self.hosts
        }

        content = {
            # Use version 2.3 as a minimum so that we can specify the runtime
            "version": "2.3",
            "services": services,
            "networks": {"sagemaker-local": {"name": "sagemaker-local"}},
        }

        docker_compose_path = os.path.join(
            self.container_root, image.DOCKER_COMPOSE_FILENAME
        )

        try:
            import yaml
        except ImportError as e:
            image.logger.error(
                sagemaker.utils._module_import_error("yaml", "Local mode", "local")
            )
            raise e

        yaml_content = yaml.dump(content, default_flow_style=False)

        # Mask all environment vars for logging, could contain secrects.
        masked_content = image.copy.deepcopy(content)
        for _, service_data in masked_content["services"].items():
            service_data["environment"] = [
                "[Masked]" for _ in service_data["environment"]
            ]

        masked_content_for_logging = yaml.dump(masked_content, default_flow_style=False)
        image.logger.info("docker compose file: \n%s", masked_content_for_logging)
        with open(docker_compose_path, "w") as f:
            f.write(yaml_content)

        return content
