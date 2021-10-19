from setuptools import find_packages, setup

setup(
    name="enhanced-pyspark-processor",
    version="0.1",
    description="Extends PySparkProcessor with local mode capabilities",
    url="https://gitlab.aws.dev/ai-ml-specialist-sa/amer-aiml-sa/tech-sector/examples/sm-processing-pyspark-local-mode",
    author="Bobby Lindsey, Tony Chen",
    author_email="bwlind@amazon.com, tkchen@amazon.com",
    license="Apache-2.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
)
