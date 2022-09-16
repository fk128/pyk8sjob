#!/usr/bin/env python

from setuptools import setup

with open("requirements.txt") as requirements_file:
    requirements = requirements_file.read().splitlines()

with open("requirements_dev.txt") as requirements_file:
    requirements_dev = requirements_file.read().splitlines()

setup(
    name="pyk8sjob",
    version="0.1.0",
    description="Run python scripts as jobs on k8s",
    author="F. K.",
    keywords="",
    license="Apache License 2.0",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "pyk8sjob=pyk8sjob.cli:app",
        ],
    },
     extras_require={
        "dev": requirements_dev,
    },
)
