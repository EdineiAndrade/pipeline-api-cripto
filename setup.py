from setuptools import setup, find_packages

setup(
    name="pipeline_dados",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'requests',
        'sqlalchemy',
        'python-dotenv'
    ],
)