#!/usr/bin/env python
from setuptools import setup, find_packages


install_requires = [
    'boto>=2.27.0',
    'Beaker>=1.6.4',
]

setup(
    name='dynamodb_beaker',
    version='0.1.1',
    description='DynamoDB backend for Beaker',
    author='xica development team',
    author_email='info@xica.net',
    url='https://github.com/xica/dynamodb-beaker',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Framework :: Pyramid',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: Database',
    ],
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        'beaker.backends': [
            'dynamodb = dynamodb_beaker:DynamoDBNamespaceManager',
        ],
    }
)
