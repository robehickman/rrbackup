#!/usr/bin/python
from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='rrbackup',
    version='0.1',
    description='Versioning backup system',
    long_description=readme(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: POSIX',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Archiving :: Backup'
    ],
    keywords='file backup',
    url='https://github.com/robehickman/rrbackup',
    author='Robert Hickman',
    author_email='robehickman@gmail.com',
    license='MIT',
    packages=['rrbackup', 'rrbackup.fsutil'],
    test_suite='nose.collector',
    tests_require=['nose'],
    install_requires=[
        'termcolor', 'pysodium'
    ],
    scripts=['cli/rrbackup'],
    zip_safe=False)

