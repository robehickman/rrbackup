from setuptools import setup

def readme():
    with open('README') as f:
        return f.read()

setup(
    name='rrbackup',
    version='0.1',
    description='Versioning backup system',
    long_description=readme(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: File Management :: File Backup',
    ],
    keywords='file backup',
    url='https://github.com/robehickman/rrbackup',
    author='Robert Hickman',
    author_email='robehickman@gmail.com',
    license='MIT',
    packages=['rrbackup'],
    test_suite='nose.collector',
    tests_require=['nose'],
    install_requires=[
        'termcolor', 'pysodium', 'shttpfs'
    ],
    scripts=['rrbackup.py'],
    zip_safe=False)

