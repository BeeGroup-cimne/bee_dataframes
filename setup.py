from setuptools import setup, find_packages

setup(
    name='bee_data_utils',
    version='0.0.1',
    packages=find_packages(),
    url='https://github.com/BeeGroup-cimne/bee_data_cleaning',
    license='MIT',
    author='BEE Group - CIMNE',
    author_email='egabaldon@cimne.upc.edu',
    install_requires=[
        'pandas',
        'numpy',
        'pymongo',
    ],
    description='Utilities to obtain data from beedata mongo.',
    test_suite='nose.collector',
    tests_require=['nose'],
)