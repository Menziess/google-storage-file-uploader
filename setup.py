from setuptools import setup, find_packages

import json
import os


def open_file(fname):
    return open(os.path.join(os.path.dirname(__file__), fname))


def read_pip_dependencies(fname):
    lockfile = open_file(fname)
    lockjson = json.load(lockfile)
    return [dependency for dependency in lockjson.get('default')]


if __name__ == '__main__':
    setup(
        name='gcloud_uploader',
        version='1.0.dev0',
        package_dir={'': 'src'},
        packages=find_packages('src', include=['gcloud_uploader*']),
        description='Uploading stuff to Google Cloud.',
        install_requires=read_pip_dependencies('Pipfile.lock'),
        entry_points={
            'console_scripts': [
                'upload=gcloud_uploader.uploader:upload'
            ]
        },
        author='menziess',
        author_email='stefan_schenk@hotmail.com'
    )
