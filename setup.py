import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
VERSION = open(os.path.join(here, 'VERSION.txt')).read()
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='prf',
    version=VERSION,
    description='PRF is designed to help coding RESTful endpoints with minimal code',
    long_description=README,
    long_description_content_type='text/markdown',
    classifiers=[
        "Programming Language :: Python",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
    ],
    author='vahan',
    author_email='aivosha@gmail.com',
    url='',
    keywords='web wsgi bfg pylons pyramid rest',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'paste.app_factory': [
            'main = prf:main',
        ],
        'console_scripts':[
            'prf.mongo_index = prf.scripts.mongo_index:run',
        ]

    },
)
