import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()
VERSION = open(os.path.join(here, 'VERSION.txt')).read()

install_requires = [
]

setup(
    name='prf',
    version=VERSION,
    description='Pyramid RESTful Framework is designed to help coding REST CRUD endpoints with couple of lines of code.',
    long_description=README + '\n\n' +  CHANGES,
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
    install_requires=install_requires,
    entry_points={
        'paste.app_factory': [
            'main = prf:main',
        ],
        'console_scripts':[
            'prf.mongo_index = prf.scripts.mongo_index:run'
        ]

    },
)
