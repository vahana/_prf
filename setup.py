import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()
VERSION = open(os.path.join(here, 'VERSION.txt')).read()

install_requires = [
    'pyramid',
    'marshmallow',
    'requests',
    'mongoengine',
    'waitress',
    'slovar',
]

setup(
    name='prf',
    version=VERSION,
    description='prf',
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
    dependency_links=[
        "git+ssh://git@github.com/vahana/slovar.git#egg=slovar"
    ]
)
