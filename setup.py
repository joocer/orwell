from setuptools import setup, find_packages  # type:ignore

exec(open('orwell/version.py').read())  # nosec

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
   name='orwell',
   version=__version__,
   description='Python Data Libraries',
   long_description=long_description,
   long_description_content_type="text/markdown",
   maintainer='Joocer',
   packages=find_packages(include=['orwell', 'orwell.*']),
   url="https://github.com/joocer/orwell/",
   install_requires=[
        'ujson',
        'python-dateutil',
        'zstandard'
   ]
)
