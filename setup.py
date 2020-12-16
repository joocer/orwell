from setuptools import setup, find_packages  # type:ignore

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
   name='orwell',
   version='0.0.15',
   description='Orwell',
   long_description=long_description,
   long_description_content_type="text/markdown",
   author='joocer',
   author_email='justin.joyce@joocer.com',
   packages=find_packages(),
   url="https://github.com/joocer/orwell",
   install_requires=[]
)