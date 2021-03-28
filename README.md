<img align="centre" alt="VAP" height="64" src="orwell-sm.png" />

**orwell** is a platform to help with authoring data processing systems.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/joocer/orwell/blob/master/LICENSE)


## What Is In It?

orwell.flows -   
orwell.operators - logging routines      
orwell.adapters -   
orwell.data - read data from various sources      
orwell.data.formats - helpers for handling data   
orwell.data.validator - schema conformity testing   



## How Do I Get It?
~~~
pip install --upgrade git+https://github.com/joocer/orwell
~~~


## Features

- Programatically define data pipelines
- Immutable datasets
- On-the-fly compression

## Concepts

### Flows

- **Flow** -
- **Operator** -
- **Run** - 

### Data

- **Dataset** -  
- **Partition** - The files in a dataset are split into chunks of 32Mb and in date formatted folders. 
- **Frame** - Batch data is written into a frame for each execution of the batch. Frames exist as folders with a prefix 'as_at_' indicating the time the batch was run. 

## Dependencies

- **[UltraJSON](https://github.com/ultrajson/ultrajson)** (AKA `ujson`) is used where `orjson` is not available. `orjson` is the preferred JSON library but is not available on all platforms and environments so `ujson` is a dependency to ensure a performant JSON library with broad support is available
- DateUtil
- zstandard 

There are a number of optional dependencies which are required for specific features and functionality. These are listed in the [requirements-optional.txt](requirements-optional.txt) file.
