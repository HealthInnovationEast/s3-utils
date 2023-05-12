# s3-utils <!-- omit in toc -->

Scripts used when manipulating S3 data.

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

- [Preparation](#preparation)
- [Scripts](#scripts)
  - [`sync.py`](#syncpy)
    - [Parallel use](#parallel-use)

## Preparation

```
python -m venv .venv
source .venv/*/activate
pip install -r requirements.txt
```

## Scripts

### `sync.py`

Used to replicate data between S3 accounts where you do not have easy access to IAM / policies to do this with `aws-cli`
or `s3cmd`.

See command line help for usage:

```
./sync.py -h
```

#### Parallel use

It is possible to use the script on multiple nodes of compute using the `--modulus` and `--remainder` options.
