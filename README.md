# s3-utils <!-- omit in toc -->

Scripts used when manipulating S3 data.

- [Preparation](#preparation)
- [Scripts](#scripts)
  - [`sync.py`](#syncpy)

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
