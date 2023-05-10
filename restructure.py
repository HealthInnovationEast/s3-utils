#!/usr/bin/env python

import boto3
import botocore
import fire
import configparser
import os
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
import logging
import sys
import math
from datetime import datetime

MB = 1024*1024
S3_DEFAULT_PART = 8 * MB
S3_MAX_PARTS = 10000
S3_DEFAULT_MAX = S3_DEFAULT_PART * S3_MAX_PARTS
TMP_DL_FILE = '/tmp/sync_tmp_file'
CPUS = os.cpu_count()

client_config = botocore.config.Config(
    max_pool_connections=CPUS,
)

def log_config(level: str):
    logging.basicConfig(level=getattr(logging, level))

def get_client(purpose: str, account_sec):
    return boto3.client(
        purpose,
        region_name=account_sec["region"],
        aws_access_key_id=account_sec["access_key"],
        aws_secret_access_key=account_sec["secret_key"],
        config=client_config
    )

def load_conf(config_file: str) -> Dict[str, Any]:
    config = configparser.ConfigParser()
    config.read(config_file)
    # get s3 clients for both from and to options
    clients = {}
    for s in config.sections():
        clients[s] = get_client("s3", config[s])
    return clients

def extract_bucket(s3_path:str):
    if s3_path.startswith("s3://") is not True:
        raise ValueError("*_S3 must include s3:// prefix")
    clean = s3_path.replace("s3://", "")
    (bucket, *rest) = clean.split("/")
    prefix = "/".join(rest)
    if prefix.endswith("/") is False:
        prefix = prefix + "/"
    return (bucket, prefix)

def load_restructure(from_to:str, modulus:int, remainder:int) -> List[Dict[str,str]]:
    to_migrate = []
    with open(from_to, 'rt') as f:
        lc = 0
        for line in f:
            lc += 1
            (source, dest) = line.strip().split(",")
            if lc % modulus != remainder:
                logging.info(f"modulo skip: {lc} -> {source}")
                continue
            to_migrate.append({'source': source, 'dest': dest})
    return to_migrate

def obj_info(s3, bucket, key):
    logging.info(f"list_objects_v2(Bucket={bucket}, Prefix={key})")
    bkt_resp = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    response = None
    if bkt_resp["KeyCount"] > 0:
        for obj in bkt_resp["Contents"]:
            if obj["Key"] == key:
                response = obj
    return response

def bucket_key_from_uri(object_uri:str) -> List[str]:
    clean = object_uri.replace("s3://", "")
    (bkt, *rest) = clean.split("/")
    key = "/".join(rest)
    return bkt, key

def transfer_conf(size):
    trans_conf = None
    part_size = S3_DEFAULT_PART
    if size > S3_DEFAULT_MAX:
        ## generates a value scaled to next MB
        part_size = math.ceil(size / 10000 / MB) * MB

    trans_conf = boto3.s3.transfer.TransferConfig(
        multipart_threshold=part_size,
        multipart_chunksize=part_size,
        max_concurrency=CPUS,
        use_threads=True
    )
    return trans_conf

def calc_transfer_speed(start:float, end:float, size_bytes):
    return math.floor((size_bytes / MB) / (end-start))

def sync_files(clients:Dict[str,Any], to_migrate:List[Dict[str,str]], write:bool, storage_class:bool):
    source_client = clients["FROM"]
    dest_client = clients["TO"]

    dl_t_cfg = boto3.s3.transfer.TransferConfig(
        max_concurrency=CPUS,
        use_threads=True
    )

    up_extra_args = None
    if storage_class is not None:
        up_extra_args = {"StorageClass": storage_class}

    for item in to_migrate:
        source = item['source']
        transfer_item = {'is_s3': False}
        if source.startswith("s3://"):
            ## evaluate S3 object presence, on both ends and compare
            (bkt, key) = bucket_key_from_uri(source)
            src_obj = obj_info(source_client, bkt, key)
            if src_obj is None:
                logging.error(f"Source file not found: {source} (bkt: {bkt}, key: {key})")
                sys.exit(1)
            transfer_item['is_s3'] = True
            transfer_item['src_bucket'] = bkt
            transfer_item['src_key'] = key
            transfer_item['src_size'] = src_obj['ObjectSize']
        elif os.path.isfile(source):
            ## evaluate local file and check for presence at dest
            transfer_item['src_path'] = source
            transfer_item['src_size'] = os.path.getsize(source)
        else:
            logging.error(f"File not locally or with s3:// prefix: {source}")
            sys.exit(1)

        # dest is always S3
        dest = item['dest']
        (dest_bkt, dest_key) = bucket_key_from_uri(dest)
        dest_obj = obj_info(dest_client, dest_bkt, dest_key)
        if dest_obj is not None:
            if dest_obj['ObjectSize'] == transfer_item['src_size']:
                logging.warn(f"File already transferred: {source} -> {dest}")

        ### Then actually do a file transfer

        if write is False:
            logging.info("Skipping file transfers due to: write=False")
            continue

        if transfer_item['is_s3'] is True:
            ## here we make the file local if not already
            logging.info(f"Downloading: {src}")
            if os.path.exists(TMP_DL_FILE):
                os.remove(TMP_DL_FILE)

            start = datetime.now().timestamp()
            source_client.download_file(transfer_item['src_bucket'], transfer_item['src_key'], TMP_DL_FILE, Config=dl_t_cfg)
            end = datetime.now().timestamp()
            logging.info(f"Download MBs/s \t {calc_transfer_speed(start, end, transfer_item['src_size'])} <- {transfer_item['src_bucket']}/{transfer_item['src_key']}")

            transfer_item['src_path'] = TMP_DL_FILE
            dl_size = os.path.getsize(TMP_DL_FILE)
            if transfer_item['src_size'] != dl_size:
                logging.error(f"Downloaded file has different size, {transfer_item['src_size']} vs {dl_size}, {source} vs {TMP_DL_FILE}")
                sys.exit(1)

        ### Now copy to destination
        trans_conf = transfer_conf(transfer_item['src_size'])

        start = datetime.now().timestamp()
        logging.info(f"upload_file({dest_bkt}, {dest_key})")
        dest_client.upload_file(transfer_item['src_path'], dest_bkt, dest_key, ExtraArgs=up_extra_args, Config=trans_conf)
        end = datetime.now().timestamp()
        logging.info(f"Upload MBs/s \t {calc_transfer_speed(start, end, transfer_item['src_size'])} -> {dest_bkt}/{dest_key}")

def run(config:str, from_to:str, write:bool=False, allow_multipart:bool=False, storage_class:Optional[str]=None, modulus:int=1, remainder:int=0, loglevel:str="WARNING"):
    """
    !! Do not use outside of AWS on large data, egress/firewall charges !!

    Used to restructure data in AWS, potentially between credential sets, local and S3 source files supported.

    Positional arguments

        CONFIG:
            AWS account details for source (FROM) and destination (TO)
            accounts, see example demo.conf

        FROM_TO:
            CSV file mapping source and destination location, allows s3 or local file as source

            s3://some-bucket/key/file.ext,s3://some-other-bucket/diff-structure/file.ext
            local/file.ext,s3://some-other-bucket/was-local/file.ext

    Flags - short/long form detailed at end of help

        write
            By default no data transfer will be performed.

        allow_multipart
            When not specified all files over 8 MB are ignored, useful for initial testing

        storage_class
            When supported allows you to direct files into a particular storage class to reduce costs:
            STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING | GLACIER |
            DEEP_ARCHIVE | GLACIER_IR. Defaults to 'STANDARD' or bucket policy.

        modulus
            ! Use with remainder !
            Allows you to split workload for frozen source bucket into even sized chunks for parallel processing
            on different hosts.

            Specify the number of instances you will execute i.e. 4

        remainder
            ! Use with modulus !
            Specify a value of 0-(modulus-1) for this invocation of the script.

        loglevel
            DEBUG|INFO|WARNING|ERROR|CRITICAL

    Other:

    It is possible to use this to transfer data within an account, however be concious that it will not delete data
    so storage costs will apply.
    """
    if remainder < 0 or remainder >= modulus:
        logging.error(f"Option --remainder ({remainder}) must be less than --modulus ({modulus}) (and >= 0)")
        sys.exit(1)

    loglevel = loglevel.upper()

    log_config(loglevel)
    clients = load_conf(config)
    to_migrate = load_restructure(from_to, modulus, remainder)
    sync_files(clients, to_migrate, write, storage_class)
    # def sync_files(clients:Dict[str,Any], to_migrate:List[Dict[str,str]], write:bool):


if __name__ == '__main__':
  fire.Fire(run)
