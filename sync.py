#!/usr/bin/env python

import boto3
import fire
import configparser
from typing import Any
from typing import Dict
from typing import Optional
import logging
import math
import sys
import os

MB = 1024*1024
TMP_DL_FILE = '/tmp/sync_tmp_file'

logging.basicConfig(level=getattr(logging, "INFO"))

def get_client(purpose: str, account_sec):
    return boto3.client(
        purpose,
        region_name=account_sec["region"],
        aws_access_key_id=account_sec["access_key"],
        aws_secret_access_key=account_sec["secret_key"],
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


def list_s3(clients, location:str, url:str, allow_multipart:bool):
    bucket, prefix = extract_bucket(url)
    s3 = clients[location]
    s3_set = {}
    bkt_resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    while True:
        for f in bkt_resp["Contents"]:
            if f["Key"].endswith("/"): continue

            attr_resp = s3.get_object_attributes(
                Bucket=bucket,
                Key=f["Key"],
                ObjectAttributes=['ObjectParts','ObjectSize']
            )

            part_size = None
            if "ObjectParts" in attr_resp:
                # ceil then rescale back to bytes
                part_size = math.ceil(attr_resp["ObjectSize"] / attr_resp["ObjectParts"]["TotalPartsCount"] / MB) * MB

            if part_size is not None:
                if allow_multipart == False:
                    logging.warning(f"Multipart, skipping: {f['Key']} (see --help)")
                    continue

            s3_set[f["Key"]] = {"ETag": f["ETag"], "PartSize" : part_size}
        if bkt_resp["IsTruncated"] is False:
            break
        else:
            bkt_resp = s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=bkt_resp["NextContinuationToken"]
            )
    return bucket, prefix, s3_set

def obj_info(s3, bucket, key):
    bkt_resp = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    response = None
    if bkt_resp["KeyCount"] > 0:
        for obj in bkt_resp["Contents"]:
            if obj["Key"] == key:
                response = obj
    return response

def transfer_objects(clients:Dict[str,Any], src_bkt:str, src_prefix:str, src_objects:Dict[str,str], dest_url:str, storage_class: Optional[str]):
    issue_list = []
    bucket, prefix = extract_bucket(dest_url)
    source_client = clients["FROM"]
    dest_client = clients["TO"]
    up_extra_args = None
    if storage_class is not None:
        up_extra_args = {"StorageClass": storage_class}

    for src, src_obj in src_objects.items():
        # dict content to vars
        chk = src_obj["ETag"]
        part_size = src_obj["PartSize"]

        target = src.replace(src_prefix, prefix)
        target_obj = obj_info(dest_client, bucket, target)
        if target_obj is not None:
            if target_obj["ETag"] == chk:
                logging.info(f"Skipping as found with matching ETag: {src}")
                continue
            logging.warning(f"Redoing as mismatched ETag: {src}")
        # download the file
        logging.info(f"Downloading: {src}")
        if os.path.exists(TMP_DL_FILE):
            os.remove(TMP_DL_FILE)
        source_client.download_file(src_bkt, src, TMP_DL_FILE)
        # upload the file
        logging.info(f"Uploading: {target}")

        trans_conf = None
        if part_size is not None:
            trans_conf = boto3.s3.transfer.TransferConfig(
                multipart_threshold=part_size,
                multipart_chunksize=part_size,
                max_concurrency=10,
                use_threads=True
            )
        dest_client.upload_file('sync_tmp_file', bucket, target, ExtraArgs=up_extra_args, Config=trans_conf)
        target_obj = obj_info(dest_client, bucket, target)
        if target_obj["ETag"] != chk:
            issue_list.append(f"ETag mismatch: {src_bkt}/{src} : {bucket}/{target}")
    if os.path.exists(TMP_DL_FILE):
        os.remove(TMP_DL_FILE)
    if len(issue_list) > 0:
        for i in issue_list:
            print(i, file=sys.stderr)
        sys.exit(1)




def run(config:str, source_s3:str, dest_s3:str, allow_multipart:bool=False, storage_class:Optional[str]=None):
    """
    Used to synchronise data between buckets from different AWS accounts.
    !! Do not use outside of AWS on large data, egress charges !!

    Positional arguments

        CONFIG:
            AWS account details for source (FROM) and destination (TO)
            accounts, see example demo.conf

        SOURCE_S3:
            The Bucket path to replicate, this can point to a sub-path within a bucket.
            Must include 's3://' prefix.

        DEST_S3
            The Bucket path to deposit data at, this can point to a sub-path within a bucket.
            Must include 's3://' prefix.

    Flags - short/long form detailed at end of help

        allow_multipart
            When not specified all files over 8 MB are ignored, useful for initial testing

        storage_class
            When supported allows you to direct files into a particular storage class to reduce costs:
            STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING | GLACIER |
            DEEP_ARCHIVE | GLACIER_IR. Defaults to 'STANDARD'

    Other:

    It is possible to use this to transfer data within an account, however be concious that it will not delete data
    so storage costs will apply.
    """
    clients = load_conf(config)
    src_bkt, src_prefix, src_objects = list_s3(clients, "FROM", source_s3, allow_multipart)
    transfer_objects(clients, src_bkt, src_prefix, src_objects, dest_s3, storage_class)

if __name__ == '__main__':
  fire.Fire(run)
