#!/usr/bin/env python

import boto3
import fire
import configparser
from typing import Any
from typing import Dict
# from pprint import pprint
import logging

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


def list_s3(clients, location:str, url:str):
    bucket, prefix = extract_bucket(url)
    s3 = clients[location]
    s3_set = {}
    bkt_resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    while True:
        for f in bkt_resp["Contents"]:
            if f["Key"].endswith("/"): continue
            if f["Key"].endswith("md5") is False:
                logging.warning(f"only keeping md5, skipping: {f['Key']}")
                continue
            s3_set[f["Key"]] = f["ETag"]
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

def transfer_objects(clients:Dict[str,Any], src_bkt:str, src_prefix:str, src_objects:Dict[str,str], dest_url:str):
    bucket, prefix = extract_bucket(dest_url)
    source_client = clients["FROM"]
    dest_client = clients["TO"]
    for src, chk in src_objects.items():
        target = src.replace(src_prefix, prefix)
        target_obj = obj_info(dest_client, bucket, target)
        if target_obj is not None:
            if target_obj["ETag"] == chk:
                logging.info(f"Skipping as found with matching ETag: {src}")
                continue
            logging.warning(f"Redoing as mismatched ETag: {src}")
        # download the file
        logging.info(f"Downloading: {src}")
        source_client.download_file(src_bkt, src, 'sync_tmp_file')
        # upload the file
        logging.info(f"Uploading: {target}")
        dest_client.upload_file('sync_tmp_file', bucket, target)




def run(config, source_s3, dest_s3):
    clients = load_conf(config)
    src_bkt, src_prefix, src_objects = list_s3(clients, "FROM", source_s3)
    transfer_objects(clients, src_bkt, src_prefix, src_objects, dest_s3)

if __name__ == '__main__':
  fire.Fire(run)
