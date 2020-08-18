#!/usr/bin/env python3
import argparse
import json
import logging
import os
from time import gmtime, strftime

from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.helpers import streaming_bulk


def main():
    parser = argparse.ArgumentParser(description='Send JSON file to ELK.')
    parser.add_argument('--project-id', required=True, type=int, help="Project ID")
    parser.add_argument('--task-id', required=True, type=str, help="Task ID")
    parser.add_argument('--elk-url', required=True, type=str, help="ELK URL for bulk upload.")
    parser.add_argument('--elk-username', required=True, type=str, help="ELK username")
    parser.add_argument('--elk-password', required=True, type=str, help="ELK password")
    parser.add_argument('--json-file', required=True, type=str, help="JSON file to upload to ES")
    args = parser.parse_args()

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s [%(levelname)s] -- %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=f"/tmp/json_to_elk_task{args.task_id.lstrip('tool_')}.log"
    )

    logging.info(f"Loading input JSON file '{args.json_file}'...")
    with open(args.json_file, 'r') as f:
        vcf_dict = json.load(f)

    biomed_env = os.getenv("BIOMED_ENV", "not-found")

    logging.info(f"Indexing and sending JSON file to ELK...")
    es_client = Elasticsearch(args.elk_url, http_auth=(args.elk_username, args.elk_password))
    logging.info(f"ELK metadata: {es_client.info()}")

    if len(vcf_dict) == 0:
        logging.warning(f"The JSON file is empty, no data will be uploaded to ELK!")

    # More info at parse_commits() and load_repo() methods of:
    # https://github.com/elastic/elasticsearch-py/blob/master/example/load.py#L67-L118
    def yield_indexed_data():
        for idx, variant in enumerate(vcf_dict, start=1):
            index = {
                "_index": f"variants_{biomed_env}_{args.project_id}",
                "_type": "_doc",
                "_id": f"{args.task_id}_{idx}"
            }
            yield {**index, **variant}

    for ok, result in streaming_bulk(es_client, yield_indexed_data()):
        action, result = result.popitem()
        doc_id = f"{result.get('_id', 'NOT_FOUND')}"
        # Check if document has been successfully indexed
        if not ok:
            logging.error(f"Failed to {action} document {doc_id}: {result}")
            raise ElasticsearchException(f"Failed to {action} document {doc_id}: {result}")
        else:
            logging.info(f"Success in '{action}' action. Result: {result}")


if __name__ == "__main__":
    print(f'[{strftime("%Y-%m-%d %H:%M:%S", gmtime())}] Uploading JSON file of variants to ELK...')
    main()
    print(f'[{strftime("%Y-%m-%d %H:%M:%S", gmtime())}] Done.')
