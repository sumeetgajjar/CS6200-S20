#!/bin/bash

set -e

redis-cli set MAX_URLS_TO_CRAWL 60000
redis-cli set URL_PROCESSOR_BATCH_SIZE 10

redis-cli del CRAWLED_URLS_BF TOTAL_URLS_CRAWLED

for queue in $(redis-cli keys "QUEUES::*");
do
    redis-cli del ${queue}
done
