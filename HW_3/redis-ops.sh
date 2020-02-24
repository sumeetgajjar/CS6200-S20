#!/usr/bin/env bash

set -e

MAX_URLS_TO_CRAWL_KEY='MAX_URLS_TO_CRAWL'
URL_PROCESSOR_BATCH_SIZE_KEY='URL_PROCESSOR_BATCH_SIZE'
CRAWLED_URLS_BF_KEY='CRAWLED_URLS_BF'
TOTAL_URLS_CRAWLED_KEY='TOTAL_URLS_CRAWLED'
QUEUES_PREFIX='QUEUES::*'
FRONTIER_QUEUE='QUEUES::FRONTIER'
SEED_URLS=("http://en.wikipedia.org/wiki/American_Revolutionary_War" \
            "http://www.history.com/topics/american-revolution/american-revolution-history" \
            "http://en.wikipedia.org/wiki/American_Revolution" \
            "http://www.revolutionary-war.net/causes-of-the-american-revolution.html")


function refresh() {
    redis-cli set ${MAX_URLS_TO_CRAWL_KEY} 60000
    redis-cli set ${URL_PROCESSOR_BATCH_SIZE_KEY} 10

    redis-cli del ${CRAWLED_URLS_BF_KEY} ${TOTAL_URLS_CRAWLED_KEY}

    for QUEUE in $(redis-cli keys ${QUEUES_PREFIX});
    do
        redis-cli del ${QUEUE}
    done
}

function status() {
    echo "Max urls to crawl -> $(redis-cli get ${MAX_URLS_TO_CRAWL_KEY})"
    echo "Total urls crawled -> $(redis-cli get ${TOTAL_URLS_CRAWLED_KEY})"
    echo "Url processor batch size -> $(redis-cli get ${URL_PROCESSOR_BATCH_SIZE_KEY})"

    QUEUES=$(redis-cli keys ${QUEUES_PREFIX})
    for QUEUE in ${QUEUES};
    do
        echo "${QUEUE} -> $(redis-cli zcard ${QUEUE})"
    done
}

function queue_seed() {
    for SEED in ${SEED_URLS[@]};
    do
        echo "Queueing ${SEED}"
        redis-cli zincrby ${FRONTIER_QUEUE} 1000000 ${SEED}
    done
}

while [[ $# -ne 0 ]];
do
   case $1 in
        refresh) refresh ;;
         status) status;;
         queue-seed) queue_seed;;
         *) echo "Invalid option: $1, correct usage is: redis-ops.sh [refresh|status|queue-seed]";;
   esac
   shift
done