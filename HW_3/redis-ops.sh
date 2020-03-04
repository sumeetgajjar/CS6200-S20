#!/usr/bin/env bash

set -e

MAX_URLS_TO_CRAWL_KEY='MAX_URLS_TO_CRAWL'
URL_PROCESSOR_BATCH_SIZE_KEY='URL_PROCESSOR_BATCH_SIZE'
CRAWLED_URLS_BF_KEY='CRAWLED_URLS_BF'
TOTAL_URLS_CRAWLED_KEY='TOTAL_URLS_CRAWLED'
QUEUES_PREFIX='QUEUES::*'
FRONTIER_QUEUE='QUEUES::FRONTIER'
DOMAIN_INLINKS_COUNT_KEY="DOMAIN_INLINKS_COUNT"
URL_INLINKS_COUNT_KEY="URL_INLINKS_COUNT"
URL_RELEVANCE_KEY='URL_RELEVANCE'
SEED_URLS=("https://en.wikipedia.org/wiki/American_Revolutionary_War" \
           "https://www.history.com/topics/american-revolution/american-revolution-history" \
           "https://en.wikipedia.org/wiki/American_Revolution" \
           "https://www.revolutionary-war.net/causes-of-the-american-revolution.html" \
           "https://www.britannica.com/event/American-Revolution" \
	       "https://www.battlefields.org/learn/articles/overview-american-revolutionary-war")


function refresh() {
    redis-cli set ${MAX_URLS_TO_CRAWL_KEY} 60000
    redis-cli set ${URL_PROCESSOR_BATCH_SIZE_KEY} 10

    redis-cli del ${CRAWLED_URLS_BF_KEY} \
                  ${TOTAL_URLS_CRAWLED_KEY}
#                  ${URL_INLINKS_COUNT_KEY} \
#                  ${DOMAIN_INLINKS_COUNT_KEY} \
#                  ${URL_RELEVANCE_KEY}

    for QUEUE in $(redis-cli keys ${QUEUES_PREFIX});
    do
        redis-cli del ${QUEUE}
    done
}

function status() {
    echo "Max urls to crawl -> $(redis-cli get ${MAX_URLS_TO_CRAWL_KEY})"
    echo "Total urls crawled -> $(redis-cli get ${TOTAL_URLS_CRAWLED_KEY})"
    echo "Url processor batch size -> $(redis-cli get ${URL_PROCESSOR_BATCH_SIZE_KEY})"
    echo "Url inlinks size -> $(redis-cli hlen ${URL_INLINKS_COUNT_KEY})"
    echo "Domain inlinks size -> $(redis-cli hlen ${DOMAIN_INLINKS_COUNT_KEY})"
    echo "Url relevance size -> $(redis-cli hlen ${URL_RELEVANCE_KEY})"

    QUEUES=$(redis-cli keys ${QUEUES_PREFIX})
    for QUEUE in ${QUEUES};
    do
        echo "${QUEUE} -> $(redis-cli llen ${QUEUE})"
    done
}

function queue_seed() {
    for SEED in ${SEED_URLS[@]};
    do
        echo "Queueing ${SEED}"
        redis-cli lpush ${FRONTIER_QUEUE} "{\"url\": \"${SEED}\", \"wave\": 0}"
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