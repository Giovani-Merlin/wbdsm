#!/bin/bash
language=$1
if [ -z "$language" ]
then
    echo "language is empty"
    exit 1
fi
mongo_uri=$2
mongo_uri=${mongo_uri:-mongodb://localhost:27017}
echo "mongo_uri: $mongo_uri"
echo "language: $language"

ls venv/bin/activate
. venv/bin/activate
cd celery/links
docker start redis_celery || docker run --name redis_celery -d -p 6379:6379 redis
celery -A extract_links_worker worker -Ofair --queues=links_to_extract --loglevel=info --concurrency=17 --language $language --mongo_uri $mongo_uri -n extract --detach
celery -A extract_links_worker worker -Ofair --queues=links_to_index --loglevel=info --concurrency=2  --language $language --mongo_uri $mongo_uri -n index --detach
python extract_links_app.py --mongo_uri $mongo_uri --language $language