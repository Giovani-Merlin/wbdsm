import logging
import time
from datetime import datetime

import pymongo
from celeryconfig import broker_url
from extract_links_worker import app
from extract_links_task import extract_links_task, index_links_task
from flower.utils.broker import Broker
from pymongo import MongoClient

from celery import chain
from wbdsm.wbdsm_arg_parser import WBDSMArgParser

logger = logging.getLogger(__name__)
if __name__ == "__main__":
    #

    parser = WBDSMArgParser()
    args = parser.parse_known_args()[0].__dict__

    mongo_uri = args["mongo_uri"]
    language = args["language"]
    db_name = language + "wiki"
    last_id = args.get("last_pageID")

    client = MongoClient(mongo_uri)
    pages = client[db_name]["pages"]
    query = {"isRedirect": False}
    CHUNK_SIZE = 500
    #
    mentions_batch = []
    initial = datetime.now()
    #
    page_batch = []
    all_jobs = []
    i = app.control.inspect()
    # to track the status of the tasks
    broker = Broker(broker_url)
    # Create compound index between pageID and isRedirect
    pages.create_index(
        [("isRedirect", pymongo.DESCENDING), ("pageID", pymongo.ASCENDING)]
    )
    # Create title index - used to find pages by title given in links
    pages.create_index("title")
    # Do first page
    if not last_id:
        last_id = pages.find_one({"isRedirect": False}, sort=[("pageID", 1)])["pageID"]
        articles = list(pages.find({"isRedirect": False}).sort("pageID", 1).limit(1))
    else:
        # Note that last_id must be already encoded - kept in this way to make it easier to recover from a crash (logs are in encoded form)
        articles = list(
            pages.find({"pageID": {"$gte": last_id}, "isRedirect": False})
            .sort("pageID", 1)
            .limit(1)
        )

    # all_jobs.append(
    #     chain(
    #         extract_links_task.s(articles[0]["pageID"], CHUNK_SIZE), index_links_task.s()
    #     ).apply_async()
    # )
    n_pages = 0
    while articles:
        debug = extract_links_task(skip=articles[-1]["pageID"], limit=CHUNK_SIZE)
        test = index_links_task(links=debug)
        articles = list(
            pages.find({"pageID": {"$gt": articles[0]["pageID"]}, "isRedirect": False})
            .sort("pageID", 1)
            .limit(1)
            .skip(CHUNK_SIZE - 1)
        )
        if not articles:
            break
        all_jobs.append(
            chain(
                extract_links_task.s(articles[0]["pageID"], CHUNK_SIZE),
                index_links_task.s(),
            ).apply_async()
        )
        n_pages = n_pages + CHUNK_SIZE
        logger.info(f"Processed {n_pages} pages")
        page_batch = []
        # Avoid memory overflow
        if n_pages % 100000 == 0:
            queue_size = broker.queues(["links_to_extract"]).result()[0]["messages"]
            logger.info(f"Queue size: {queue_size}")
            # Wait to reduce queue
            while queue_size > 50:
                queue_size = broker.queues(["links_to_extract"]).result()[0]["messages"]
                logger.info(f"Queue size: {queue_size}")
                time.sleep(1)

    # Wait to finish all jobs
    # Could use results backend but given the size of the data, it is not worth it
    # Easily we can overflow redis memory
    indexing_jobs = broker.queues(["links_to_index"]).result()[0]["messages"]
    extract_jobs = broker.queues(["links_to_extract"]).result()[0]["messages"]
    jobs = indexing_jobs + extract_jobs
    while jobs:
        time.sleep(1)
        indexing_jobs = broker.queues(["links_to_index"]).result()[0]["messages"]
        extract_jobs = broker.queues(["links_to_extract"]).result()[0]["messages"]
        jobs = indexing_jobs + extract_jobs
        logger.info(f"Jobs: {jobs}")

    # 1 minute sleep to be sure - increase if chunk is too big
    time.sleep(60)
    app.control.shutdown()

    logger.info(f"Finished in {datetime.now() - initial}")
    logger.info(f"Processed {n_pages} pages")
