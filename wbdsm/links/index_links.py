import logging

from typing import List

from pymongo import UpdateOne
from pymongo.collection import Collection


logger = logging.getLogger(__name__)


def index_links(links: List[dict], links_collection: Collection) -> None:
    """
    Indexes links in MongoDB. Uses _id as unique identifier to avoid duplicates.
    """
    operations = []
    # Bulk upsert
    # https://stackoverflow.com/questions/5292370/fast-or-bulk-upsert-in-pymongo
    for link in links:
        operations.append(UpdateOne({"_id": link["_id"]}, {"$set": link}, upsert=True))
    n_links = len(operations)
    logger.info(f"Inserting {n_links}")
    if operations:
        links_collection.bulk_write(operations)
        logger.info(f"Inserted {n_links}")
