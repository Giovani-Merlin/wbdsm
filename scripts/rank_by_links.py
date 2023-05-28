"""
can take a while...
"""


import logging

import pymongo
from pymongo import MongoClient, UpdateOne

from wbdsm.wbdsm_arg_parser import WBDSMArgParser

logger = logging.getLogger(__name__)
parser = WBDSMArgParser()
args = parser.parse_known_args()[0].__dict__
mongo_uri = args["mongo_uri"]
language = args["language"]
db_name = language + "wiki"
#
client = MongoClient(mongo_uri)
links_collection = client[db_name]["links"]
pages_collection = client[db_name]["pages"]


def rank_by_links(links_collection):
    """
    Rank pages on mongoDB by using the links collection. Also, return a dictionary with the following structure:
    {url: links : {mention_0: mention_0_count, mention_i: mention_i_count, ...},
          sum: sum_count,
          ranking: ranking_count }

    """

    logger.info("Counting unique text surfaces per page")
    rank = [
        # // Groyp by links_to, source doc and text to recover 1 mention per doc per text
        {
            "$group": {
                "_id": {
                    "links_to": "$links_to",
                    "source_doc": "$source_doc",
                    "text": "$text",
                }
            }
        },
        # // Group by links_to and sums the count, also keep the source doc info
        {
            "$group": {
                "_id": "$_id.links_to",
                "count": {"$sum": 1},
            },
        },
        {"$sort": {"count": -1}},
    ]

    result = links_collection.aggregate(rank).batch_size(1000)
    #
    page_operations = []
    agg_text_surfaces = []
    logger.info("Updating page collection with the rank of the text surfaces")
    for rank, row in enumerate(result):
        # Update mongo db page with rank
        page_operations.append(
            UpdateOne(
                {"title": row["_id"]}, {"$set": {"reference_rank": rank}}, upsert=True
            )
        )
        # Keep counts for the text surfaces
        row["rank"] = rank
        agg_text_surfaces.append(row)
    logger.info("Indexing the page collection ranking")
    pages_collection.bulk_write(page_operations)
    pages_collection.create_index([("reference_rank", pymongo.ASCENDING)])

    return agg_text_surfaces


agg_text_surfaces = rank_by_links(links_collection)

logging.info("Done")
