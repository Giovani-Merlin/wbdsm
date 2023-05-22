"""
Extract links/mentions from wikipedia indexed in mongodb by wtf_wikipedia.
Needs to parse redirects beforehand

The database will be created at datapath/{language}wiki_mentions.db

Couldn't send arguments to celery worker, therefore we need to change the language and data path manually here.
"""

import logging
import re
from datetime import datetime
from typing import List

import pymongo
from celery import Celery, Task, bootsteps
from pymongo import MongoClient, UpdateOne

from wbdsm.documents import Page, RedirectPage
from wbdsm.links.extract_links_worker import app
from wbdsm.preprocessing import encode_id

# Maybe can get some false positives, but it's better than false negatives
# Occurs rarely
legends = re.compile(r"\w+\|[\w']+ {0,1}\[\[[^\\]*\]\]")
logger = logging.getLogger(__name__)


# https://docs.celeryq.dev/en/latest/userguide/tasks.html#instantiation
class IndexMentions(Task):
    abstract = True

    mongo_uri = "mongodb://localhost:27017"
    language = "de"

    # the cached requests.session object
    def __init__(self):
        #
        self.language = IndexMentions.language
        self.mongo_uri = IndexMentions.mongo_uri
        print(self.language)
        print(self.mongo_uri)
        db_name = self.language + "wiki"
        self.db_name = db_name
        client = MongoClient(self.mongo_uri)
        self.mentions_collection = client[db_name]["mentions"]
        self.pages_collection = client[db_name]["pages"]
        self.mentions_collection.create_index([("links_to", pymongo.HASHED)])
        self.mentions_collection.create_index([("source_doc", pymongo.HASHED)])
        self.mentions_collection.create_index([("text", pymongo.HASHED)])
        print("loaded")


# ! NOT WORKING
# https://stackoverflow.com/questions/27070485/initializing-a-worker-with-arguments-using-celery
# Make bootstep to add custom arguments


class CustomArgs(bootsteps.Step):
    def __init__(self, worker, mongo_uri, language, **options):
        super().__init__(worker, **options)
        print("Storing language and data_path")
        print("Language: ", language)
        print("Mongo URI: ", mongo_uri)
        IndexMentions.language = language[0]
        IndexMentions.mongo_uri = mongo_uri[0]

    def start(self, parent):
        # our step is started together with all other Worker/Consumer
        # bootsteps.
        print("{0!r} is starting".format(parent))


app.steps["worker"].add(CustomArgs)


# Could parse the article in different ways, like getting the text per paragraph, ignoring lists, depends on the objective. To match Zeshel's and mewsli's (uses wikiextractor) we will just append all the texts.
@app.task(
    name="extract_links",
    bind=True,
    queue="mentions_to_extract",
    base=IndexMentions,
    language=None,
    mongo_uri=None,
)
def extract_links(self, skip: str, limit: int, min_query_size: int = 50):
    """
    Retrieve links information from a wikipedia page parsed by wtf_wikipedia.

    It:
    1. Parse sentence by sentence to retrieve correct position of the link in the sentence text.
        wtf_wikipedia doesn't give link position and we can only retrieve the position by using string search. The links are ordered, therefore we need to progressively search the link in the text.
    2. Use redirects table to get source page of the link.
    3. Normalize mention's accents
    4. Avoid parsing image/tables/... legends
    5. TODO(GM): Skip lists objects and others "bad source"/"badly parsed" docs (needs wiki_id)

    Args:
        skip (str): Skip articles with id lower than this
        limit (int): Number of articles to parse
        min_query_size (int, optional): Min size of the query (in chars) to be considered. Defaults to 50. Values lower than this will be ignored.
    """
    # First
    logger.info("Getting articles from pages collection")
    time_now = datetime.now()
    # Get pages from pages collection
    pages = list(
        self.pages_collection.find({"isRedirect": False, "pageID": {"$gt": skip}})
        .sort("pageID", 1)
        .limit(limit)
    )
    # Transform in dataclasses to make it easier to work with and encapsulate parsing/cleaning logic
    pages_obj = [Page.from_mongo(page, self.language) for page in pages]
    logger.info(f"Got articles from pages collection in {datetime.now() - time_now}")
    logger.info("Processing articles")
    mentions = []
    for page_obj in pages_obj:
        for section in page_obj.sections:
            text_size = 0
            article_text = ""
            # Keep track of the last link to reduce the search space (links are ordered)
            last_link_position = 0
            #  If it's not a legend (for images, tables....)
            if (
                not legends.match(section.content)
                and len(section.content) > min_query_size
            ):
                # Adds text per link (to keep track of the links in the section)
                for link in section.links:
                    section_text_to_scan = section.content[last_link_position:]
                    links_to_section = link.section
                    # If the link is for a wikipedia page. If it's not, it's an external link and it's not interesting for us.
                    if link.type == "internal":
                        links_to = link.page
                        # Normally this condition/error happens when the macro is generated "at the moment", i.g. {{LASTYEAR}} = ""
                        if links_to != "":
                            link_text = link.text
                            # Find mention position in the filtered article text.
                            mention_position = section_text_to_scan.find(link_text)
                            # If -1 it means that the link is not in the text. This should not happen.
                            if mention_position != -1:
                                start = (
                                    mention_position + text_size + last_link_position
                                )
                                end = start + len(link_text)
                                # Assert that the link is in the correct position
                                assert section.content[start:end] == link_text
                                # Translate redirects to the source page
                                projections = {"sections": 0}
                                links_to_article_page = self.pages_collection.find_one(
                                    {"title": encode_id(links_to, encode_title=True)},
                                    projections,
                                )
                                # If page exists
                                # ! TODO(GM): Map this complicated logic to "process_redirects"
                                if links_to_article_page is not None:
                                    # If it's a redirect, map to the source page
                                    if links_to_article_page["isRedirect"]:
                                        links_to_article_page_obj = (
                                            RedirectPage.from_mongo(
                                                links_to_article_page
                                            )
                                        )
                                        links_to = (
                                            links_to_article_page_obj.redirectToPage
                                        )
                                        # Check if redirect page exists - if redirect page has a redirect link (rarely it doesn't exist)
                                        if links_to:
                                            links_to_article_page = (
                                                self.pages_collection.find_one(
                                                    {
                                                        "title": encode_id(
                                                            links_to, encode_title=True
                                                        )
                                                    },
                                                    projections,
                                                )
                                            )
                                            # If redirect page doesn't exist, set links_to to None
                                            if links_to_article_page is None:
                                                links_to = None
                                        # Some links sends to a redirect page, which can be a redirect to a section.
                                        # ! TODO(GM): Using source doc section as the correct position, but needs to check it.
                                        elif not links_to_section:
                                            links_to_section = (
                                                links_to_article_page_obj.redirectToSection
                                            )
                                    # Sometimes redirectToPage is broken
                                    if links_to:
                                        mentions.append(
                                            {
                                                "_id": str(start)
                                                + page_obj.id,  # No overlap in the same article
                                                "start": start,
                                                "end": end,
                                                "text": link_text,
                                                "links_to": encode_id(
                                                    links_to
                                                ),  # To mach documents when searching
                                                "source_doc": encode_id(page_obj.id),
                                                "links_to_section": encode_id(
                                                    links_to_section,
                                                    upper_case_first_letter=False,
                                                )
                                                if links_to_section
                                                else "Abstract",
                                                "source_doc_section": encode_id(
                                                    section.title,
                                                    upper_case_first_letter=False,
                                                ),
                                                "language": link.language,
                                                "type": link.type,
                                            }
                                        )
                                    else:
                                        logger.info(
                                            f"Broken link: {link.text} in {page_obj.id} and section {section.index} with redirect {links_to_article_page_obj.id}"
                                        )
                                else:
                                    # Can just be the "red" links in wikipedia
                                    logger.warning(
                                        f"Broken link: {link.text} in {page_obj.id} and section {section.title} linksto {links_to}"
                                    )
                                    pass
                                last_link_position = (
                                    mention_position
                                    + len(link_text)
                                    + last_link_position
                                )
                            else:
                                logger.error(
                                    f"Link not found in section {section.index} in {page_obj.id}"
                                )
                text_size = len(article_text)

    logger.info(f"Finished in {datetime.now() - time_now}")
    logger.info("Done")
    return mentions


# change to upsert https://stackoverflow.com/questions/30943076/mongoengine-bulk-update-without-objects-update
@app.task(
    base=IndexMentions,
    bind=True,
    queue="mentions_to_index",
    language=None,
    mongo_uri=None,
)
def index_mentions(self, mentions: List[dict]):
    """
    Index mentions in the database
    """
    # mentions_batch = [mention for batch in mentions for mention in batch
    operations = []
    # Bulk upsert
    # https://stackoverflow.com/questions/5292370/fast-or-bulk-upsert-in-pymongo
    for article_mention in mentions:
        operations.append(
            UpdateOne(
                {"_id": article_mention["_id"]}, {"$set": article_mention}, upsert=True
            )
        )
    n_mentions = len(operations)
    print(f"Inserting {n_mentions}")
    if operations:
        self.mentions_collection.bulk_write(operations)
        print(f"Inserted {n_mentions}")


# Add custom args
