import json
import logging
import os
from typing import List
from pymongo.collection import Collection
from wbdsm.documents import Page
from wbdsm.links.entity_linking.parse import get_dataset_item
from wbdsm.links.entity_linking.queries import get_entity_linking_query
from wbdsm.preprocessing import clean_text

logger = logging.getLogger(__name__)


def create_links_dataset_by_agg(
    abstract_titles: List[str],
    links_collection: Collection,
    pages_collection: Collection,
    language: str,
    candidate_surface_appearance: int,
    candidate_text_surfaces: int,
    query_max_chars: int,
    sample_size: int,
    output_path: str,
    **kwargs,
):
    links_query = {"$match": {"links_to": {"$in": abstract_titles}}}
    sample = {
        "$sample": {
            "size": sample_size,
        },
    }
    # Shuffle the result
    sample_shuffle = {"$sample": {"size": len(abstract_titles)}}
    agg_query = get_entity_linking_query(
        candidate_surface_appearance=candidate_surface_appearance,
        candidate_text_surfaces=candidate_text_surfaces,
    )
    # Allow disk use - we are using a lot of memory
    agg_links = links_collection.aggregate(
        [sample, links_query] + agg_query + [sample_shuffle], allowDiskUse=True
    ).batch_size(10000)
    logger.info("Stop")
    index_link = 0
    BATCH_SIZE = 100
    batch_n = 0
    # We will have approx BATCH SIZE * n_text_surfaces * n_querys_per_surface items in memory at the same time per batch
    # Open the links json lines file
    links_file = open(os.path.join(output_path, "links.jsonl"), "w")
    cursor_alive = True
    while cursor_alive:
        # Batch of links
        batch = []
        for _ in range(BATCH_SIZE):
            try:
                batch.append(next(agg_links))
            except StopIteration:
                cursor_alive = False
                break
        # Flatten the links
        links_per_title = [link for link_dict in batch for link in link_dict["links"]]
        links_per_doc = [
            link for link_dict in links_per_title for link in link_dict["link"]
        ]
        source_doc_ids = [doc["source_doc"] for doc in links_per_doc]
        # Add the candidate index
        for link_doc in links_per_doc:
            link_doc["candidate_index"] = abstract_titles.index(link_doc["links_to"])
        # Get the docs with the sections
        docs = list(pages_collection.find({"title": {"$in": source_doc_ids}}))
        docs = {doc["title"]: doc for doc in docs}
        # Extract the links for each candidate/section
        candidate_data = []
        last_candidate = links_per_doc[0]["links_to"]
        for link_doc in links_per_doc:
            doc = docs.get(link_doc["source_doc"])
            if doc:
                # Save by candidate to make it easier to create an one-shot dataset and to split train/dev/test
                if last_candidate != link_doc["links_to"] and candidate_data:
                    candidate_links = {link_doc["links_to"]: candidate_data}
                    links_file.write(json.dumps(candidate_links) + "\n")
                    last_candidate = link_doc["links_to"]
                    candidate_data = []

                # To parse correctly everything
                page = Page.from_mongo(doc, language=language)
                section = page.get_section(clean_text(link_doc["source_doc_section"]))
                # Get the query docs for these links
                # should not fail, but we can have alpha as "A" in link info when linking to a section with alpha in the name (really rare)...
                if section:
                    item = get_dataset_item(
                        section, link_doc, index_link, query_max_chars
                    )
                    # That is, if it was a success
                    if item["query_left"]:
                        candidate_data.append(item)
                        index_link = index_link + 1
                else:
                    logger.info(
                        f"Section not found {link_doc['source_doc_section']} for {link_doc['source_doc']}"
                    )
            else:
                logger.info(f"Doc not found {link_doc['source_doc']}")

        batch_n = batch_n + 1
        logger.info(f"Finished processing {batch_n * BATCH_SIZE} candidates")
    links_file.close()
    return True


def get_abstracts(
    min_rank: int,
    max_rank: int,
    pages_collection: Collection,
    init_index: int = 0,
    max_chars: int = 1000,
):
    """
    Get abstracts from min_rank to max_rank.
    If max_rank is None, get all the abstracts from min_rank.
    If min_rank is None, get all the abstracts from 0 to max_rank.
    If both are None, get all the abstracts - even without reference rank.

    """
    project = {"_id": 0, "sections.Abstract.text": 1, "reference_rank": 1, "title": 1}
    query = {"isRedirect": False}
    query["reference_rank"] = {}
    if max_rank:
        query["reference_rank"]["$lt"] = max_rank
    if min_rank:
        query["reference_rank"]["$gte"] = min_rank

    pages_query = pages_collection.find(
        query, projection=project, no_cursor_timeout=True
    )
    abstracts = pages_query.batch_size(10000)
    logger.info("Starting to iterate over the abstracts")
    abstract_total = []
    index = 0
    for row in abstracts:
        # Gets the abstract
        abstract = row["sections"].get("Abstract", None)
        # Years and some list pages doesn't have abstract - skip
        if abstract and len(abstract["text"]) > 64:
            abstract = clean_text(abstract["text"])
            abstract_total.append(
                {
                    "candidate": row["title"],
                    "abstract": abstract[:max_chars],
                    "reference_rank": row["reference_rank"],
                    "candidate_index": index + init_index,
                }
            )
            index += 1
    logger.info("Finished iterating over the abstracts")

    return abstract_total
