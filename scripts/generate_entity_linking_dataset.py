import json
import logging
import os
from itertools import cycle
from typing import List

from pymongo import MongoClient

from wbdsm.documents import Page
from wbdsm.links.entity_linking.queries import get_entity_linking_query
from wbdsm.preprocessing import clean_text
from wbdsm.wbdsm_arg_parser import WBDSMArgParser

logger = logging.getLogger(__name__)
parser = WBDSMArgParser()
args = parser.parse_known_args()[0].__dict__
mongo_uri = args["mongo_uri"]
output_path = args["output_path"]
language = args["language"]
db_name = language + "wiki"
candidate_text_surfaces = args["candidate_text_surfaces"]
candidate_surface_appearance = args["candidate_surface_appearance"]
os.makedirs(output_path, exist_ok=True)
#
client = MongoClient(mongo_uri)

args["query_max_chars"] = args["max_chars"] // 2

# Get wikipedia connection
client = MongoClient(mongo_uri)
pages_collection = client[db_name]["pages"]
links_collection = client[db_name]["links"]


def get_left_right_query(start_index, end_index, link, text):
    """
    Get the left and right query of a link.
    """
    link_on_query = text[start_index:end_index]
    link_query_left = text[max(0, start_index - args["query_max_chars"]) : start_index]
    link_query_right = text[
        end_index : min(len(text), end_index + args["query_max_chars"]) + 1
    ]
    try:
        assert link == link_on_query
        return link_query_left, link_query_right
    except AssertionError:
        # ! TODO(GM): Should not ever happen, It's a problem of the normalization of the source doc mixing different documents together. Really rare so not a priority.
        logger.error(
            "link {} is not in the query {} || {}".format(
                link, link_query_left, link_query_right
            )
        )
        return None, None


def get_dataset_item(section, link, index):
    """
    Given a dict of links to a candidate_doc and a dict of query documents return the dataset items.
    """

    item = {}
    item["link"] = link["text"]
    item["candidate_index"] = link["candidate_index"]
    item["source_doc"] = link["source_doc"]
    item["source_doc_section"] = section.title
    item["query_left"], item["query_right"] = get_left_right_query(
        link["start"],
        link["end"],
        link["text"],
        section.content,
    )
    item["query_index"] = index

    return item


def get_abstracts(min_rank, max_rank, init_index=0):
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
                    "abstract": abstract,
                    "reference_rank": row["reference_rank"],
                    "candidate_index": index + init_index,
                }
            )
            index += 1
    logger.info("Finished iterating over the abstracts")

    return abstract_total


def create_links_dataset_by_agg(abstract_titles: List[str], output_path: str):
    links_query = {"$match": {"links_to": {"$in": abstract_titles}}}
    sample = {
        "$sample": {
            "size": 1e7,
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
                    item = get_dataset_item(section, link_doc, index_link)
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


if args["max_rank"]:
    logger.info(f"Getting abstracts from rank 0 to {args['max_rank']}")
    abstracts = get_abstracts(0, args["max_rank"])
else:
    abstracts = get_abstracts(None, None)

all_ids = [a["candidate"] for a in abstracts]
n_abstracts = len(all_ids)
# # Save abstracts
with open(os.path.join(output_path, "candidates.jsonl"), "w") as f:
    for abstract in abstracts:
        json.dump(abstract, f)
        f.write("\n")
# # Get the links
links = create_links_dataset_by_agg(all_ids, output_path=output_path)

# Count the number of successful extracted candidates - if a doc is not found all_ids != total candidates
count = 0
with open(os.path.join(output_path, "links.jsonl"), "r") as f:
    for line in f:
        count += 1
train_size = count - args["validation_size"] - args["test_size"]

# Open the links file and create the dataset
train_indexes = (0, train_size)
validation_indexes = (train_size + 1, train_size + args["validation_size"] + 1)
test_indexes = (validation_indexes[1] + 1, n_abstracts)
full_dataset = {}
index = 0
# ! Just one-shot creation for now, sizes are considered by the candidates
# ! Needs to implement a fully random dataset creation and one for "text-surface one-shot". With these the numbers would be by number of links
# ! By number of links we need to first unroll the links and then create the dataset - for the "text-surface one-shot" we need to keep one of the text surfaces for dev/test.
train_file = open(os.path.join(output_path, "train.jsonl"), "w")
validation_file = open(os.path.join(output_path, "validation.jsonl"), "w")
test_file = open(os.path.join(output_path, "test.jsonl"), "w")
#
with open(os.path.join(output_path, "links.jsonl"), "r") as f:
    for line in f:
        candidate_links = json.loads(line)
        for link in list(candidate_links.values())[0]:
            # Write to the file
            if index >= train_indexes[0] and index <= train_indexes[1]:
                train_file.write(json.dumps(link) + "\n")
            elif index >= validation_indexes[0] and index <= validation_indexes[1]:
                validation_file.write(json.dumps(link) + "\n")
            elif index >= test_indexes[0] and index <= test_indexes[1]:
                test_file.write(json.dumps(link) + "\n")
            else:
                raise ValueError(f"Numbers don't match")

        index = index + 1

# Complete dataset size if it's not "full candidates list"
if args["candidates_size"]:
    candidates_to_get = args["candidates_size"] - n_abstracts
    rank_to_get = candidates_to_get + args["max_rank"]
    complete_candidates = get_abstracts(
        args["max_rank"], rank_to_get, init_index=n_abstracts
    )
    with open(os.path.join(output_path, "candidates.jsonl"), "a") as f:
        for abstract in complete_candidates:
            json.dump(abstract, f)
            f.write("\n")
with open(os.path.join(output_path, "dataset_description.json"), "wt") as f:
    json.dump(args, f)
