import json
import logging
import os

from pymongo import MongoClient


from wbdsm.links.entity_linking.process_mongodb import (
    create_links_dataset_by_agg,
    get_abstracts,
)
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

max_rank = args["max_rank"]
if max_rank:
    logger.info(f"Getting abstracts from rank 0 to {max_rank}")
    abstracts = get_abstracts(
        min_rank=0, max_rank=max_rank, pages_collection=pages_collection
    )
else:
    abstracts = get_abstracts(
        min_rank=None, max_rank=None, pages_collection=pages_collection
    )

all_ids = [a["candidate"] for a in abstracts]
n_abstracts = len(all_ids)
# # Save abstracts
with open(os.path.join(output_path, "candidates.jsonl"), "w") as f:
    for abstract in abstracts:
        json.dump(abstract, f)
        f.write("\n")
# # Get the links
links = create_links_dataset_by_agg(
    abstract_titles=all_ids,
    links_collection=links_collection,
    pages_collection=pages_collection,
    **args,
)

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
    rank_to_get = candidates_to_get + max_rank
    complete_candidates = get_abstracts(
        min_rank=max_rank,
        max_rank=rank_to_get,
        pages_collection=pages_collection,
        init_index=n_abstracts,
    )

    with open(os.path.join(output_path, "candidates.jsonl"), "a") as f:
        for abstract in complete_candidates:
            json.dump(abstract, f)
            f.write("\n")
with open(os.path.join(output_path, "dataset_description.json"), "wt") as f:
    json.dump(args, f)
