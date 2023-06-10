<div align="center">
<a href="https://www.buymeacoffee.com/giovanimerlin" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-orange.png" alt="Buy Me A Coffee" height="41" width="174"></a>
<a href="https://github.com/giovani-merlin/wbdsm" title="Go to GitHub repo"><img src="https://img.shields.io/static/v1?label=giovani-merlin&message=wbdsm&color=blue&logo=github" alt="giovani-merlin - wbdsm"></a>
<br/>

</div>

# WBDSM

<img src="/assets/wbdsm_logo.png"  style="height: 220px; width:220px;margin: 0 0 0 15px;" align="right"/>
Wikipedia Based Data-Set Maker
  
## Objective

WBDSM addresses a common challenge in Deep Learning NLP projects. While many repositories share preprocessed datasets to benefit the community, these datasets are often available only in English and lack the means for generating new datasets. The primary objective of WBDSM is to provide an effortless solution for creating reliable datasets from Wikipedia, supporting any language. Key features of WBDSM include:

* Seamless data extraction from Wikipedia using the libraries wtf_wikipedia and dumpster-dive.
* Modifying the output of dumpster-dive and introducing a dedicated library for efficient communication with the extracted data.
* Employing advanced processing techniques with Celery and Redis to parallelize operations, ensuring optimal performance.

## Applications

WBDSM offers a range of applications to facilitate various NLP tasks:

1. Entity Linking: WBDSM provides a complete and robust solution for generating entity linking datasets. This application is already implemented and available.
2. Entity Recognition: In progress. Utilizing the output of entity linking, entity recognition can be easily accomplished.
3. ...

## What it provides

To empower users in their dataset creation endeavors, WBDSM offers the following components:

1. Documentation for indexing data in a specific format, enabling the generation of diverse datasets (see mongodb_format.md).
2. Redirect mapping functionality, ensuring accurate connections between related pages.
3. Well-designed classes to facilitate seamless communication with the indexed data.
4. Feature-rich functions to enhance data, including the ability to incorporate Wikipedia ranking based on linking references.
5. A comprehensive script for generating Entity Linking datasets.

## TODO

* [ ] Add wikidata info to the dataset

## Requirements

Docker, MongoDB, Redis, npm and nodejs 18. Python versions used was 3.9.16

## How to use

### Indexing wikipedia on mongoDB

This section is just the use of dumpster-dive, with some changes of the default article's format on MongoDB to avoid using too much space and to have a reliable way to use the article's data.

1. Download the desired wikipedia dump (***-pages-articles-multistream.xml.bz2** ) from `https://dumps.wikimedia.org/{language_code}wiki/` and extract it (on linux I recommend using lbzip2 for parallel extraction).
2. Index wikipedia dump on mongoDB using index_wiki_mongo.js script. It expects 3 arguments: the path to the extracted wikipedia dump, the language code and the mongoDB connection string. Example:

```bash
cd dumpster
npm install .
node index_wiki_mongo.js ~/Downloads/wikipedia/en/enwiki-20230401-pages-articles-multistream.xml en mongodb://localhost:27017/ 
```

## Extract links

As the full wikipedia dump has a lot of pages, and for extracting the links we need to map the links to the correct page, we need to do it in a efficient way. For that, we use celery and redis to parallelize the processing using a queue system. Try to give enough run for Redis when processing for the EN wikipedia, giving its huge size.

1. Pull redis `docker pull redis` and run it `docker run --name redis_celery -d -p 6379:6379 redis | docker run redis`
2. ~Change the attributes in IndexMentions class on wbdsm.links.extract_links_task to match the desired language and mongoDB connection string.~ Problem with the bootsraping of the celery app, for now changing it manually. If you know why the bootstraping is not working, please let me know.
3. Run the workers - created a script to do all these steps on scripts/start_workers.sh but it seems that the detached mode is not working. For now, run the following commands on different terminals:

```bash
cd celery/links
mongo_uri="mongodb://localhost:27017"
language="en"
celery -A extract_links_worker worker -Ofair --queues=links_to_extract --loglevel=info --concurrency=17 --language $language --mongo_uri $mongo_uri -n extract
```

```bash
cd celery/links
mongo_uri="mongodb://localhost:27017"
language="en"
celery -A extract_links_worker worker -Ofair --queues=links_to_index --loglevel=info --concurrency=5  --language $language --mongo_uri $mongo_uri -n index
```

```bash
cd celery/links
mongo_uri="mongodb://localhost:27017"
language="en"
python extract_links_app.py --mongo_uri $mongo_uri --language $language
```

If you need to purge the queues, run:

```bash
celery -A extract_links_worker purge --queues links_to_extract -f
celery -A extract_links_worker purge --queues links_to_index -f
```

It's usefull to follow the progress of the workers. For that, you can use:

```bash
cd celery/links
celery --broker=redis://localhost:6379/0 flower
```

## Rank Pages

After extracting the links, we can rank the pages based on the number of links that point to them. This can be done running the script rank_by_links.py. It expects the mongoDB connection string and the language code as arguments. Example:

```bash
python scripts/rank_by_links.py --mongo_uri mongodb://localhost:27017/ --language fr
```

## Generate Entity Linking Dataset

After extracting the links, we can generate the entity linking dataset. This can be done running the script generate_entity_linking_dataset.py. It expects the mongoDB connection string, the language code, the number of candidates and the number of mentions as arguments. Example:

```bash
python scripts/generate_entity_linking_dataset.py --mongo_uri mongodb://localhost:27017/ --language fr --max_rank 200000 --test_size 1000 --validation_size 1000 --candidates_size 1000000 --candidate_text_surfaces 10 --candidate_surface_appearance 2
```

### Time to generate the dataset

Using 12th Gen Intel(R) Core(TM) i7-12700H

1. 75 min to process dumpster-dive for english wikipedia, 20 for the german one.
2. 3h16 to extract links for english wikipedia (113M links) - 3 min indexing and started process on 16:47:19, 1h53 for the german one (53.8M links).
3. Rank pages - 14 min DE and 40 min EN - 1h6 agr
4. Generate dataset* - 1h15 DE. Something close ofr the EN one. 1h 10 100k with sample size of 30M (just increase to 7m the groupby...) 1.4M

Memory used for the EN dataset: 26GB.

\* 200k max rank, 1M candidates, 2 candidate_surface_appearance, 10 candidate_text_surfaces - generated +- 1.1M entries.

For 5 candidate surface, 1M, 100k, 10 candidates surfaces -> 1M FR and 1h05

EN wikipedia: 19GB Pages,n Links +23 GB (7.5 GB from _id, to optimize) + celery workers. Be sure to have at least 40GB Ram more swap.

72kk 22:17 94.6 at 23:49 => 1h32 for 22.6M (double of the expected time)

# Improve test set

Test set and validation sets, needs to have more candidates / maybe bm25 before creating it
