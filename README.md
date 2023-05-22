# WBDSM

Wikipedia Based Data-Set Maker

## Objective

Many DeepLearning NLP repositories share their datasets, already processed, in order to help the community. However, most of them are in English, and normally they don't provide an way for generating the datset - just the final result. This project aims to:

1. Provide an easy way to create different reliable datasets from Wikipedia - for any language.
2. Use the strong-consolidated library [wtf_wikipedia](https://github.com/spencermountain/wtf_wikipedia) and [dumpster-dive](www.github.com/spencermountain/dumpster-dive) to extract the data from Wikipedia.
3. Change the output of dumpster-dive and provide a library to comunicate with it.
4. Do the processing in an efficient way, using celery and redis to parallelize the processing.

## Applications

1. Entity Linking - The only one finished up to now.
2. Entity recognition - TODO - can be easily done with the output of the entity linking.
3. ...

## What it provides

HERE LINK TO DOCS

1. Index data in a specific format that enables us to generate different datasets - mongodb_format.md
2. Map redirects.
3. Classes to communicate with the indexed data.
4. Functions to enhance data (i.g, add wikipedia ranking based in linking references)
5. Script to generate Entity Linking dataset.

## TODO

- [ ] Add wikidata info to the dataset
- [ ] Optimize storage management for entity linking (keeping ids as full title string)

## Requirements

Docker, MongoDB, Redis, npm and nodejs 18

## How to use

### Indexing wikipedia on mongoDB

This section is just the use of dumpster-dive, with some changes of the default article's format on MongoDB to avoid using too much space and to have a reliable way to use the article's data.

1. Download the desired wikipedia dump (***-pages-articles-multistream.xml.bz2** ) from `https://dumps.wikimedia.org/{language_code}wiki/` and extract it (on linux I recommend using lbzip2 for parallel extraction).
2. Index wikipedia dump on mongoDB using index_wiki_mongo.js script. It expects 3 arguments: the path to the extracted wikipedia dump, the language code and the mongoDB connection string. Example:

```bash
cd dumpster
npm instlal .
node index_wiki_mongo.js ~/Downloads/wikipedia/de/dewiki-20230501-pages-articles-multistream.xml de mongodb://localhost:27017/ 
```

This will take a while (75 min for the english wikipedia, 20 for german*)
/* Using 12th Gen Intel(R) Core(TM) i7-12700H
