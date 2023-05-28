# Dataset generation

Once all the pages from Wikipedia have been indexed and the links have been processed and remapped, the dataset generation process can begin. The dataset is generated in two steps:

Grouping links: All the links that point to the same page are grouped together, and a list of candidates is generated for each page. The ranking information is used to prioritize and select meaningful candidates.

Processing groups: Each group of links is processed individually to extract the link along with its context. The data is then formatted according to the requirements of the "BET" repository.

## Grouping links

This step is crucial for optimization. It involves recovering the pointed page for each link to obtain the necessary context. To ensure efficiency, a query is performed using the wbdsm.links.entity_linking.queries.py module. This query module is designed to execute the following operations:

1. Group by link target, source document an text surface.
2. Count the number of occurrences of each text surface for each link.
3. Keeps randomly 'candidate_surface_appearance' different links for each text surface.
4. Keeps the first 'candidate_text_surfaces' different text surfaces for each candidate (i.g, United-States, USA, America for the USA). Ordered by the number of occurrences.
5. Return it ordered by the candidate's rank.

This is done to avoid having oversampling of popular pages, also to avoid querying link by link making the dataset creation unnecessary slow.

### Arguments

- `--language` (type: str): Specifies the language.
- `--mongo_uri` (type: str, default: "mongodb://localhost:27017"): Path to the wiki database.
- `--last_pageID` (type: str, default: None): Last pageID to parse - in case of interruption.
- `--max_chars` (type: int, default: 2048): Max chars to keep for entity description and for query left + right.
- `--candidates_size` (type: int, default: 100000): Number of candidates to keep for hard negatives and for full evaluation, 0 to keep all.
- `--train_size` (type: int, default: 100000): Size of the generated train dataset.
- `--test_size` (type: int, default: 10000): Size of the generated test dataset.
- `--validation_size` (type: int, default: 10000): Size of the generated validation dataset.
- `--output_path` (type: str, default: "data/bef_format"): Path to the output directory.
- `--candidate_text_surfaces` (type: int, default: 5): Occurrences of different text surfaces for the same candidate.
- `--candidate_surface_appearance` (type: int, default: 10): Occurrences of the same text surface of the same candidate.
- `--max_rank` (type: int, default: 100000): Max rank of the entity, 0 for using all.
- `--sample_size` (type: int, default: 10000000): Number of samples to use for generating the dataset. A higher number will result in a more accurate dataset, but will take longer to generate.

The size of the dataset will be at maximum max_rank X candidate_text_surfaces X candidate_surface_appearance. For example, for FR wikipedia, with max_rank = 200k, candidate_text_surfaces = 10 and candidate_surface_appearance = 2, the max size of the dataset will be 4M. In practice, just 940k is generated with the 2023/05/01 dump.
