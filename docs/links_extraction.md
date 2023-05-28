# Links extraction

The code is responsible for extracting "internal" links, which are links that point to other pages within the same wiki. It does not parse external links or links to other wikis. The links obtained from the "dumpster-dive" process are ordered but without its exact position on the text. Therefore, a progressive processing approach is required. Additionally, the attributes from the dumpster-dive are not ordered, preventing the accurate association of lists, quotes, and other elements with their respective links. (To remove list pages, it would be good to have the wikidata info to remove for example instances of Q13406463).

The extraction process involves ordering the links by PageID and indexing the title to perform the extraction. Each link needs to be mapped to an existing page, and if there are redirects, the correct page needs to be remapped. These steps are implemented within the wbdsm.links.extract_links function.

## Problems

There is an issue where documents with three dots in their names are indexed in the links collection with only two dots. The reason for this behavior is not currently understood. This issue is the source of the "source_doc not found" error that occurs during the dataset generation process.
