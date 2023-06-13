# Format on MongoDB

By default dumpster-dive will index all the information on the wikipedia-dump. This requires a lot of space and is not necessary for our use cases. So we will change the default format to only index the information we need.

## Pages

- _id -> The encoded title of the page (can't change it, default from dumpster-dive)
- Sections -> Array of sections indexed by the section name.
  - Each section has
  - Text: The text of the section
  - Links: The links inside the section as array of "text", "type" and "page" (the page is the title of the page that the link points to)
  - Index: Order of the section on the page

- Categories -> Array of categories.
  - Categories in the language of the dump. Not really usefull, wikidata would be more powerfull.

- isRedirect -> Boolean, if the page is a redirect page.
- redirectTo -> If the page is a redirect page, the title of the page it redirects to.
- url -> The url of the page on wikipedia.
- title -> Normalized and title-cased title of the page (to avoid mismatch between others pages elements as links and the title of the page) without the encoding.
- pageId -> The id of the page on wikipedia.

## Encoding

To encode str to bson (index default), we need to do some conversions:
\ -> \\
$ -> \\u0024
. -> \\u002e
This is automatically done by dumpster-dive on the _id field, and done in index_wiki_mongo.js for the Section's title fields.
