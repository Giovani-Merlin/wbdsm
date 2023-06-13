import logging
import re
from datetime import datetime
from typing import List

from wbdsm.documents import Page, RedirectPage, Section, Link
from wbdsm.preprocessing import encode_id
from pymongo.collection import Collection

logger = logging.getLogger(__name__)

# Maybe can get some false positives, but it's better than false negatives
# Occurs rarely
legends = re.compile(r"\w+\|[\w']+ {0,1}\[\[[^\\]*\]\]")


def extract_links(
    pages_obj: List[Page], pages_collection: Collection, min_query_size: int = 50
):
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
    logger.info("Processing articles")
    time_now = datetime.now()
    links = []
    for page_obj in pages_obj:
        for section in page_obj.sections:
            #  If it's not a legend (for images, tables....)
            if (
                not legends.match(section.content)
                and len(section.content) > min_query_size
            ):
                extracted_links = extract_section_links(
                    section, pages_collection=pages_collection, source_page=page_obj
                )
                links.extend(extracted_links)

    logger.info(f"Finished in {datetime.now() - time_now}")
    logger.info("Done")
    return links


def extract_section_links(
    section: Section, pages_collection: Collection, source_page: Page
):
    """
    For each link in the section, parse the link and retrieve the source page of the link.
    """
    # Keep track of the last link to reduce the search space (links are ordered)
    last_link_position = 0
    extracted_links = []
    for link in section.links:
        # If the link is for a wikipedia page. If it's not, it's an external link and it's not interesting for us.
        if link.type == "internal":
            links_to = link.page
            # Normally this condition/error happens when the macro is generated "at the moment", i.g. {{LASTYEAR}} = ""
            if links_to != "":
                link, last_link_position = parse_page_link(
                    section, link, last_link_position, source_page, pages_collection
                )
                if link:
                    extracted_links.append(link)

    return extracted_links


def parse_page_link(
    section: Section,
    link: Link,
    last_link_position: int,
    source_page: Page,
    pages_collection: Collection,
):
    """
    Parse a link to a wikipedia page. It do links scanning progressively, as we don't have the link position from dumpster-dive but we have an ordered list of links.
    """
    section_text_to_scan = section.content[last_link_position:]
    link_text = link.text
    links_to_section = link.section
    # Find mention position in the filtered article text.
    mention_position = section_text_to_scan.find(link_text)
    parsed_link = None
    # If -1 it means that the link is not in the text. This should not happen.
    if mention_position != -1:
        start = mention_position + last_link_position
        end = start + len(link_text)
        # Assert that the link is in the correct position
        assert section.content[start:end] == link_text
        # Check if link links to an existing page

        # If page exists
        # Translate redirects to the source page
        links_to_encoded = encode_id(link.page, encode_title=True)
        links_to, redirect_section = translate_link(
            links_to_encoded, pages_collection, links_to_section=links_to_section
        )
        if redirect_section:
            links_to_section = redirect_section
        # Sometimes redirectToPage is broken
        if links_to:
            parsed_link = {
                "_id": str(start) + source_page.id,  # No overlap in the same article
                "start": start,
                "end": end,
                "text": link_text,
                "links_to": links_to_encoded,  # To mach documents when searching
                "source_doc": encode_id(source_page.title, encode_title=True),
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

        else:
            logger.info(
                f"Broken link: Text: {link.text} Source page: {source_page.id} Section: {section.index} Links to: {link.page}"
            )
        # Update last link position
        last_link_position = mention_position + len(link_text) + last_link_position
    else:
        logger.error(f"Link not found in section {section.index} in {source_page.id}")

    return parsed_link, last_link_position


def translate_link(links_to: str, pages_collection: Collection, links_to_section: str):
    """
    Translate the link's information to the source page of the link.
    It handles the redirects and the links to sections.

    If links_to_sections is None and links to a redirect, and the redirect has a redirectToSection it will use it.
            # ! TODO(GM): verify if this is correct:
        If it has redirectToSection and the links has a section, it will use the section of the link.

    """

    projections = {"sections": 0}
    links_to_encoded_title = encode_id(links_to, encode_title=True)
    links_to_article_page = pages_collection.find_one(
        {"title": links_to_encoded_title},
        projections,
    )
    if links_to_article_page:
        # If it's a redirect, map to the source page
        if links_to_article_page["isRedirect"]:
            links_to_article_page_obj = RedirectPage.from_mongo(links_to_article_page)
            links_to = links_to_article_page_obj.redirectToPage
            # Check if redirect page exists - if redirect page has a redirect link (rarely it doesn't exist)
            if links_to:
                links_to_article_page = pages_collection.find_one(
                    {"title": links_to_encoded_title},
                    projections,
                )
                # Redirect to an invalid page (broken redirect)
                if links_to_article_page is None:
                    links_to = None

                # If the correct is from the redirect page, we should enable it here without the "if"
                elif links_to_section:
                    links_to_section = links_to_article_page_obj.redirectToSection
            else:
                # Redirect to nothing (broken redirect)
                links_to = None

        else:
            # Not a redirect, all good
            links_to = links_to_encoded_title

    else:
        # No reference
        links_to = None

    return links_to, links_to_section
