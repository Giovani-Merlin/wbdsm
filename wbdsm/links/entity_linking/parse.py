import logging

logger = logging.getLogger(__name__)


def get_left_right_query(start_index, end_index, link, text, query_max_chars=100):
    """
    Get the left and right query of a link.
    """
    link_on_query = text[start_index:end_index]
    link_query_left = text[max(0, start_index - query_max_chars) : start_index]
    link_query_right = text[end_index : min(len(text), end_index + query_max_chars) + 1]
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


def get_dataset_item(section, link, index, query_max_chars=100):
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
        query_max_chars=query_max_chars,
    )
    item["query_index"] = index

    return item
