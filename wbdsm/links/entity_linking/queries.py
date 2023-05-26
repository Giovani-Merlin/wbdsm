def get_entity_linking_query(
    candidate_surface_appearance: int = 5, candidate_text_surfaces: int = 10
):
    """
    Get the query to get the candidates and links for entity linking ranked by the number of documents they appear in.
    For each candidate get candidate_surface_appearance different text surfaces and for each text surface get candidate_text_surfaces different links.
    #
    To make it random we need to do considerably more steps
    We need to add a random value to each group, then multiply the size of the group by the random value
    Then we floor the result to get a random position in the group and finally slice the group to get n elements starting from the random position
    """

    randomly_limited_source_doc_query = [
        # ! TODO(GM): Enable links to any section, needs to change "bet_dataset.py"
        {"$match": {"links_to_section": "Abstract"}},
        {
            "$group": {
                "_id": {
                    "links_to": "$links_to",
                    "source_doc": "$source_doc",
                    "text": "$text",
                },
                "source_doc_section": {"$first": "$source_doc_section"},
                "start": {"$first": "$start"},
                "end": {"$first": "$end"},
            }
        },
        {
            "$group": {
                "_id": {"links_to": "$_id.links_to", "text": "$_id.text"},
                "link": {
                    "$push": {
                        "source_doc": "$_id.source_doc",
                        "source_doc_section": "$source_doc_section",
                        "start": "$start",
                        "end": "$end",
                        "links_to": "$_id.links_to",
                        "text": "$_id.text",
                    }
                },
            }
        },
        {
            "$project": {
                "link": 1,
                "source_doc_count": {"$size": "$link"},
                "randomvalue": {"$rand": {}},
            }
        },
        {
            "$project": {
                "link": 1,
                "source_doc_count": 1,
                "randomposition": {
                    "$floor": {
                        "$multiply": [
                            "$randomvalue",
                            {
                                "$max": [
                                    0,
                                    {
                                        "$subtract": [
                                            "$source_doc_count",
                                            candidate_surface_appearance,
                                        ]
                                    },
                                ]
                            },
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "link": {
                    "$slice": [
                        "$link",
                        "$randomposition",
                        candidate_surface_appearance,
                    ]
                },
                "source_doc_count": "$source_doc_count",
            }
        },
        {"$sort": {"source_doc_count": -1}},
        {
            "$group": {
                "_id": "$_id.links_to",
                "sum": {"$sum": "$source_doc_count"},
                "links": {"$push": {"link": "$link", "count": "$source_doc_count"}},
            }
        },
        {"$sort": {"sum": -1}},
        {"$project": {"links": {"$slice": ["$links", candidate_text_surfaces]}}},
    ]
    return randomly_limited_source_doc_query
