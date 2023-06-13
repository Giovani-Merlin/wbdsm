def get_entity_recognition_query():
    entity_recognition_candidates_abstracts_ordered = [
        {"$match": {"isRedirect": False}},
        {"$addFields": {"sections_array": {"$objectToArray": "$sections"}}},
        {"$unwind": "$sections_array"},
        {"$project": {"title": 1, "sections_array": 1, "_id": 0}},
        {
            "$addFields": {
                "text_size": {
                    "$cond": {
                        "if": {"$lte": [{"$strLenCP": "$sections_array.v.text"}, 100]},
                        "then": None,
                        "else": {"$strLenCP": "$sections_array.v.text"},
                    }
                },
                "section_name": "$sections_array.k",
            }
        },
        {"$unwind": "$sections_array.v.links"},
        {
            "$addFields": {
                "link_text": "$sections_array.v.links.text",
                "link_size": {
                    "$cond": {
                        "if": {"$eq": ["$sections_array.v.links.text", None]},
                        "then": 0,
                        "else": {"$strLenCP": "$sections_array.v.links.text"},
                    }
                },
                "link_type": "$sections_array.v.links.type",
            }
        },
        {"$match": {"link_type": "internal"}},
        {
            "$group": {
                "_id": {"title": "$title", "section_name": "$section_name"},
                "links_size": {"$sum": "$link_size"},
                "text_size": {"$first": "$text_size"},
            }
        },
        {"$project": {"links_density": {"$divide": ["$links_size", "$text_size"]}}},
        {"$sort": {"links_density": -1}},
    ]
    return entity_recognition_candidates_abstracts_ordered
