import re
from dataclasses import dataclass
from typing import List

from wbdsm.preprocessing import clean_text, decode_id

NO_PAGE = "\]ds[];x;[s892jkmnsnoas8742981u9cunmsn1892][/x"


@dataclass
class Link:
    """Link to a document."""

    text: str
    page: str
    type: str
    language: str
    section: str = None

    def __post_init__(self):
        # Clean text and page - no lower case to match content
        self.page = decode_id(self.page)
        # If text=page we have from dumpster that link["page"]=None
        self.text = clean_text(self.text) if self.text else self.page
        # Links with space sometimes are with underscore to represent the space
        self.page = self.page.replace("_", " ")

    @classmethod
    def from_mongo_section(cls, data, language):
        # Needs language to parse interwiki links
        section = None
        if data["type"] == "interwiki":
            # Stange cases when wiki is a dict
            if type(data["wiki"]) == dict:
                language = data["wiki"]["lang"]
                data["page"] = data["text"]
        data["page"] = data.get("page") or data.get("site")
        # If no page, it uses the "anchor" schema
        if not data["page"]:
            # Each Link type has a different schema, saving it to further study. We have doc on wikipedia but it seems "instable"
            # ! TODO(GM): Do specific parse for each link type
            data["page"] = data.get("anchor") or data.get("text")
            # If not with wiki page, it's on the same wiki
            language = data.get("wiki", language)
        if not data["page"]:
            # Parse failed
            # Easy fix, but not sure if it's the best.  When not finding the page it will be dropped later.
            data["page"] = NO_PAGE

        return cls(
            text=data["text"],
            page=data["page"],
            type=data["type"],
            language=language,
            section=section,
        )


@dataclass
class Section:
    """Section of a document."""

    title: str
    content: str
    links: List[str]
    index: str

    def __post_init__(self):
        self.title = decode_id(self.title)
        self.content = clean_text(self.content)

    @classmethod
    def from_mongo_page(cls, title, section, language):
        return cls(
            title=title,
            content=section["text"],
            index=section["index"],
            links=[
                Link.from_mongo_section(link, language) for link in section["links"]
            ],
        )


@dataclass
class Page:
    """Page of a document."""

    id: str
    title: str
    sections: List[Section]
    language: str

    def __post_init__(self):
        self.id = decode_id(self.id)
        self.title = decode_id(self.title)

    @classmethod
    def from_mongo(cls, data, language):
        # Parse if redirect

        return cls(
            id=data["_id"],
            title=data.get("title"),
            sections=[
                Section.from_mongo_page(title, section, language)
                for title, section in data.get("sections", {}).items()
            ],
            language=language,
        )

    def get_section(self, section_title):
        for section in self.sections:
            if section.title == section_title:
                return section
        return None


@dataclass
class RedirectPage:
    """Page of a redirect document."""

    id: str
    title: str
    redirectToPage: str = None
    redirectToSection: str = None

    def __post_init__(self):
        self.id = decode_id(self.id)
        self.title = clean_text(self.title)
        # Some pages are redirect but don't have redirectToPage. We drop them. Ex case: when a page is a category (redirects to itself but as Category:)
        self.redirectToPage = (
            decode_id(self.redirectToPage).replace("_", " ")
            if self.redirectToPage
            else None
        )

    @classmethod
    def from_mongo(cls, data):
        # Parse redirect
        redirect_to_dict = data["redirectTo"]
        redirectToSection = None
        redirectToPage = None
        if redirect_to_dict and redirect_to_dict.get("page"):
            redirectToPage = redirect_to_dict["page"]
            raw = redirect_to_dict.get("raw")
            redirectToSection = re.findall(
                rf"\[\[{re.escape(redirectToPage)}#(.*)\]\]", raw
            )
            # If to section or not
            redirectToSection = redirectToSection[0] if redirectToSection else None
        else:
            # Broken redirect
            # As redirectToPage = None, mention will be dropped
            pass

        return cls(
            id=data["_id"],
            title=data.get("title"),
            redirectToPage=redirectToPage,
            redirectToSection=redirectToSection,
        )
