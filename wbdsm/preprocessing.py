import unicodedata
import re
import unidecode
from urllib.parse import quote


def decode_id(text: str) -> str:
    """
    - Replace mongodb conversions
    - normalize data to have unique types of characters for each language
    """
    # Normalize data (doing it before indexing with wtf_wikipedia)

    text = unicodedata.normalize("NFKD", text)

    # Threat mongodb conversions
    # https://stackoverflow.com/questions/12397118/mongodb-dot-in-key-name/30254815#30254815
    text = (
        text.replace("\\\\", "\\")
        .replace("\\u0024", "$")
        .replace("\\u002e", ".")
        .replace("&quot;", '"')
        .replace("&amp;", "&")
    )

    return text


def encode_id(text: str, upper_case_first_letter: bool = True, encode_title=False):
    """
    Opposite of decode_id for the mongodb conversions, but not for normalization.
    Also makes upper case the first letter of the title (as in wikipedia/wtf_wikipedia indexing)
    """
    # Normalize data (doing it before indexing with wtf_wikipedia)
    text = unicodedata.normalize("NFKD", text)
    # Don't upper case for section names
    if upper_case_first_letter:
        # Wikimedia links are case insensitive for the first char and sensitive for the rest
        # Titles always start with an uppercase letter
        if len(text) == 0:
            return text
        elif len(text) == 1:
            return text.upper()
        text = text[0].upper() + text[1:]
    # Redo mongodb conversions
    if not encode_title:
        text = (
            text.replace("\\", "\\\\").replace("$", "\\u0024").replace(".", "\\u002e")
        )

    text = text.replace("&", "&amp;").replace('"', "&quot;")
    text = text.strip()

    return text


def clean_text(text: str) -> str:
    """
    Clean text output from wtf_wikipedia.

    Needs to:
    - Remove new lines to spaces
    - remove extra spaces
    - remove bold and italics (COMMENTED OUT, leaves to training time dataloader to decide)
    - fix \` and \' (mongo db or wikipedia?)
    - Fix bad parsing resulting in empty parenthesis, brackets, etc.
    - Fix bad parsings resulting in double , and . with spaces in between.
    """
    # Decode text # ?
    text = decode_id(text)
    # Replace new lines with spaces
    text = re.sub(r"\n", " ", text)
    # wtf_wikipedia generates extra spaces when a link is in a new line and when fails to parse a link
    text = re.sub(r" +", " ", text).strip()
    # Remove bold and italics
    # text = re.sub(r"'''|''", "", text)
    # Fix \` and \'
    text = re.sub(r"\\`", "`", text)
    text = re.sub(r"\\'", "'", text)
    # From bad parsing
    # Remove empty parenthesis
    text = re.sub(r"\(\s*\)", "", text)
    # Remove empty brackets
    text = re.sub(r"\[\s*\]", "", text)
    # Remove empty braces
    text = re.sub(r"\{\s*\}", "", text)
    # Remove double , and .
    text = re.sub(r",\s*,", ",", text)
    text = re.sub(r"\.\s*\.", ".", text)
    return text


def normalize_punctuation(text: str):
    # Decode only punctuation and quotes. https://www.compart.com/en/unicode/category
    text = "".join(
        [
            unidecode.unidecode(char)
            if unicodedata.category(char)[0]
            in [
                "Po",
                "Cc",
                "Pi",
                "Pf",
                "Ps",
            ]
            else char
            for char in text
        ]
    )

    return text


def encode_to_url(text: str):
    return quote(text, safe="")
