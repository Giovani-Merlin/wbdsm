import argparse


def boolean_string(s):
    if s not in {"False", "True"}:
        raise ValueError("Not a valid boolean string")
    return s == "True" or s == "true"


class WBDSMArgsParser(argparse.ArgumentParser):
    def __init__(self):
        super().__init__(
            description="Parser for wikipedia dataset generation",
            conflict_handler="resolve",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )

        self.add_argument("--language", type=str)
        self.add_argument(
            "--mongo_uri",
            help="Path to the wiki database",
            metavar="\b",
            default="mongodb://localhost:27017",
            type=str,
        )
        self.add_argument(
            "--last_pageID",
            type=str,
            default=None,
            help="Last pageID to parse - in case of interruption",
        )
