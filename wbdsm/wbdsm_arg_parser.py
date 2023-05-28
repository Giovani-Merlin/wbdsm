import argparse


def boolean_string(s):
    if s not in {"False", "True"}:
        raise ValueError("Not a valid boolean string")
    return s == "True" or s == "true"


class WBDSMArgParser(argparse.ArgumentParser):
    def __init__(self):
        super().__init__(
            description="self for wikipedia dataset generation",
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
        #
        self.add_argument(
            "--max_chars",
            default=2048,
            type=int,
            help="Max chars to keep for candidate's description and for query left + right",
            metavar="\b",
        )
        self.add_argument(
            "--candidates_size",
            default=10**5,
            type=int,
            help="Number of candidates to keep for hard negatives and for full evaluation, 0 to keep all",
            metavar="\b",
        )
        self.add_argument(
            "--train_size",
            default=10**5,
            type=int,
            help="Size of the generated train dataset",
            metavar="\b",
        )
        self.add_argument(
            "--test_size",
            default=10**4,
            type=int,
            help="Size of the generated test dataset",
            metavar="\b",
        )
        self.add_argument(
            "--validation_size",
            default=10**4,
            type=int,
            help="Size of the generated validation dataset",
            metavar="\b",
        )

        self.add_argument(
            "--output_path",
            default="data/bef_format",
            help="Path to the output directory",
            metavar="\b",
        )
        self.add_argument(
            "--candidate_text_surfaces",
            default=5,
            type=int,
            help="Occurrences of different text surfaces for the same candidate",
            metavar="\b",
        )
        self.add_argument(
            "--candidate_surface_appearance",
            default=10,
            type=int,
            help="Occurrences of the same text surface of the same candidate",
            metavar="\b",
        )
        self.add_argument(
            "--max_rank",
            default=100 * 10**3,
            type=int,
            help="Max rank of the entity, 0 for using all",
            metavar="\b",
        )
        self.add_argument(
            "--sample_size",
            default=1e7,
            type=int,
            help="Number of samples to use for generating the dataset. A higher number will result in a more accurate dataset, but will take longer to generate",
            metavar="\b",
        )
