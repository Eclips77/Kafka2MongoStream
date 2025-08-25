"""
publisher_data_analyzer.py
--------------------------
Utility module that fetches and prepares messages from the 20 Newsgroups dataset.

This module exposes a simple analyzer class that:
- Loads the 20 Newsgroups dataset categories split into "interesting" and "not_interesting".
- Samples exactly one message per category for each group.
- Returns the messages as plain dict objects ready to be published to Kafka.

Environment: No environment variables are required here.
Dependencies: scikit-learn
"""

import random
from typing import Dict, List
from sklearn.datasets import fetch_20newsgroups


INTERESTING_CATEGORIES = [
    "alt.atheism",
    "comp.graphics",
    "comp.os.ms-windows.misc",
    "comp.sys.ibm.pc.hardware",
    "comp.sys.mac.hardware",
    "comp.windows.x",
    "misc.forsale",
    "rec.autos",
    "rec.motorcycles",
    "rec.sport.baseball",
]

NOT_INTERESTING_CATEGORIES = [
    "rec.sport.hockey",
    "sci.crypt",
    "sci.electronics",
    "sci.med",
    "sci.space",
    "soc.religion.christian",
    "talk.politics.guns",
    "talk.politics.mideast",
    "talk.politics.misc",
    "talk.religion.misc",
]


class DataAnalyzer:
    """
    DataAnalyzer loads the 20 Newsgroups dataset and prepares messages for publishing.

    The class provides one public method:
        - sample_messages() -> Dict[str, List[dict]]

    The method returns a dictionary with two keys:
        {
            "interesting": [ { "category": str, "text": str }, ... 10 items ... ],
            "not_interesting": [ { "category": str, "text": str }, ... 10 items ... ]
        }

    Each list contains exactly one message sampled from each category in its group.
    """

    def __init__(self, subset: str = "train") -> None:
        """
        Initialize the analyzer.

        Args:
            subset: Which subset to use from the dataset: 'train', 'test', or 'all'.
        """
        self.subset = subset

    def _sample_one_from_category(self, category: str) -> str:
        """
        Sample a single message from a specific category.

        Args:
            category: The dataset category to sample from.

        Returns:
            A single raw text message.
        """
        ds = fetch_20newsgroups(subset=self.subset, categories=[category], remove=("headers", "footers", "quotes"))
        if not ds.data:
            return ""
        return random.choice(ds.data)

    def _build_group(self, categories: List[str]) -> List[Dict[str, str]]:
        """
        Build a list of message dicts by sampling exactly one message per category.

        Args:
            categories: List of category names.

        Returns:
            List of message dictionaries with 'category' and 'text' keys.
        """
        items: List[Dict[str, str]] = []
        for cat in categories:
            text = self._sample_one_from_category(cat) or ""
            items.append({"category": cat, "text": text})
        return items

    def sample_messages(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Sample messages for both groups: interesting and not_interesting.

        Returns:
            A dictionary with two lists of message dicts (10 per group).
        """
        return {
            "interesting": self._build_group(INTERESTING_CATEGORIES),
            "not_interesting": self._build_group(NOT_INTERESTING_CATEGORIES),
        }
