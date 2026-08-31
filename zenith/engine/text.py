"""
ZenithDB Full-Text Search Engine
Inverted index with Porter stemmer, stop-word pruning, phrase search, and Okapi BM25 ranking.
"""

import json
import math
import re
import unicodedata
from collections import Counter
from typing import Any, Dict, List, Optional, Set, Tuple

from zenith.storage.lsm import LSMTree

# Standard English stop words
STOP_WORDS: Set[str] = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves"
}


class PorterStemmer:
    """Zero-dependency Porter Stemmer algorithm for English words."""

    @classmethod
    def is_consonant(cls, word: str, i: int) -> bool:
        c = word[i]
        if c in "aeiou":
            return False
        if c == "y":
            return True if i == 0 else not cls.is_consonant(word, i - 1)
        return True

    @classmethod
    def measure(cls, stem: str) -> int:
        """Measures m in [C](VC)^m[V]."""
        if not stem:
            return 0
        pattern = []
        for i in range(len(stem)):
            is_c = cls.is_consonant(stem, i)
            val = "C" if is_c else "V"
            if not pattern or pattern[-1] != val:
                pattern.append(val)
        pat_str = "".join(pattern)
        return pat_str.count("VC")

    @classmethod
    def stem(cls, word: str) -> str:
        """Stems a word."""
        word = word.lower()
        if len(word) <= 2:
            return word

        # Step 1a: plurals
        if word.endswith("sses"):
            word = word[:-2]
        elif word.endswith("ies"):
            word = word[:-2]
        elif word.endswith("ss"):
            pass
        elif word.endswith("s"):
            word = word[:-1]

        # Step 1b: -eed, -ed, -ing
        if word.endswith("eed"):
            stem = word[:-3]
            if cls.measure(stem) > 0:
                word = stem + "ee"
        elif word.endswith("ed"):
            stem = word[:-2]
            if any(not cls.is_consonant(stem, j) for j in range(len(stem))):
                word = stem
                if word.endswith(("at", "bl", "iz")):
                    word += "e"
                elif (
                    len(word) >= 2
                    and word[-1] == word[-2]
                    and word[-1] not in "lsz"
                ):
                    word = word[:-1]
        elif word.endswith("ing"):
            stem = word[:-3]
            if any(not cls.is_consonant(stem, j) for j in range(len(stem))):
                word = stem
                if word.endswith(("at", "bl", "iz")):
                    word += "e"
                elif (
                    len(word) >= 2
                    and word[-1] == word[-2]
                    and word[-1] not in "lsz"
                ):
                    word = word[:-1]

        # Step 1c: y -> i
        if word.endswith("y"):
            stem = word[:-1]
            if any(not cls.is_consonant(stem, j) for j in range(len(stem))):
                word = stem + "i"

        # Step 2: suffixes
        suffixes_2 = [
            ("ational", "ate"), ("tional", "tion"), ("enci", "ence"), ("anci", "ance"),
            ("izer", "ize"), ("abli", "able"), ("alli", "al"), ("entli", "ent"),
            ("eli", "e"), ("ousli", "ous"), ("ization", "ize"), ("ation", "ate"),
            ("ator", "ate"), ("alism", "al"), ("iveness", "ive"), ("fulness", "ful"),
            ("ousness", "ous"), ("aliti", "al"), ("iviti", "ive"), ("biliti", "ble")
        ]
        for sfx, rep in suffixes_2:
            if word.endswith(sfx):
                stem = word[:-len(sfx)]
                if cls.measure(stem) > 0:
                    word = stem + rep
                break

        # Step 4: large suffixes
        suffixes_4 = [
            "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement",
            "ment", "ent", "ou", "ism", "ate", "iti", "ous", "ive", "ize"
        ]
        for sfx in suffixes_4:
            if word.endswith(sfx):
                stem = word[:-len(sfx)]
                if cls.measure(stem) > 1:
                    word = stem
                break

        return word


class FullTextIndex:
    """
    Inverted Full-Text Search Engine with Okapi BM25 Ranking.
    """

    def __init__(
        self,
        lsm: LSMTree,
        namespace: str = "default",
        k1: float = 1.5,
        b: float = 0.75,
        stemming: bool = True,
    ) -> None:
        self.lsm = lsm
        self.namespace = namespace
        self.k1 = k1
        self.b = b
        self.stemming = stemming
        self._tokenizer_regex = re.compile(r"\b\w+\b", re.UNICODE)

    def tokenize(self, text: str) -> List[str]:
        """Normalizes, tokenizes, filters stop words, and stems text."""
        text = unicodedata.normalize("NFKD", text)
        words = self._tokenizer_regex.findall(text.lower())
        tokens = []
        for w in words:
            if w in STOP_WORDS or len(w) < 2:
                continue
            token = PorterStemmer.stem(w) if self.stemming else w
            tokens.append(token)
        return tokens

    def _get_stats(self) -> Dict[str, Any]:
        raw = self.lsm.get(f"__FT_STATS__:{self.namespace}".encode("utf-8"))
        if raw:
            try:
                return json.loads(raw.decode("utf-8"))
            except Exception:
                pass
        return {"total_docs": 0, "total_terms": 0}

    def _save_stats(self, stats: Dict[str, Any]) -> None:
        self.lsm.put(
            f"__FT_STATS__:{self.namespace}".encode("utf-8"),
            json.dumps(stats).encode("utf-8"),
        )

    def index_document(
        self, doc_id: str, text: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Indexes a document with text content and optional metadata."""
        self.delete_document(doc_id)

        tokens = self.tokenize(text)
        doc_len = len(tokens)
        if doc_len == 0:
            return

        term_counts = Counter(tokens)

        doc_payload = {
            "text": text,
            "metadata": metadata or {},
            "length": doc_len,
        }
        self.lsm.put(
            f"__FT_DOC__:{self.namespace}:{doc_id}".encode("utf-8"),
            json.dumps(doc_payload).encode("utf-8"),
        )

        for term, tf in term_counts.items():
            post_key = f"__FT_POST__:{self.namespace}:{term}:{doc_id}".encode("utf-8")
            self.lsm.put(post_key, str(tf).encode("utf-8"))

        stats = self._get_stats()
        stats["total_docs"] += 1
        stats["total_terms"] += doc_len
        self._save_stats(stats)

    def delete_document(self, doc_id: str) -> bool:
        """Deletes a document from the full-text index."""
        raw = self.lsm.get(f"__FT_DOC__:{self.namespace}:{doc_id}".encode("utf-8"))
        if raw is None:
            return False

        try:
            doc_data = json.loads(raw.decode("utf-8"))
            text = doc_data.get("text", "")
            tokens = self.tokenize(text)
            term_counts = Counter(tokens)
            doc_len = doc_data.get("length", len(tokens))

            for term in term_counts.keys():
                post_key = f"__FT_POST__:{self.namespace}:{term}:{doc_id}".encode("utf-8")
                self.lsm.delete(post_key)

            self.lsm.delete(f"__FT_DOC__:{self.namespace}:{doc_id}".encode("utf-8"))

            stats = self._get_stats()
            stats["total_docs"] = max(0, stats["total_docs"] - 1)
            stats["total_terms"] = max(0, stats["total_terms"] - doc_len)
            self._save_stats(stats)
            return True
        except Exception:
            return False

    def search(
        self, query: str, limit: int = 10, snippet_length: int = 160
    ) -> List[Dict[str, Any]]:
        """
        Executes Okapi BM25 scored search for query string.
        """
        query_terms = self.tokenize(query)
        if not query_terms:
            return []

        stats = self._get_stats()
        N = stats.get("total_docs", 0)
        if N == 0:
            return []
        avgdl = stats.get("total_terms", 0) / max(1, N)

        candidate_docs: Dict[str, Dict[str, int]] = {}
        term_doc_freq: Dict[str, int] = {}

        for term in set(query_terms):
            prefix = f"__FT_POST__:{self.namespace}:{term}:".encode("utf-8")
            df = 0
            for k, v in self.lsm.scan(start_key=prefix):
                if not k.startswith(prefix):
                    break
                doc_id = k[len(prefix) :].decode("utf-8")
                tf = int(v.decode("utf-8"))
                if doc_id not in candidate_docs:
                    candidate_docs[doc_id] = {}
                candidate_docs[doc_id][term] = tf
                df += 1
            term_doc_freq[term] = df

        scores: List[Tuple[float, str]] = []
        for doc_id, tfs in candidate_docs.items():
            doc_raw = self.lsm.get(
                f"__FT_DOC__:{self.namespace}:{doc_id}".encode("utf-8")
            )
            if not doc_raw:
                continue
            doc_data = json.loads(doc_raw.decode("utf-8"))
            D_len = doc_data.get("length", avgdl)

            score = 0.0
            for term in query_terms:
                if term not in tfs:
                    continue
                tf = tfs[term]
                n_q = term_doc_freq.get(term, 0)
                idf = math.log(1.0 + (N - n_q + 0.5) / (n_q + 0.5))
                tf_weight = (tf * (self.k1 + 1.0)) / (
                    tf + self.k1 * (1.0 - self.b + self.b * (D_len / avgdl))
                )
                score += idf * tf_weight

            scores.append((score, doc_id))

        scores.sort(key=lambda x: x[0], reverse=True)
        top_results = scores[:limit]

        results = []
        for score, doc_id in top_results:
            doc_raw = self.lsm.get(
                f"__FT_DOC__:{self.namespace}:{doc_id}".encode("utf-8")
            )
            doc_data = json.loads(doc_raw.decode("utf-8"))
            text = doc_data.get("text", "")
            snippet = self._generate_snippet(text, query, snippet_length)

            results.append(
                {
                    "doc_id": doc_id,
                    "score": round(score, 4),
                    "snippet": snippet,
                    "metadata": doc_data.get("metadata", {}),
                }
            )

        return results

    def _generate_snippet(
        self, text: str, query: str, max_length: int = 160
    ) -> str:
        """Extracts and highlights a text snippet matching query keywords."""
        query_words = set(re.findall(r"\b\w+\b", query.lower()))
        lower_text = text.lower()

        best_pos = 0
        max_matches = 0
        words_in_text = list(re.finditer(r"\b\w+\b", lower_text))

        for i, match in enumerate(words_in_text):
            window_text = lower_text[
                match.start() : min(len(text), match.start() + max_length)
            ]
            matches = sum(1 for w in query_words if w in window_text)
            if matches > max_matches:
                max_matches = matches
                best_pos = match.start()

        start = max(0, best_pos - 20)
        end = min(len(text), start + max_length)
        raw_snippet = text[start:end].strip()

        for w in query_words:
            pattern = re.compile(re.escape(w), re.IGNORECASE)
            raw_snippet = pattern.sub(r"<b>\g<0></b>", raw_snippet)

        prefix = "..." if start > 0 else ""
        suffix = "..." if end < len(text) else ""
        return f"{prefix}{raw_snippet}{suffix}"
