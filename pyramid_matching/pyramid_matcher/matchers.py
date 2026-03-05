from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, TypeAlias

from rapidfuzz import fuzz, process
from shapely.geometry.base import BaseGeometry

# ================================
# Domain types
# ================================

CandidateAttributes: TypeAlias = list[str]


@dataclass(frozen=True)
class MatchResult:
    """Data class to hold the result of a match operation."""

    query: str
    matched: str
    attributes: dict[str, Any]
    score: float


class BaseMatcher(ABC):
    """Abstract base class for matchers that compute similarity scores."""

    @abstractmethod
    def get_similarity(
        self, query: str | BaseGeometry, candidates: dict[str | BaseGeometry, CandidateAttributes]
    ) -> MatchResult | None:
        """Return similarity scores for the candidates."""
        pass


class FuzzyMatcher(BaseMatcher):
    """Matcher that uses fuzzy string matching to compute similarity scores."""

    def __init__(self, threshold: float = 80, scorer_name: str = "wratio"):
        self.fuzz = fuzz
        self.process = process
        self.set_scorer(scorer_name)
        self.threshold = threshold

    def set_scorer(self, scorer_name: str):
        """Set the scorer function based on the provided name."""
        scorer_name = scorer_name.lower()
        if scorer_name == "ratio":
            self.scorer = self.fuzz.ratio
        elif scorer_name == "partial_ratio":
            self.scorer = self.fuzz.partial_ratio
        elif scorer_name == "token_sort_ratio":
            self.scorer = self.fuzz.token_sort_ratio
        elif scorer_name == "token_set_ratio":
            self.scorer = self.fuzz.token_set_ratio
        elif scorer_name == "wratio":
            self.scorer = self.fuzz.WRatio
        else:
            raise ValueError(f"Unknown scorer: {scorer_name}")

    def set_threshold(self, threshold: float):
        """Set the similarity threshold for matches."""
        self.threshold = threshold

    def get_similarity(
        self, query: str, candidates: dict[str, CandidateAttributes]
    ) -> MatchResult | None:
        """Return the best fuzzy match among candidates if above threshold.

        candidates
            A mapping of candidate names to their associated attributes.

            Each key is the human-readable name of an entity (e.g. a health zone,
            district, or administrative unit), and the value is a list of attributes
            linked to that entity, typically IDs in the candidate pyramid.

        Example:
            {
                "MAINDOMBE": ["u0vP3ZicczY"],
                "TANGANYIKA": ["hyvduSNKvfe"],
                "BAS UELE": ["rWrCdr321Qu"],
            }

        Returns:
            dict: A dictionary with the best match and its score, or None
            if no match meets the threshold.
        """
        candidate_strings = list(candidates.keys())
        best_match = self.process.extractOne(query, candidate_strings, scorer=self.scorer)

        if best_match is None:
            return None

        if best_match[1] >= self.threshold:
            match_str = best_match[0]
            attributes_pyramid = candidates[match_str]
            score = best_match[1]
            return MatchResult(
                query=query,
                matched=match_str,
                attributes=attributes_pyramid,
                score=score,
            )

        return None

    def __str__(self) -> str:
        return f"FuzzyMatcher(scorer: {self.scorer.__name__})"


class SentenceTransformerMatcher(BaseMatcher):
    """Matcher that uses sentence transformers to compute similarity scores.

    NOTE: Not yet implemented.
    """

    def __init__(self, model_name: str | None = "sentence-transformers/all-MiniLM-L6-v2"):
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415

        model_name = "sentence-transformers/all-MiniLM-L6-v2"
        self.model = SentenceTransformer(model_name)

    def get_similarity(
        self, query: str | BaseGeometry, candidates: list[str | BaseGeometry]
    ) -> MatchResult | None:
        # from sentence_transformers.util import cos_sim
        # query_emb = self.model.encode(query, convert_to_tensor=True)
        # cand_embs = self.model.encode(candidates, convert_to_tensor=True)
        # scores = cos_sim(query_emb, cand_embs)[0].cpu().numpy()
        """Return similarity scores for the candidates using sentence transformers."""
        raise NotImplementedError("SentenceTransformerMatcher.get_similarity is not implemented.")

    def __str__(self) -> str:
        return f"TransformerMatcher(scorer: {self.model.__name__})"


class GeometryMatcher(BaseMatcher):
    """Match org units using spatial proximity and overlap.

    NOTE: Not yet implemented. This is a test implementation.
    """

    def __init__(
        self,
        max_distance: float = 10_000,  # meters
        use_overlap: bool = True,
        overlap_weight: float = 0.3,
    ):
        self.max_distance = max_distance
        self.use_overlap = use_overlap
        self.overlap_weight = overlap_weight

    def get_similarity(
        self,
        query: BaseGeometry,
        candidates: dict[BaseGeometry, CandidateAttributes],
    ) -> MatchResult | None:
        """A ChatGPT produced code. It is not yet tested and may contain errors.

        candidates:
            {
                "ou_id_1": {"geometry": BaseGeometry, ...},
                "ou_id_2": {"geometry": BaseGeometry, ...},
            }

        Returns:
            The best spatial match among candidates if within max_distance, optionally
              considering overlap.
        """
        best_id = None
        best_score = 0.0

        for ou_id, attrs in candidates.items():
            cand_geom = attrs["geometry"]
            score = self._score(query, cand_geom)

            if score is not None and score > best_score:
                best_score = score
                best_id = ou_id

        if best_id is None:
            return None

        return MatchResult(
            query=self._geom_id(query),
            matched=best_id,
            score=best_score,
            attributes=candidates[best_id],
        )

    def _score(self, ref: BaseGeometry, cand: BaseGeometry) -> float | None:
        distance = ref.distance(cand)

        if distance > self.max_distance:
            return None

        # Normalize distance into [0,1]
        distance_score = 1.0 - (distance / self.max_distance)

        overlap_score = 0.0
        if self.use_overlap and ref.geom_type == "Polygon" and cand.geom_type == "Polygon":
            inter = ref.intersection(cand).area
            union = ref.union(cand).area
            if union > 0:
                overlap_score = inter / union

        # Final weighted score
        return (1 - self.overlap_weight) * distance_score + self.overlap_weight * overlap_score

    def _geom_id(self, geom: BaseGeometry) -> str:
        """Return an identifier for the query geometry.

        Returns:
          the 'id' attribute if it exists, otherwise returns a default string.
        """
        return getattr(geom, "id", "query")

    def __str__(self) -> str:
        return f"GeometryMatcher(distance: {self.scorer.__name__})"
