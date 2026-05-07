import polars as pl

from pyramid_matcher.matchers import MatchResult

# ================================
# Pyramid DataFrames
# ================================

# Hierarchy (10 rows):
#   level_1: TSHUAPA, HAUT LOMAMI
#   level_2: TSHUAPA → BOENDE, BOKUNGU       | HAUT LOMAMI → KABALO, MALEMBA NKULU
#   level_3: BOENDE  → BONGANDANGA, BEFALE   | KABALO      → KABALO TERRITOIRE, NYUNZU
#            BOKUNGU → DJOLU                 | MALEMBA NKULU → MALEMBA TERRITOIRE
#   level_4: BONGANDANGA → HZ NORD, HZ SUD   | KABALO TERRITOIRE → HZ NORD, HZ SUD
#            (others have one health zone each)
#   level_5: HZ BONGANDANGA NORD → HA N1, HA N2 | HZ KABALO NORD → HA N1, HA N2
#            (others have one health area each)

reference_pyramid = pl.DataFrame(
    {
        "level_1_name": ["TSHUAPA"] * 5 + ["HAUT LOMAMI"] * 5,
        "level_1_id": ["ref_id_1"] * 5 + ["ref_id_2"] * 5,
        "level_2_name": ["BOENDE"] * 4
        + ["BOKUNGU"]
        + ["KABALO"] * 4
        + ["MALEMBA NKULU"],
        "level_2_id": ["ref_id_3"] * 4 + ["ref_id_4"] + ["ref_id_5"] * 4 + ["ref_id_6"],
        "level_3_name": ["BONGANDANGA"] * 3
        + ["BEFALE"]
        + ["DJOLU"]
        + ["KABALO TERRITOIRE"] * 3
        + ["NYUNZU"]
        + ["MALEMBA TERRITOIRE"],
        "level_3_id": ["ref_id_7"] * 3
        + ["ref_id_8"]
        + ["ref_id_9"]
        + ["ref_id_10"] * 3
        + ["ref_id_11"]
        + ["ref_id_12"],
        "level_4_name": ["HZ BONGANDANGA NORD"] * 2
        + ["HZ BONGANDANGA SUD"]
        + ["HZ BEFALE"]
        + ["HZ DJOLU"]
        + ["HZ KABALO NORD"] * 2
        + ["HZ KABALO SUD"]
        + ["HZ NYUNZU"]
        + ["HZ MALEMBA"],
        "level_4_id": ["ref_id_13"] * 2
        + ["ref_id_14"]
        + ["ref_id_15"]
        + ["ref_id_16"]
        + ["ref_id_17"] * 2
        + ["ref_id_18"]
        + ["ref_id_19"]
        + ["ref_id_20"],
        "level_5_name": [
            "HA BONGANDANGA N1",
            "HA BONGANDANGA N2",
            "HA BONGANDANGA S1",
            "HA BEFALE 1",
            "HA DJOLU 1",
            "HA KABALO N1",
            "HA KABALO N2",
            "HA KABALO S1",
            "HA NYUNZU 1",
            "HA MALEMBA 1",
        ],
        "level_5_id": [f"ref_id_{i}" for i in range(21, 31)],
        # --- attributes (5 columns, unique to reference pyramid) ---
        # level_1: 1 attr | level_2: 1 attr | level_3: 1 attr | level_4: 0 | level_5: 2 attrs
        "level_1_region": ["EQUATEUR PROVINCE"] * 5 + ["KATANGA PROVINCE"] * 5,
        "level_2_code": ["BOE-01"] * 4 + ["BOK-01"] + ["KAB-01"] * 4 + ["MAL-01"],
        "level_3_chief_town": ["BONGANDANGA CENTRE"] * 3
        + ["BEFALE CENTRE"]
        + ["DJOLU CENTRE"]
        + ["KABALO CENTRE"] * 3
        + ["NYUNZU CENTRE"]
        + ["MALEMBA CENTRE"],
        "level_5_type": ["RURAL"] * 9 + ["URBAN"],
        "level_5_settlement": [
            "DISPERSED",
            "DISPERSED",
            "GROUPED",
            "GROUPED",
            "DISPERSED",
            "DISPERSED",
            "DISPERSED",
            "GROUPED",
            "GROUPED",
            "DISPERSED",
        ],
    }
)

candidate_pyramid = pl.DataFrame(
    {
        "level_1_name": ["TSHUAPA"] * 5 + ["HAUT LOMAMI"] * 5,
        "level_1_id": ["can_id_1"] * 5 + ["can_id_2"] * 5,
        "level_2_name": ["BOENDE"] * 4
        + ["BOKUNGU"]
        + ["KABALO"] * 4
        + ["MALEMBA NKULU"],
        "level_2_id": ["can_id_3"] * 4 + ["can_id_4"] + ["can_id_5"] * 4 + ["can_id_6"],
        "level_3_name": ["BONGANDANGA"] * 3
        + ["BEFALE"]
        + ["DJOLU"]
        + ["KABALO TERRITOIRE"] * 3
        + ["NYUNZU"]
        + ["MALEMBA TERRITOIRE"],
        "level_3_id": ["can_id_7"] * 3
        + ["can_id_8"]
        + ["can_id_9"]
        + ["can_id_10"] * 3
        + ["can_id_11"]
        + ["can_id_12"],
        "level_4_name": ["HZ BONGANDANGA NORD"] * 2
        + ["HZ BONGANDANGA SUD"]
        + ["HZ BEFALE"]
        + ["HZ DJOLU"]
        + ["HZ KABALO NORD"] * 2
        + ["HZ KABALO SUD"]
        + ["HZ NYUNZU"]
        + ["HZ MALEMBA"],
        "level_4_id": ["can_id_13"] * 2
        + ["can_id_14"]
        + ["can_id_15"]
        + ["can_id_16"]
        + ["can_id_17"] * 2
        + ["can_id_18"]
        + ["can_id_19"]
        + ["can_id_20"],
        "level_5_name": [
            "HA BONGANDANGA N1",
            "HA BONGANDANGA N2",
            "HA BONGANDANGA S1",
            "HA BEFALE 1",
            "HA DJOLU 1",
            "HA KABALO N1",
            "HA KABALO N2",
            "HA KABALO S1",
            "HA NYUNZU 1",
            "HA MALEMBA 1",
        ],
        "level_5_id": [f"can_id_{i}" for i in range(21, 31)],
        # --- attributes (5 columns, unique to candidate pyramid) ---
        # level_1: 1 attr | level_2: 1 attr | level_3: 1 attr | level_4: 0 | level_5: 2 attrs
        "level_1_source": ["SRC-A"] * 5 + ["SRC-B"] * 5,
        "level_2_abbreviation": ["BDE"] * 4 + ["BKG"] + ["KBL"] * 4 + ["MLM"],
        "level_3_population_group": ["PG-NORTH"] * 3
        + ["PG-WEST"]
        + ["PG-EAST"]
        + ["PG-SOUTH"] * 3
        + ["PG-EAST"]
        + ["PG-WEST"],
        "level_5_urban_rural": ["RURAL"] * 9 + ["URBAN"],
        "level_5_facility_count": ["1", "1", "2", "1", "1", "1", "1", "2", "1", "1"],
    }
)

reference_pyramid_with_duplicates = pl.concat(
    [reference_pyramid, reference_pyramid.head(1)]
)

candidate_pyramid_with_duplicates = pl.concat(
    [candidate_pyramid, candidate_pyramid.head(1)]
)

reference_pyramid_no_level_cols = pl.DataFrame(
    {
        "name": ["TSHUAPA", "HAUT LOMAMI"],
        "id": ["ref_id_1", "ref_id_2"],
    }
)

reference_pyramid_pandas = reference_pyramid.to_pandas()
candidate_pyramid_pandas = candidate_pyramid.to_pandas()

reference_pyramid_one_level = reference_pyramid.select(
    ["level_1_name", "level_1_id"]
).unique()

candidate_pyramid_one_level = candidate_pyramid.select(
    ["level_1_name", "level_1_id"]
).unique()

# candidate_pyramid_no_overlap: has level_ columns but none share a level name with reference
# (level_a vs level_1..5) — used to test empty intersection and missing-column errors.
candidate_pyramid_no_overlap = pl.DataFrame(
    {
        "level_a_name": ["ZONE_A"],
        "level_a_id": ["no_id_1"],
    }
)

# candidate_pyramid_partial: only levels 1–3 — used to test partial intersection detection.
candidate_pyramid_partial = candidate_pyramid.select(
    [
        c
        for c in candidate_pyramid.columns
        if any(c.startswith(f"level_{n}_") for n in [1, 2, 3])
    ]
)


# ================================
# _match_level_group inputs and expected outputs
# ================================

match_level_group_schema_level1 = [
    "candidate_level_1_name",
    "reference_level_1_name",
    "reference_level_1_id",
    "score_level_1",
    "candidate_level_1_id",
]

match_level_group_attributes_level1 = {
    "reference": ["level_1_id"],
    "candidate": ["level_1_id"],
}

match_level_group_df_matches_all_level1 = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA", "HAUT LOMAMI"],
        "reference_level_1_name": ["TSHUAPA", "HAUT LOMAMI"],
        "reference_level_1_id": ["ref_id_1", "ref_id_2"],
        "score_level_1": [100.0, 100.0],
        "candidate_level_1_id": ["can_id_1", "can_id_2"],
    }
)


match_level_group_unmatched_ref_when_tshuapa_matched = (
    reference_pyramid_one_level.filter(pl.col("level_1_name") != "TSHUAPA")
)

match_level_group_unmatched_can_when_tshuapa_matched = (
    candidate_pyramid_one_level.filter(pl.col("level_1_name") != "TSHUAPA")
)

match_results_level1 = {
    "TSHUAPA": MatchResult(
        query="TSHUAPA", matched="TSHUAPA", attributes=["ref_id_1"], score=100.0
    ),
    "HAUT LOMAMI": MatchResult(
        query="HAUT LOMAMI", matched="HAUT LOMAMI", attributes=["ref_id_2"], score=100.0
    ),
}


# ================================
# _add_already_matched_levels inputs and expected outputs
# ================================

add_already_matched_input_df = pl.DataFrame(
    {
        "candidate_level_2_name": ["BOENDE", "BOKUNGU"],
        "reference_level_2_name": ["BOENDE", "BOENDE"],
    }
)

add_already_matched_row_one_level = {
    "candidate_level_1_name": "TSHUAPA",
    "reference_level_1_name": "TSHUAPA",
}

add_already_matched_expected_one_level = pl.DataFrame(
    {
        "candidate_level_2_name": ["BOENDE", "BOKUNGU"],
        "reference_level_2_name": ["BOENDE", "BOENDE"],
        "candidate_level_1_name": ["TSHUAPA", "TSHUAPA"],
        "reference_level_1_name": ["TSHUAPA", "TSHUAPA"],
    }
)

add_already_matched_input_df_level3 = pl.DataFrame(
    {
        "candidate_level_3_name": ["BONGANDANGA", "BEFALE"],
        "reference_level_3_name": ["BONGANDANGA", "BEFALE"],
    }
)

add_already_matched_row_two_levels = {
    "candidate_level_1_name": "TSHUAPA",
    "reference_level_1_name": "TSHUAPA",
    "candidate_level_2_name": "BOENDE",
    "reference_level_2_name": "BOENDE",
}

add_already_matched_expected_two_levels = pl.DataFrame(
    {
        "candidate_level_3_name": ["BONGANDANGA", "BEFALE"],
        "reference_level_3_name": ["BONGANDANGA", "BEFALE"],
        "candidate_level_1_name": ["TSHUAPA", "TSHUAPA"],
        "reference_level_1_name": ["TSHUAPA", "TSHUAPA"],
        "candidate_level_2_name": ["BOENDE", "BOENDE"],
        "reference_level_2_name": ["BOENDE", "BOENDE"],
    }
)


# ================================
# _select_group expected outputs
# ================================

select_group_ref_level1_tshuapa = reference_pyramid.filter(
    pl.col("level_1_name") == "TSHUAPA"
)
select_group_can_level1_haut_lomami = candidate_pyramid.filter(
    pl.col("level_1_name") == "HAUT LOMAMI"
)
select_group_ref_level1_tshuapa_level2_boende = reference_pyramid.filter(
    (pl.col("level_1_name") == "TSHUAPA") & (pl.col("level_2_name") == "BOENDE")
)
select_group_can_level1_tshuapa_level2_boende = candidate_pyramid.filter(
    (pl.col("level_1_name") == "TSHUAPA") & (pl.col("level_2_name") == "BOENDE")
)


# ================================
# _match_level inputs and expected outputs
# ================================

# already_matched input: re-uses match_level_group_df_matches_all_level1 (2 rows, one per level_1 province)

match_level_single_match_level2 = pl.DataFrame(
    {
        "candidate_level_2_name": ["BOENDE"],
        "reference_level_2_name": ["BOENDE"],
        "score_level_2": [100.0],
    }
)


# ================================
# _add_repeated_matches inputs and expected outputs
# ================================

# No repeated reference: each reference maps to exactly one candidate.
# Derived from match_level_group_df_matches_all_level1 (id columns not needed here).
add_repeated_matches_no_repeats = match_level_group_df_matches_all_level1.select(
    ["candidate_level_1_name", "reference_level_1_name", "score_level_1"]
)

add_repeated_matches_no_repeats_expected = add_repeated_matches_no_repeats.with_columns(
    pl.lit(False).alias("repeated_matches_level_1")
)

# Mixed: TSHUAPA (reference) matched to two different candidates; HAUT LOMAMI matched to one.
add_repeated_matches_with_repeats = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA", "TSHUAPA ALT", "HAUT LOMAMI"],
        "reference_level_1_name": ["TSHUAPA", "TSHUAPA", "HAUT LOMAMI"],
        "score_level_1": [100.0, 85.0, 100.0],
    }
)

add_repeated_matches_with_repeats_expected = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA", "TSHUAPA ALT", "HAUT LOMAMI"],
        "reference_level_1_name": ["TSHUAPA", "TSHUAPA", "HAUT LOMAMI"],
        "score_level_1": [100.0, 85.0, 100.0],
        "repeated_matches_level_1": [True, True, False],
    }
)

# Upper levels: BOENDE (level_2 reference) appears under both TSHUAPA and HAUT LOMAMI (level_1).
# Only the TSHUAPA group has two distinct candidates for BOENDE → repeated.
# The HAUT LOMAMI group has a single candidate for BOENDE → not repeated.
add_repeated_matches_upper_levels_input = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA", "TSHUAPA", "HAUT LOMAMI"],
        "reference_level_1_name": ["TSHUAPA", "TSHUAPA", "HAUT LOMAMI"],
        "score_level_1": [100.0, 100.0, 100.0],
        "candidate_level_2_name": ["BOENDE", "BOENDE ALT", "BOENDE"],
        "reference_level_2_name": ["BOENDE", "BOENDE", "BOENDE"],
        "score_level_2": [100.0, 85.0, 100.0],
    }
)

add_repeated_matches_upper_levels_expected = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA", "TSHUAPA", "HAUT LOMAMI"],
        "reference_level_1_name": ["TSHUAPA", "TSHUAPA", "HAUT LOMAMI"],
        "score_level_1": [100.0, 100.0, 100.0],
        "candidate_level_2_name": ["BOENDE", "BOENDE ALT", "BOENDE"],
        "reference_level_2_name": ["BOENDE", "BOENDE", "BOENDE"],
        "score_level_2": [100.0, 85.0, 100.0],
        "repeated_matches_level_2": [True, True, False],
    }
)


# ================================
# _reorder_match_columns inputs and expected outputs
# ================================

# Single level, no attributes.
reorder_match_columns_one_level_no_attrs_input = pl.DataFrame(
    {
        "repeated_matches_level_1": [False, False],
        "score_level_1": [100.0, 100.0],
        "reference_level_1_name": ["TSHUAPA", "HAUT LOMAMI"],
        "candidate_level_1_name": ["TSHUAPA", "HAUT LOMAMI"],
    }
)

reorder_match_columns_one_level_no_attrs_attributes = {
    "level_1": {"reference": [], "candidate": []},
}

reorder_match_columns_one_level_no_attrs_full = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA", "HAUT LOMAMI"],
        "reference_level_1_name": ["TSHUAPA", "HAUT LOMAMI"],
        "score_level_1": [100.0, 100.0],
        "repeated_matches_level_1": [False, False],
    }
)

reorder_match_columns_one_level_no_attrs_simple = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA", "HAUT LOMAMI"],
        "reference_level_1_name": ["TSHUAPA", "HAUT LOMAMI"],
    }
)

# Single level, with reference and candidate attributes.
reorder_match_columns_one_level_with_attrs_input = pl.DataFrame(
    {
        "repeated_matches_level_1": [False],
        "score_level_1": [100.0],
        "reference_level_1_name": ["TSHUAPA"],
        "candidate_level_1_name": ["TSHUAPA"],
        "reference_level_1_id": ["ref_id_1"],
        "candidate_level_1_id": ["can_id_1"],
        "reference_level_1_region": ["EQUATEUR PROVINCE"],
        "candidate_level_1_source": ["SRC-A"],
    }
)

reorder_match_columns_one_level_with_attrs_attributes = {
    "level_1": {
        "reference": ["level_1_id", "level_1_region"],
        "candidate": ["level_1_id", "level_1_source"],
    },
}

reorder_match_columns_one_level_with_attrs_full = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA"],
        "candidate_level_1_id": ["can_id_1"],
        "candidate_level_1_source": ["SRC-A"],
        "reference_level_1_name": ["TSHUAPA"],
        "reference_level_1_id": ["ref_id_1"],
        "reference_level_1_region": ["EQUATEUR PROVINCE"],
        "score_level_1": [100.0],
        "repeated_matches_level_1": [False],
    }
)

reorder_match_columns_one_level_with_attrs_simple = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA"],
        "candidate_level_1_id": ["can_id_1"],
        "candidate_level_1_source": ["SRC-A"],
        "reference_level_1_name": ["TSHUAPA"],
        "reference_level_1_id": ["ref_id_1"],
        "reference_level_1_region": ["EQUATEUR PROVINCE"],
    }
)

# Single level with an extra column not part of the level ordering.
reorder_match_columns_with_extra_col_input = pl.DataFrame(
    {
        "extra_col": ["extra_value"],
        "repeated_matches_level_1": [False],
        "score_level_1": [100.0],
        "reference_level_1_name": ["TSHUAPA"],
        "candidate_level_1_name": ["TSHUAPA"],
    }
)

reorder_match_columns_with_extra_col_full = pl.DataFrame(
    {
        "extra_col": ["extra_value"],
        "candidate_level_1_name": ["TSHUAPA"],
        "reference_level_1_name": ["TSHUAPA"],
        "score_level_1": [100.0],
        "repeated_matches_level_1": [False],
    }
)

reorder_match_columns_with_extra_col_simple = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA"],
        "reference_level_1_name": ["TSHUAPA"],
    }
)

# Two levels, no attributes.
reorder_match_columns_two_levels_input = pl.DataFrame(
    {
        "repeated_matches_level_2": [False],
        "score_level_2": [100.0],
        "repeated_matches_level_1": [False],
        "score_level_1": [100.0],
        "reference_level_2_name": ["BOENDE"],
        "candidate_level_2_name": ["BOENDE"],
        "reference_level_1_name": ["TSHUAPA"],
        "candidate_level_1_name": ["TSHUAPA"],
    }
)

reorder_match_columns_two_levels_attributes = {
    "level_1": {"reference": [], "candidate": []},
    "level_2": {"reference": [], "candidate": []},
}

reorder_match_columns_two_levels_full = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA"],
        "reference_level_1_name": ["TSHUAPA"],
        "score_level_1": [100.0],
        "repeated_matches_level_1": [False],
        "candidate_level_2_name": ["BOENDE"],
        "reference_level_2_name": ["BOENDE"],
        "score_level_2": [100.0],
        "repeated_matches_level_2": [False],
    }
)

reorder_match_columns_two_levels_simple = pl.DataFrame(
    {
        "candidate_level_1_name": ["TSHUAPA"],
        "reference_level_1_name": ["TSHUAPA"],
        "candidate_level_2_name": ["BOENDE"],
        "reference_level_2_name": ["BOENDE"],
    }
)


# ================================
# run_matching: level_1 matches, level_2 does not
# ================================

# Reference has a single path: PROVINCE_A → DISTRICT_X
# Candidate has a single path: PROVINCE_A → ZONE_OMEGA
# FuzzyMatcher(threshold=80): "PROVINCE_A"↔"PROVINCE_A" scores 100 (match);
# "DISTRICT_X"↔"ZONE_OMEGA" scores far below 80 (no match).

run_matching_l1_match_l2_nomatch_reference = pl.DataFrame(
    {
        "level_1_name": ["PROVINCE_A"],
        "level_2_name": ["DISTRICT_X"],
    }
)

run_matching_l1_match_l2_nomatch_candidate = pl.DataFrame(
    {
        "level_1_name": ["PROVINCE_A"],
        "level_2_name": ["ZONE_OMEGA"],
    }
)

run_matching_l1_match_l2_nomatch_expected_ref_unmatched = pl.DataFrame(
    {
        "level_1_name": ["PROVINCE_A"],
        "level_2_name": ["DISTRICT_X"],
        "unmatched_level": ["level_2"],
    }
)

run_matching_l1_match_l2_nomatch_expected_can_unmatched = pl.DataFrame(
    {
        "level_1_name": ["PROVINCE_A"],
        "level_2_name": ["ZONE_OMEGA"],
        "unmatched_level": ["level_2"],
    }
)


# ================================
# Matcher candidates
# ================================

candidates_dict = {
    "TSHUAPA": ["ym2K6YcSNl9"],
    "HAUT LOMAMI": ["fEKDiQIuqeE"],
    "KWILU": ["BmKjwqc6BEw"],
    "HAUT KATANGA": ["F9w3VW1cQmb"],
    "EQUATEUR": ["XjeRGfqHMrl"],
    "MANIEMA": ["uyuwe6bqphf"],
}

candidates_multiple_attributes = {
    "TSHUAPA": ["id1", "id2", "id3"],
}

candidates_best_match = {
    "TSHUAPA": ["exact_id"],
    "TSHUAPAZ": ["near_id"],
}
