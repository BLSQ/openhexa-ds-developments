# Pyramid Matching

## 1. Introduction

The function `run_matching` is designed to perform a hierarchical matching between two pyramids: a reference pyramid and a candidate pyramid. The function will compare the levels of the two pyramids against each other, starting from the highest level (the one with the lowest level number) and going down to the lower levels. The function will return four outputs: the matched data between the reference and candidate pyramids, a simplified version of the matched data, the levels of the reference pyramid that were not matched with any level of the candidate pyramid, and the levels of the candidate pyramid that were not matched with any level of the reference pyramid. The function is designed to be flexible and can be used with different types of matchers, which are responsible for calculating the similarity score between the levels of the pyramids and determining whether a match is considered a match or not based on a minimum similarity score threshold. The function also allows the user to specify which levels of the pyramids to match against each other and which columns to use for the matching process. 


## 2. Usage
This section details how to use the pyramid matching functionality provided by the `PyramidMatcher` class. It covers the expected format of the input pyramids, how the matching process works, and the outputs that can be obtained.

### 2.1. Example of how to use the `PyramidMatcher` class
In order to match a candidate pyramid against a reference pyramid, you can follow these steps:

1. Create an instance of a matcher. This is the matcher that will be used to compare the different levels of the pyramids. If no matcher is provided, a default one will be used. The file `pyramid_matcher/matchers.py` contains several matchers that you can use depending on your needs.

2. Create an instance of the `PyramidMatcher` class, passing the matcher as an argument. This will initialize the pyramid matcher and prepare it for matching.

3. Call the `run_matching` method, providing the reference pyramid and the candidate pyramid as arguments. This method will perform the matching process and return the results.

```python
import polars as pl
from pyramid_matcher.matchers import FuzzyMatcher
from pyramid_matcher.pyramid_matcher import PyramidMatcher

reference_pyramid = pl.DataFrame()
candidate_pyramid = pl.DataFrame()

matcher = FuzzyMatcher()
pyramid_matcher = PyramidMatcher(matcher)

matched_data, matched_data_simplified, reference_not_matched, candidate_not_matched = (
        pyramid_matcher.run_matching(
            reference_pyramid=reference_pyramid,
            candidate_pyramid=candidate_pyramid,
        )
    )
```
In this case, if the reference pyramid is:
| level_2_name | level_2_id | level_3_name | level_3_id | level_3_other_attribute |
|--------------|------------|--------------|------------|-------------------------|
| Province 1   | 100        | District 10  | 1000       | Attribute 1000          |
| Province 1   | 100        | District 11  | 1001       | Attribute 1001          |
| Province 2   | 200        | District 20  | 2000       | Attribute 2000          |
| Province 2   | 200        | District 21  | 2001       | Attribute 2001          |
| Province 3   | 300        | District 30  | 3000       | Attribute 3000          |
| Province 4   | 400        | District 40  | 4000       | Attribute 4000          |



and the candidate pyramid is:

| level_2_name | level_2_id | level_2_another_attribute | level_3_name | level_3_id |
|--------------|------------|---------------------------|--------------|------------|
| Province 1   | 1          | Another 1                 | District 10  | 10         |
| Province 2   | 2          | Another 2                 | District 20  | 20         |
| Province 2   | 2          | Another 2                 | District 21  | 21         |
| Province 2   | 2          | Another 2                 | District 22  | 22         |
| Province 3   | 3          | Another 3                 | District30   | 31         |
| Province 5   | 5          | Another 5                 | District 50  | 50         |


then the `matched_data` output will look like:

| candidate_level_2_name | reference_level_2_name | candidate_level_2_id | candidate_level_2_another_attribute | reference_level_2_id | score_level_2 | repeated_matches_level_2 | candidate_level_3_name | reference_level_3_name | candidate_level_3_id | reference_level_3_id | reference_level_3_other_attribute | score_level_3 | repeated_matches_level_3 |
|------------------------|------------------------|----------------------|--------------------------------------|----------------------|---------------|------------------|------------------------|------------------------|----------------------|----------------------|------------------------------------|---------------|------------------|
| Province 1             | Province 1             | 1                    | Another 1                            | 100                  | 100           | False            | District 10            | District 10            | 10                   | 1000                 | Attribute 1000                    | 100           | False            |
| Province 2             | Province 2             | 2                    | Another 2                            | 200                  | 100           | False            | District 20            | District 20            | 20                   | 2000                 | Attribute 2000                    | 100           | False            |
| Province 2             | Province 2             | 2                    | Another 2                            | 200                  | 100           | False            | District 21            | District 21            | 21                   | 2001                 | Attribute 2001                    | 100           | False            |
| Province 3             | Province 3             | 3                    | Another 3                            | 300                  | 100           | False            | District30             | District 30            | 31                   | 3000                 | Attribute 3000                    | 90            | False            |



where:
- `candidate_*` columns come from your candidate pyramid
- `reference_*` columns come from your reference pyramid
- `score_level_*` columns show the similarity score for each level (0-100)
- `repeated_level_*` columns indicate whether the match for that level is repeated (i.e., if the same reference level is matched to multiple candidate levels).


The output `matched_data_simplified` will be:
| candidate_level_2_name | reference_level_2_name | candidate_level_2_id | candidate_level_2_another_attribute | reference_level_2_id | candidate_level_3_name | reference_level_3_name | candidate_level_3_id | reference_level_3_id | reference_level_3_other_attribute |
|------------------------|-----------------------|----------------------|---------------------------|----------------------|------------------------|-----------------------|----------------------|----------------------|-------------------------------|
| Province 1             | Province 1            | 1                    | Another 1                | 100                 | District 10            | District 10           | 10                   | 1000                 | Attribute 1000                 |
| Province 2             | Province 2            | 2                    | Another 2                | 200                 | District 20            | District 20           | 20                   | 2000                 | Attribute 2000                 |
| Province 2             | Province 2            | 2                    | Another 2                | 200                 | District 21            | District 21           | 21                   | 2001                 | Attribute 2001                 |
| Province 3             | Province 3            | 3                    | Another 3                | 300                 | District30             | District 30           | 31                   | 3000                 | Attribute 3000                 |

The output `reference_not_matched` will be:

| level_2_name | level_2_id | level_3_name | level_3_id | level_3_other_attribute | unmatched_level |
|--------------|------------|--------------|------------|-------------------------|----------------| 
| Province 1   | 100        | District 11  | 1001       | Attribute 1001          | level_3           |
| Province 4   | 400        | District 40  | 4000       | Attribute 4000          | level_2   |


The output `candidate_not_matched` will be:
| level_2_name | level_2_id | level_3_name | level_3_id | level_3_other_attribute | unmatched_level |
|--------------|------------|--------------|------------|-------------------------|----------------|
| Province 2   | 2          | Another 2                 | District 22  | 22        | | level_3   |
| Province 5   | 500        | District 50  | 5000       | Attribute 5000          | level_2  |


### 2.2 Inputs

`run_matching` takes several inputs:
- `reference_pyramid`: A DataFrame representing the reference pyramid. This is the pyramid that we are going to match against.
- `candidate_pyramid`: A DataFrame representing the candidate pyramid. This is the pyramid that we want to match to the reference pyramid.
- `levels_to_match`: This is optional. This contains the levels of the pyramids that we want to match against each other. If no levels are provided, all levels that are present in both pyramids will be matched. In this case, both of the pyramids will be inspected, finding the common columns that start with "level_" and ending with the `matching_col_suffix` (see below). The levels that will be matched will be those that have a common column in both pyramids.
- `matching_col_suffix`: This is optional. This is the suffix of the columns that will be matched against each other. If no suffix is provided, the suffix will be _name. This means that, in order to do the matching, we will compare the columns that end with _name in both pyramids.

### 2.3 Outputs

The `run_matching` method returns four outputs:
- `matched_data`: This is a DataFrame that contains the matched data between the reference and candidate pyramids. It includes the matched levels, the attributes of the matched levels, the similarity score of the matches and whether the matches are repeated or not. (See section 2.4 for more details about how the matches are done, including information about what is considered and attribute and what is considered a repeated match).
- `matched_data_simplified`: This is a simplified version of the `matched_data` DataFrame. It contains only the matched levels and their attributes; without the score or the information about whether the matches are repeated or not.
- `reference_not_matched`: This is a DataFrame that contains the levels of the reference pyramid that were not matched with any level of the candidate pyramid. It includes the unmatched information as well as the level that was not matched.
- `candidate_not_matched`: This is a DataFrame that contains the levels of the candidate pyramid that were not matched with any level of the reference pyramid. It includes the unmatched information as well as the level that was not matched.

### 2.4 Expected format of the pyramids

The function `run_matching` does not include any cleaning: it is expected that the user will clean both the reference and the candidate pyramids before feeding them into the functions. The expected format of the pyramids is as follows:

- The columns that will be matched against each others are named following the pattern `level_{level_number}_{matching_col_suffix}`. For example, if the `matching_col_suffix` is `_name`, the columns to do the matching with should be named `level_1_name`, `level_2_name`, etc. If the `matching_col_suffix` is `_geometry`, the columns to do the matching with should be named `level_1_geometry`, `level_2_geometry`, etc. The levels that will be matched will be those that have a common column in both pyramids.

- The characteristics of the levels that are to be included in the output (which we have been calling attributes) should have the prefix `level_{level_number}_{attribute_name}`. For example, if we want the id's to be including in the output, and we are matching the names against each other, the columns with the id's should be named `level_1_id`, `level_2_id`, etc. If we want the geometries to be included in the output, the columns with the geometries should be named `level_1_geometry`, `level_2_geometry`, etc. All of the attributes will be included in the output, even if they are included only in some levels or only in the reference or candidate pyramids. So if only the reference pyramid has a column named `level_4_antenne`, this column will still appear in the output, linked to the reference pyramid column  `level_4_{matching_col_suffix}`

### 2.5 How the matches are done

- We match hierarchically: we start by matching the highest level of the pyramids (the one with the lowest level number), and then we go down to the lower levels. This means that, for example, if we have a pyramid with three levels (level_1, level_2 and level_3), we will first match the levels 1 against each other, then we will match the levels 2 against each other, and finally we will match the levels 3 against each other. This also means that, if a level is not matched, the levels below it will not be matched either.
- We add the attributes of the matched levels to the output. For example, if we match level_1_name against level_1_name, and we have the columns level_1_id and level_1_geometry in the reference pyramid, and the columns level_1_id and level_1_population in the candidate pyramid, the output will include the columns level_1_id and level_1_geometry for the reference pyramid, and the columns level_1_id and level_1_population for the candidate pyramid.
- We calculate the similarity score of the matches. The similarity score is a number between 0 and 100 that indicates how similar the matched levels are. The way the similarity score is calculated depends on the matcher that is used. The matchers have a minimum similarity score threshold, which is the minimum score that a match must have in order to be considered a match. If the similarity score of a match is below the threshold, the match will not be included in the output.
- We indicate whether the matches are repeated or not. A match is considered repeated if the same reference level is matched to multiple candidate levels. For example, if we have a reference level called "Province 1" that is matched to two candidate levels called "Province 1" and "Province 1 bis", the match between "Province 1" in the reference pyramid and "Province 1" in the candidate pyramid will be considered repeated, as well as the match between "Province 1" in the reference pyramid and "Province 1 bis" in the candidate pyramid. The information about whether a match is repeated or not is included in the output as a boolean column (True if the match is repeated, False if it is not).



## 3. Matching Algorithms

The `PyramidMatcher` class is designed to be flexible and can be used with different types of matchers, which are responsible for calculating the similarity score between the levels of the pyramids and determining whether a match is considered a match or not based on a minimum similarity score threshold. The file `pyramid_matcher/matchers.py` contains several matchers that you can use depending on your needs.

For now, only a `FuzzyMatcher` is implemented

### 3.1 FuzzyMatcher
The `FuzzyMatcher` is a matcher that calculates the similarity score between two levels based on the Levenshtein distance between the strings in the levels. The Levenshtein distance is a measure of the difference between two strings, defined as the minimum number of single-character edits (insertions, deletions or substitutions) required to change one string into the other. The similarity score is calculated as follows:

## 4. Installation

Install this library on its own:

```bash
pip install git+https://github.com/BLSQ/openhexa-ds-developments.git#subdirectory=pyramid_matching
```