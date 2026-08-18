import json

# Load existing 105 cases
with open("tests/ai_accuracy_cases.json", "r", encoding="utf-8") as f:
    cases = json.load(f)

print(f"Loaded existing {len(cases)} test cases.")

# 50 Additional Paraphrased Test Cases
additional_cases = [
    # Paraphrases for Movie Duration & Length (15 new)
    {"id": 106, "prompt": "how long are movies", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 107, "prompt": "typical movie runtime", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 108, "prompt": "movie runtime average", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 109, "prompt": "average duration of a Netflix movie", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 110, "prompt": "how many minutes does a movie usually run", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 111, "prompt": "longest movie on Netflix dataset", "expected_metric": "longest_movie", "expected_value": 312, "expected_title": "Black Mirror: Bandersnatch", "unit": "minutes"},
    {"id": 112, "prompt": "what film has the longest duration?", "expected_metric": "longest_movie", "expected_value": 312, "expected_title": "Black Mirror: Bandersnatch", "unit": "minutes"},
    {"id": 113, "prompt": "shortest movie feature", "expected_metric": "shortest_movie", "expected_value": 3, "expected_title": "Silent", "unit": "minutes"},
    {"id": 114, "prompt": "movie duration in minutes", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 115, "prompt": "average length of films", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 116, "prompt": "typical film runtime on netflix", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 117, "prompt": "how long is a film on average?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 118, "prompt": "movie length breakdown", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 119, "prompt": "average feature film duration", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 120, "prompt": "mean movie duration", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},

    # Paraphrases for TV Seasons & Series Length (15 new)
    {"id": 121, "prompt": "typical number of seasons", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 122, "prompt": "average TV seasons", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 123, "prompt": "TV series usually have how many seasons", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 124, "prompt": "maximum number of seasons", "expected_metric": "max_seasons", "expected_value": 17, "unit": "seasons"},
    {"id": 125, "prompt": "which TV show has the most seasons", "expected_metric": "max_seasons", "expected_value": 17, "unit": "seasons"},
    {"id": 126, "prompt": "average number of seasons per series", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 127, "prompt": "typical series length in seasons", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 128, "prompt": "how many seasons per tv show on average?", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 129, "prompt": "mean tv seasons", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 130, "prompt": "tv show season distribution average", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 131, "prompt": "how many seasons is typical for a show?", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 132, "prompt": "tv series season average", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 133, "prompt": "lowest number of seasons", "expected_metric": "min_seasons", "expected_value": 1, "unit": "seasons"},
    {"id": 134, "prompt": "highest season count for a show", "expected_metric": "max_seasons", "expected_value": 17, "unit": "seasons"},
    {"id": 135, "prompt": "average seasons of shows", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},

    # Paraphrases for Support & Domain Questions (10 new)
    {"id": 136, "prompt": "define content age", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 137, "prompt": "what is the meaning of content age", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 138, "prompt": "explain the Gold layer", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 139, "prompt": "what is the Gold schema", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 140, "prompt": "explain Movie Ratio", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 141, "prompt": "what does content age measure?", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 142, "prompt": "explain medallion architecture", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 143, "prompt": "how is movie ratio calculated?", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 144, "prompt": "explain country assignment share", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 145, "prompt": "what is scd type 2?", "expected_metric": "support_query", "expected_agent": "Support Agent"},

    # Additional Paraphrases for Catalog & Country Metrics (10 new)
    {"id": 146, "prompt": "how many movies exist in data warehouse?", "expected_metric": "total_movies", "expected_value": 6131, "unit": "titles"},
    {"id": 147, "prompt": "count of all tv shows", "expected_metric": "total_tv_shows", "expected_value": 2676, "unit": "titles"},
    {"id": 148, "prompt": "total distinct titles", "expected_metric": "total_titles", "expected_value": 8807, "unit": "titles"},
    {"id": 149, "prompt": "what is netflix total title count?", "expected_metric": "total_titles", "expected_value": 8807, "unit": "titles"},
    {"id": 150, "prompt": "average age of netflix catalog", "expected_metric": "average_content_age", "expected_value": 11.8, "unit": "years"},
    {"id": 151, "prompt": "which country has second most content?", "expected_metric": "top_5_countries", "expected_value": 1046, "expected_name": "India", "unit": "titles"},
    {"id": 152, "prompt": "top country for content production", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 153, "prompt": "what is US catalog share?", "expected_metric": "top_country", "expected_value": 41.9, "unit": "%"},
    {"id": 154, "prompt": "most popular genre category", "expected_metric": "top_genre", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 155, "prompt": "highest release year for netflix", "expected_metric": "peak_release_year", "expected_value": 2018, "expected_count": 1147, "unit": "titles"}
]

all_cases = cases + additional_cases

with open("tests/ai_accuracy_cases.json", "w", encoding="utf-8") as f:
    json.dump(all_cases, f, indent=2)

print(f"Successfully expanded test bank from {len(cases)} to {len(all_cases)} natural-language test cases!")
