import json

test_cases = [
    # --- 1. CATALOG KPIs (15 cases) ---
    {"id": 1, "prompt": "total how many movies are there", "expected_metric": "total_movies", "expected_value": 6131, "unit": "titles"},
    {"id": 2, "prompt": "how many movies are there?", "expected_metric": "total_movies", "expected_value": 6131, "unit": "titles"},
    {"id": 3, "prompt": "what is the movie count", "expected_metric": "total_movies", "expected_value": 6131, "unit": "titles"},
    {"id": 4, "prompt": "how many films are in Netflix?", "expected_metric": "total_movies", "expected_value": 6131, "unit": "titles"},
    {"id": 5, "prompt": "number of movies", "expected_metric": "total_movies", "expected_value": 6131, "unit": "titles"},
    {"id": 6, "prompt": "how many TV shows are there", "expected_metric": "total_tv_shows", "expected_value": 2676, "unit": "titles"},
    {"id": 7, "prompt": "total tv show count", "expected_metric": "total_tv_shows", "expected_value": 2676, "unit": "titles"},
    {"id": 8, "prompt": "how many series are on netflix", "expected_metric": "total_tv_shows", "expected_value": 2676, "unit": "titles"},
    {"id": 9, "prompt": "how many total titles are there", "expected_metric": "total_titles", "expected_value": 8807, "unit": "titles"},
    {"id": 10, "prompt": "what is the total catalog size?", "expected_metric": "total_titles", "expected_value": 8807, "unit": "titles"},
    {"id": 11, "prompt": "total content count", "expected_metric": "total_titles", "expected_value": 8807, "unit": "titles"},
    {"id": 12, "prompt": "what is the average content age", "expected_metric": "average_content_age", "expected_value": 11.8, "unit": "years"},
    {"id": 13, "prompt": "how old is the average netflix title?", "expected_metric": "average_content_age", "expected_value": 11.8, "unit": "years"},
    {"id": 14, "prompt": "average content age of catalog", "expected_metric": "average_content_age", "expected_value": 11.8, "unit": "years"},
    {"id": 15, "prompt": "what is the movie ratio", "expected_metric": "movie_ratio", "expected_value": 69.62, "unit": "%"},

    # --- 2. MOVIE DURATION & METRICS (20 cases) ---
    {"id": 16, "prompt": "tell me the average movie duration", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 17, "prompt": "what is the average movie duration?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 18, "prompt": "how long is a typical movie?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 19, "prompt": "what is the average length of movies?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 20, "prompt": "how many minutes is the average movie?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 21, "prompt": "average movie length", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 22, "prompt": "movie duration average", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 23, "prompt": "show me average movie duration", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 24, "prompt": "what is the longest movie", "expected_metric": "longest_movie", "expected_value": 312, "expected_title": "Black Mirror: Bandersnatch", "unit": "minutes"},
    {"id": 25, "prompt": "which movie is the longest?", "expected_metric": "longest_movie", "expected_value": 312, "expected_title": "Black Mirror: Bandersnatch", "unit": "minutes"},
    {"id": 26, "prompt": "longest movie on netflix", "expected_metric": "longest_movie", "expected_value": 312, "expected_title": "Black Mirror: Bandersnatch", "unit": "minutes"},
    {"id": 27, "prompt": "what is the shortest movie", "expected_metric": "shortest_movie", "expected_value": 3, "expected_title": "Silent", "unit": "minutes"},
    {"id": 28, "prompt": "shortest movie on netflix", "expected_metric": "shortest_movie", "expected_value": 3, "expected_title": "Silent", "unit": "minutes"},
    {"id": 29, "prompt": "which movie has the shortest runtime?", "expected_metric": "shortest_movie", "expected_value": 3, "expected_title": "Silent", "unit": "minutes"},
    {"id": 30, "prompt": "minimum movie duration", "expected_metric": "min_movie_duration", "expected_value": 0, "unit": "minutes"},
    {"id": 31, "prompt": "maximum movie duration", "expected_metric": "max_movie_duration", "expected_value": 312, "unit": "minutes"},
    {"id": 32, "prompt": "average duration of movies", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 33, "prompt": "movie runtime average", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 34, "prompt": "average length of netflix movies", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 35, "prompt": "movie duration summary", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},

    # --- 3. TV SHOW SEASONS & METRICS (15 cases) ---
    {"id": 36, "prompt": "average tv show seasons", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 37, "prompt": "what is the average number of seasons for tv shows?", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 38, "prompt": "how many seasons does a show usually have?", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 39, "prompt": "average seasons per tv show", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 40, "prompt": "average tv duration in seasons", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 41, "prompt": "minimum seasons", "expected_metric": "min_seasons", "expected_value": 1, "unit": "seasons"},
    {"id": 42, "prompt": "maximum seasons", "expected_metric": "max_seasons", "expected_value": 17, "unit": "seasons"},
    {"id": 43, "prompt": "tv show season count", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 44, "prompt": "number of seasons for series", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 45, "prompt": "average number of seasons", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 46, "prompt": "tv series average duration", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 47, "prompt": "how long are tv series on average?", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 48, "prompt": "average seasons for netflix shows", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 49, "prompt": "tv show season average", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 50, "prompt": "average length of tv shows", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},

    # --- 4. COUNTRY RANKINGS & SHARES (15 cases) ---
    {"id": 51, "prompt": "which country makes most content?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 52, "prompt": "What are the top 5 countries by Netflix content?", "expected_metric": "top_5_countries", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 53, "prompt": "Show top 10 countries by content.", "expected_metric": "top_countries", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 54, "prompt": "Why is the US dominant?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 55, "prompt": "country with most content on netflix", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 56, "prompt": "which nation has the highest titles?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 57, "prompt": "top content producing countries", "expected_metric": "top_5_countries", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 58, "prompt": "country ranking by title count", "expected_metric": "top_5_countries", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 59, "prompt": "how many titles does US have?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 60, "prompt": "united states content count", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 61, "prompt": "top countries distribution", "expected_metric": "top_5_countries", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 62, "prompt": "what is india content count?", "expected_metric": "country_india", "expected_value": 1046, "expected_name": "India", "unit": "titles"},
    {"id": 63, "prompt": "how many titles from UK?", "expected_metric": "country_uk", "expected_value": 806, "expected_name": "United Kingdom", "unit": "titles"},
    {"id": 64, "prompt": "top 3 countries by content", "expected_metric": "top_5_countries", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 65, "prompt": "country content breakdown", "expected_metric": "top_5_countries", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},

    # --- 5. GENRES & CATEGORIES (10 cases) ---
    {"id": 66, "prompt": "what is the top genre on netflix?", "expected_metric": "top_genre", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 67, "prompt": "top genres on netflix", "expected_metric": "top_genres", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 68, "prompt": "genre distribution", "expected_metric": "top_genres", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 69, "prompt": "most popular genre", "expected_metric": "top_genre", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 70, "prompt": "top 5 movie genres", "expected_metric": "top_genres", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 71, "prompt": "which category has the most titles?", "expected_metric": "top_genre", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 72, "prompt": "genre breakdown of catalog", "expected_metric": "top_genres", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 73, "prompt": "leading genre category", "expected_metric": "top_genre", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 74, "prompt": "what are the main genres?", "expected_metric": "top_genres", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},
    {"id": 75, "prompt": "most common genre", "expected_metric": "top_genre", "expected_value": 2752, "expected_name": "International Movies", "unit": "titles"},

    # --- 6. TIME & GROWTH METRICS (10 cases) ---
    {"id": 76, "prompt": "Analyze Netflix content growth.", "expected_metric": "growth_trend", "expected_value": 2018, "expected_count": 1147, "unit": "titles"},
    {"id": 77, "prompt": "which year had the most releases?", "expected_metric": "peak_release_year", "expected_value": 2018, "expected_count": 1147, "unit": "titles"},
    {"id": 78, "prompt": "peak release year", "expected_metric": "peak_release_year", "expected_value": 2018, "expected_count": 1147, "unit": "titles"},
    {"id": 79, "prompt": "releases by year", "expected_metric": "growth_trend", "expected_value": 2018, "expected_count": 1147, "unit": "titles"},
    {"id": 80, "prompt": "content growth over time", "expected_metric": "growth_trend", "expected_value": 2018, "expected_count": 1147, "unit": "titles"},
    {"id": 81, "prompt": "Predict content growth.", "expected_metric": "ml_forecast", "expected_r2": 0.6981, "unit": "titles"},
    {"id": 82, "prompt": "forecast future catalog releases", "expected_metric": "ml_forecast", "expected_r2": 0.6981, "unit": "titles"},
    {"id": 83, "prompt": "annual release velocity", "expected_metric": "growth_trend", "expected_value": 2018, "expected_count": 1147, "unit": "titles"},
    {"id": 84, "prompt": "content additions timeline", "expected_metric": "growth_trend", "expected_value": 2018, "expected_count": 1147, "unit": "titles"},
    {"id": 85, "prompt": "historical release trend", "expected_metric": "growth_trend", "expected_value": 2018, "expected_count": 1147, "unit": "titles"},

    # --- 7. DIRECTORS, RATINGS & ML ANOMALIES (10 cases) ---
    {"id": 86, "prompt": "top director on netflix", "expected_metric": "top_director", "expected_value": 22, "expected_name": "Rajiv Chilaka", "unit": "titles"},
    {"id": 87, "prompt": "director with most titles", "expected_metric": "top_director", "expected_value": 22, "expected_name": "Rajiv Chilaka", "unit": "titles"},
    {"id": 88, "prompt": "most frequent director", "expected_metric": "top_director", "expected_value": 22, "expected_name": "Rajiv Chilaka", "unit": "titles"},
    {"id": 89, "prompt": "content rating distribution", "expected_metric": "top_rating", "expected_value": 3207, "expected_name": "TV-MA", "unit": "titles"},
    {"id": 90, "prompt": "most common rating", "expected_metric": "top_rating", "expected_value": 3207, "expected_name": "TV-MA", "unit": "titles"},
    {"id": 91, "prompt": "Which countries have unusual content patterns?", "expected_metric": "ml_anomaly", "expected_agent": "ML Agent", "unit": "countries"},
    {"id": 92, "prompt": "outlier countries in catalog", "expected_metric": "ml_anomaly", "expected_agent": "ML Agent", "unit": "countries"},
    {"id": 93, "prompt": "anomalous content distributions", "expected_metric": "ml_anomaly", "expected_agent": "ML Agent", "unit": "countries"},
    {"id": 94, "prompt": "cluster countries by content", "expected_metric": "ml_anomaly", "expected_agent": "ML Agent", "unit": "countries"},
    {"id": 95, "prompt": "segmentation of netflix markets", "expected_metric": "ml_anomaly", "expected_agent": "ML Agent", "unit": "countries"},

    # --- 8. DOMAIN SUPPORT QUESTIONS (5 cases) ---
    {"id": 96, "prompt": "What does Movie Ratio mean?", "expected_metric": "support_query", "expected_agent": "Support Agent", "expected_value": 69.62},
    {"id": 97, "prompt": "What does Content Age mean?", "expected_metric": "support_query", "expected_agent": "Support Agent", "expected_value": 11.8},
    {"id": 98, "prompt": "how do i use this app?", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 99, "prompt": "explain the gold schema", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 100, "prompt": "what data is in netflix dw?", "expected_metric": "support_query", "expected_agent": "Support Agent"},

    # --- 9. POWER BI CONTEXT FILTERED CASES (5 cases) ---
    {"id": 101, "prompt": "what are the top genres?", "context": {"country": "India"}, "expected_metric": "context_india_genres", "expected_value": 864, "expected_name": "International Movies"},
    {"id": 102, "prompt": "how many titles are there?", "context": {"country": "India"}, "expected_metric": "context_india_titles", "expected_value": 1046},
    {"id": 103, "prompt": "how many titles are there?", "context": {"year": 2020}, "expected_metric": "context_2020_titles", "expected_value": 953},
    {"id": 104, "prompt": "how many movies are there?", "context": {"country": "United Kingdom"}, "expected_metric": "context_uk_movies", "expected_value": 534},
    {"id": 105, "prompt": "top genres", "context": {"type": "TV Show"}, "expected_metric": "context_tv_genres", "expected_name": "International TV Shows"}
]

with open("tests/ai_accuracy_cases.json", "w", encoding="utf-8") as f:
    json.dump(test_cases, f, indent=2)

print(f"Successfully generated {len(test_cases)} natural-language test cases in tests/ai_accuracy_cases.json!")
