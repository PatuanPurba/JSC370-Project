import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime
import time
from scripts.api_call import getMovieIDList, buildDataset
from sklearn.preprocessing import MultiLabelBinarizer
from pathlib import Path


important_crew = [
  "Director",
  "Screenplay", "Writer", "Story",
  "Producer",
  "Director of Photography",
  "Original Music Composer"
]

movie_path = Path("data/movies.csv")
FORCED = True
if not movie_path.exists() or FORCED:
    short_movie_ids, full_movie_ids = getMovieIDList(
        baseline_url="https://api.themoviedb.org/3/discover/movie",
        short_page_per_month=20, full_page_per_month=40,
        year_start=2000, year_end=2025
    )
    
    buildDataset(
        list_movie_id=full_movie_ids, type_movie="full_movie",
        add_person= False,
        cast_per_film=10,
        important_crew=important_crew
    )

    buildDataset(
        list_movie_id=short_movie_ids, type_movie="short_movie",
        add_person= True,
        cast_per_film=10,
        important_crew=important_crew,
        movies_person_limit=3000, 
    )

    

