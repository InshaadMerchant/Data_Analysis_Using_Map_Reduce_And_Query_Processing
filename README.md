# IMDb Data Analysis Project

This project analyzes IMDb movie data using both Hadoop MapReduce and SQL queries. The analysis focuses on movie genres, ratings, and time periods to extract meaningful insights from the IMDb dataset.

## Project Structure

```
.
├── hadoop/
│   ├── GenreAnalysis.java       # MapReduce implementation for genre analysis
│   └── imdbproj3.jar           # Compiled JAR file
├── sql/
│   ├── top_comedy_romance_movies.sql  # SQL query for top movies
│   └── explain_query_plan.sql         # Query execution plan analysis
├── visualizations/
│   ├── movie_counts_bar_chart.png     # Bar chart of movie counts
│   └── visualize.py                    # Python script for visualization
└── README.md
```

## Part 1: Hadoop MapReduce Analysis

### Overview
The MapReduce program analyzes the distribution of movies across different genre combinations and time periods (1991-2000, 2001-2010, 2011-2020).

### Genre Combinations Analyzed
- Action & Thriller
- Adventure & Drama
- Comedy & Romance

### Requirements
- Hadoop 3.3.2
- JDK 11
- Python 3.11.5 (for visualizations)
- matplotlib (Python library)

### Running the MapReduce Job

1. Compile the Java code:
```bash
javac -cp $(hadoop classpath) GenreAnalysis.java
```

2. Create the JAR file:
```bash
jar cvf imdbproj3.jar *.class
```

3. Run the MapReduce job:
```bash
hadoop jar imdbproj3.jar GenreAnalysis /imdbinput /imdboutput
```

### Results
The analysis shows the following distribution:
```
[1991-2000],Action,Thriller     55
[1991-2000],Adventure,Drama     56
[1991-2000],Comedy,Romance      215
[2001-2010],Action,Thriller     76
[2001-2010],Adventure,Drama     141
[2001-2010],Comedy,Romance      400
[2011-2020],Action,Thriller     208
[2011-2020],Adventure,Drama     343
[2011-2020],Comedy,Romance      590
```

## Part 2: SQL Analysis

### Overview
SQL queries analyze the top 5 movies in specific genre combinations, focusing on:
- Movies from 2011-2020
- Movies with at least 150,000 votes
- Rating analysis
- Lead actor/actress information

### Database Schema
Uses the IMDb database on omega with tables:
- imdb00.TITLE_BASICS
- imdb00.TITLE_RATINGS
- imdb00.TITLE_PRINCIPALS
- imdb00.NAME_BASICS

### Running SQL Queries

1. Connect to the omega database:
```sql
sqlplus username/password@omega
```

2. Set up formatting:
```sql
SET LINESIZE 200
SET PAGESIZE 50
COLUMN "Movie Title" FORMAT A50
COLUMN "Lead Actor/Actress" FORMAT A30
```

3. Run the analysis query:
```sql
@top_comedy_romance_movies.sql
```

## Visualizations

### Bar Chart Analysis
The project includes a bar chart visualization showing:
- Movie counts by genre combination
- Distribution across three decades
- Comparative analysis of genre popularity

To generate visualizations:
```bash
python3 visualize.py
```

## Key Findings

1. Genre Trends:
   - Comedy-Romance is consistently the most popular combination
   - All genre combinations show growth from 1991 to 2020
   - Significant increase in production during 2011-2020

2. Rating Analysis:
   - Detailed analysis of top-rated movies
   - Focus on highly-voted movies (150,000+ votes)
   - Lead actor/actress correlation with ratings

## Contributors
- Inshaad Merchant
- Araohat Kokate
- Aindrila Bhattacharya

## License
This project is part of the academic coursework for [Course Number] at [University Name].
