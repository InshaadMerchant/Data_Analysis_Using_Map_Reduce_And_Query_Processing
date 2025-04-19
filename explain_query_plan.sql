-- Generate execution plan for the Comedy-Romance movies query
EXPLAIN PLAN 
SET statement_id = 'ex_plan1' FOR
WITH ComedyRomanceMovies AS (
    SELECT tb.tconst, 
           tb.primarytitle,
           tr.averagerating,
           tr.numvotes,
           nb.primaryname as lead_actor
    FROM imdb00.title_basics tb
    JOIN imdb00.title_ratings tr ON tb.tconst = tr.tconst
    JOIN imdb00.title_principals tp ON tb.tconst = tp.tconst
    JOIN imdb00.name_basics nb ON tp.nconst = nb.nconst
    WHERE tb.titletype = 'movie'
    AND tb.startyear BETWEEN '2011' AND '2020'
    -- Check for both Comedy and Romance in genres
    AND tb.genres LIKE '%Comedy%'
    AND tb.genres LIKE '%Romance%'
    -- Filter for lead actor/actress
    AND tp.ordering = '1'
    -- Filter for movies with at least 150000 votes
    AND tr.numvotes >= 150000
    AND tr.averagerating >= 7.5
)
SELECT 
    primarytitle AS "Movie Title",
    averagerating AS "Rating",
    numvotes AS "Number of Votes",
    lead_actor AS "Lead Actor/Actress"
FROM ComedyRomanceMovies
ORDER BY averagerating DESC
FETCH FIRST 5 ROWS ONLY;

-- Display the execution plan
SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, 'ex_plan1', 'BASIC'));