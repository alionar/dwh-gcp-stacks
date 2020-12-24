create table stockbit_test.movies_popular_released_per_year as
with pop_movie1 as (
  select 
    rw2.id as movie_id, 
    EXTRACT(ISOYEAR FROM rw2.release_date) as year, 
    rw2.popularity as popularity
  from stockbit_test.raw_movies rw2
  where rw2.status = 'Released'
  group by 1,2,3
)
, pop_movie2 as (
  select 
    p1.year, 
    ARRAY_AGG(STRUCT(p1.movie_id, p1.popularity) ORDER BY p1.popularity DESC LIMIT 1) AS popular
  from pop_movie1 p1
  where year is not null
  group by 1
)
, pop_movie3 as(
  select year, pp.movie_id, pp.popularity
  from pop_movie2 pm2, unnest(pm2.popular) pp
  order by 1 desc
)
, etc_att as (
  select 
    a.id as movie_id,
    array_agg(distinct pct.name respect nulls) as movie_country,
    array_agg(distinct rg.name respect nulls) as genre_name,
    array_agg(distinct ph.name respect nulls) as ph_name
  from 
    stockbit_test.raw_movies a,
    unnest(a.production_countries) pct,
    unnest(a.genres) rg,
    unnest(a.production_companies) ph
  group by 1
)
, main_att as (
  select 
    b.id as movie_id,
    title,
    original_language,
    budget,
    revenue,
    runtime,
    overview,
    vote_average,
    vote_count,
    release_date
  from 
    stockbit_test.raw_movies b
  group by 1,2,3,4,5,6,7,8,9,10
)
, main_table as (
  select 
    x.movie_id,
    title,
    original_language,
    budget,
    revenue,
    runtime,
    overview,
    vote_average,
    vote_count,
    release_date,
    case
      when y.movie_country is null then null
      when y.movie_country is not null then y.movie_country
    end as movie_country,
    case
      when y.genre_name is null then null
      when y.genre_name is not null then y.genre_name
    end as genre_name,
    case
      when y.ph_name is null then null
      when y.ph_name is not null then y.ph_name
    end as ph_name
  from main_att x
  left join etc_att y on x.movie_id = y.movie_id
)
select
  pm3.year,
  pm3.movie_id,
  t.title as movie_title,
  pm3.popularity as movie_popularity,
  t.release_date,
  t.movie_country,
  t.genre_name,
  t.ph_name,
  t.budget,
  t.revenue,
  t.runtime,
  t.overview,
  t.vote_average,
  t.vote_count
from pop_movie3 pm3
left join main_table t on pm3.movie_id = t.movie_id
order by 1 desc