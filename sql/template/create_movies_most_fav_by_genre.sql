create table {{ params.bq_dataset }}.movies_most_fav_by_genre as
with vote_genre1 as (
  select
    g.name as genres,
    array_agg(struct(rm1.id, rm1.vote_average) order by rm1.vote_average desc limit 1) as vote
  from
    {{ params.bq_dataset }}.raw_movies rm1, unnest(genres) g
  group by 1
)
, vote_genre2 as (
  select 
    pg1.genres,
    p.id as movie_id,
    p.vote_average as movie_vote_avg
  from
    vote_genre1 pg1,
    unnest(pg1.vote) p
)
, main_att as (
  select 
    rm2.id as movie_id,
    rm2.title as movie_title,
    EXTRACT(ISOYEAR FROM rm2.release_date) as year,
    rm2.vote_count
  from
    {{ params.bq_dataset }}.raw_movies rm2
  group by 1,2,3,4
)
, etc_att as (
  select 
    a.id as movie_id,
    array_agg(distinct pct.name respect nulls) as movie_country,
    array_agg(distinct ph.name respect nulls) as ph_name
  from 
    {{ params.bq_dataset }}.raw_movies a,
    unnest(a.production_countries) pct,
    unnest(a.production_companies) ph
  group by 1
)
, main_table as (
  select
    x.movie_id,
    x.movie_title,
    x.vote_count,
    x.year,
    case
      when y.movie_country is null then null
      when y.movie_country is not null then y.movie_country
    end as movie_country,
    case
      when y.ph_name is null then null
      when y.ph_name is not null then y.ph_name
    end as ph_name
  from main_att x
  left join etc_att y on x.movie_id = y.movie_id
)
select
  pg2.genres, 
  t.movie_title, 
  pg2.movie_id, 
  t.year, 
  t.movie_country[OFFSET(0)] as movie_country,
  pg2.movie_vote_avg,
  t.vote_count
from 
  vote_genre2 pg2
left join main_table t on pg2.movie_id = t.movie_id
order by 3 desc