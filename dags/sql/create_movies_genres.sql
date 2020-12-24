create table {{ params.bq_dataset }}.movies_genres as 
select g.id, g.name
from {{ params.bq_dataset }}.raw_movies, unnest(genres) as g
where exists (select 1 from unnest(genres) where id is not null)
group by g.id, g.name
order by g.id