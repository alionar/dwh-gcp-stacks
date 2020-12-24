create table {{ params.bq_dataset }}.movies_spoken_language as
select sl.name, sl.iso_639_1,
from {{ params.bq_dataset }}.raw_movies, unnest(spoken_languages) as sl
where exists (select 1 from unnest(spoken_languages) where iso_639_1 is not null)
group by sl.iso_639_1, sl.name
order by sl.iso_639_1