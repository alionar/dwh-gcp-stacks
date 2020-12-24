create table stockbit_test1.movies_spoken_language as
select sl.name, sl.iso_639_1,
from stockbit_test1.raw_movies, unnest(spoken_languages) as sl
where exists (select 1 from unnest(spoken_languages) where iso_639_1 is not null)
group by sl.iso_639_1, sl.name
order by sl.iso_639_1