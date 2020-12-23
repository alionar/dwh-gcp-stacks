create table stockbit_test.movies_genres as 
select g.id, g.name
from stockbit_test.raw_movies, unnest(genres) as g
where exists (select 1 from unnest(genres) where id is not null)
group by g.id, g.name
order by g.id