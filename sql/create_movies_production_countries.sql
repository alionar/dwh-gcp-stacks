select pc.name as country_name, pc.iso_3166_1 as country_code,
from stockbit_test1.raw_movies,unnest(production_countries) as pc
where exists (select 1 from unnest(production_countries) where iso_3166_1 is not null)
group by pc.iso_3166_1, pc.name
order by pc.iso_3166_1;