create table stockbit_test.production_companies_portofolio as
select 
  pc.id as ph_id,
  pc.name as ph_name,
  pc.origin_country as ph_country,
  array_agg(struct(g.name as genres)) as ph_movie,
  count(rm.id) as ph_total_movie,
  avg(rm.popularity) as average_popularity_movie_score,
  sum(revenue) as ph_total_revenue,
  sum(budget) as ph_total_budget
from 
  stockbit_test.raw_movies rm, 
  unnest(production_companies) pc, 
  unnest(genres) g
group by 1,2,3
order by 6 desc