create table stockbit_test.movies_collection_lists as
with mov_coll as(
 select 
  bc.id as collection_id,
  bc.name as collection_name,
  rm1.id as movie_id,
  case
    when bc.poster_path is null then bc.poster_path
    when bc.poster_path is not null then CONCAT('https://image.tmdb.org/t/p/original', bc.poster_path) 
  end as poster_image_url,
  case
    when bc.backdrop_path is null then bc.backdrop_path
    when bc.backdrop_path is not null then CONCAT('https://image.tmdb.org/t/p/original', bc.backdrop_path)
  end as backdrop_image_url,
 from 
    stockbit_test.raw_movies rm1, 
    unnest(belongs_to_collection) bc
 group by 1,2,3,4,5
 )
 , movie_name as (
 select rm2.id, rm2.title
 from stockbit_test.raw_movies rm2
 group by 1,2
 )
 select 
  mcl.collection_id, mcl.collection_name, 
  array_agg(mcl.movie_id) as movie_id, 
  array_agg(mvn.title) as movie_title, 
  mcl.poster_image_url, mcl.backdrop_image_url
 from mov_coll mcl
 left join movie_name mvn on mcl.movie_id = mvn.id
 group by 1,2,5,6