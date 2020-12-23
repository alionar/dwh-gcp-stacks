create table if not exists stockbit_test.movies_media as
select
  id as movie_id,
  imdb_id, 
  title as movie_title,
  original_title as orginal_movie_title,
  case
    when rm.poster_path is null then rm.poster_path
    when rm.poster_path is not null then CONCAT('https://image.tmdb.org/t/p/original', rm.poster_path) 
  end as poster_image_url,
  case
    when rm.backdrop_path is null then rm.backdrop_path
    when rm.backdrop_path is not null then CONCAT('https://image.tmdb.org/t/p/original', rm.backdrop_path)
  end as backdrop_image_url,
  video
from
  stockbit_test.raw_movies rm
order by 1