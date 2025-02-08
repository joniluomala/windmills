
select * from  read_files(
  '/Volumes/the_catalog_dev/the_schema/jl666external', 
  format => 'csv',
  header => true,
  mode => 'FAILFAST')