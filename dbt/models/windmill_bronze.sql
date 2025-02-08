
select * from  read_files(
  '<path to files>', 
  format => 'csv',
  header => true,
  mode => 'FAILFAST')