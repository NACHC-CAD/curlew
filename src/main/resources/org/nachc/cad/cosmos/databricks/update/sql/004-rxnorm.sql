-- 
-- Files definitions are from here:
-- https://www.nlm.nih.gov/research/umls/rxnorm/docs/techdoc.html#conso 
-- 

create schema if not exists rxnorm;

drop table if exists rxnorm.rxnconso;

create table rxnorm.rxnconso (
	rxcui string,
	lat string,
	ts string,
	lui string,
	stt string,
	sui string,
	ispref string,
	rxaui string,
	saui string,
	scui string,
	sdui string,
	sab string,
	tty string,
	code string,
	str string,
	srl string,
	suppress string,
	cvf string
)
using csv
options (
  header = "false",
  inferSchema = "false",
  delimiter = "|",
  path = "/FileStore/tables/prod/global/terminology/rxnorm/full/csv/rxnconso"
);

drop table if exists rxnorm.rxnsat;

create table rxnorm.rxnsat (
	rxcui string,
	lui string,
	sui string,
	rxaui string,
	stype string,
	code string,
	atui string,
	satui string,
	atn string,
	sab string,
	atv string,
	suppress string,
	cvf string
)
using csv
options (
  header = "false",
  inferSchema = "false",
  delimiter = "|",
  path = "/FileStore/tables/prod/global/terminology/rxnorm/full/csv/rxnsat"
);

drop table if exists rxnorm.rxnrel;

create table rxnorm.rxnrel (
	rxcui1 string,
	rxaui1 string,
	stype1 string,
	rel string,
	rxcui2 string,
	rxaui2 string,
	stype2 string,
	rela string,
	rui string,
	srui string,
	sab string,
	sl string,
	rg string,
	dir string,
	suppress string,
	cvf string
)
using csv
options (
  header = "false",
  inferSchema = "false",
  delimiter = "|",
  path = "/FileStore/tables/prod/global/terminology/rxnorm/full/csv/rxnrel"
);


