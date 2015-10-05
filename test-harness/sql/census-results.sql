DROP TABLE IF EXISTS census_results CASCADE;

CREATE TABLE census_results(
  inputAddress varchar(255) NOT NULL,
  inputLongitude numeric NOT NULL,
  inputLatitude numeric NOT NULL,
  inputTract varchar(11),
  censusLongitude numeric,
  censusLatitude numeric,
  distance numeric,
  censusTract varchar(11)
);

/* 
  Census results that resulted in positive geocodes
*/

CREATE VIEW census_results_not_null as SELECT * FROM census_results where censuslongitude <> 0.0 AND censuslatitude <> 0.0;

/* 
  Census results that resulted in negative geocodes
*/

CREATE VIEW census_results_null as SELECT * FROM census_results where censuslongitude = 0.0 AND censuslatitude = 0.0;


/*
  Census results that resulted in positive geocodes, but on the wrong Census Tract
*/

CREATE VIEW census_results_wrong_tract as SELECT * FROM census_results_not_null WHERE inputTract <> censusTract;


/* 
  Census Geocoder Success rate
*/

-- SELECT success_rate FROM (SELECT COUNT(*) FROM census_results_null / SELECT COUNT(*) FROM census_results_not_null);

