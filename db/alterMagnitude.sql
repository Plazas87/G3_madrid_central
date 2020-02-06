/* Alter table magnitude for acept null in scale atributes */

ALTER TABLE magnitude ALTER COLUMN max_value_excelent DROP NOT NULL;
ALTER TABLE magnitude ALTER COLUMN min_value_good DROP NOT NULL;
ALTER TABLE magnitude ALTER COLUMN max_value_good DROP NOT NULL;
ALTER TABLE magnitude ALTER COLUMN min_value_acceptable DROP NOT NULL;
ALTER TABLE magnitude ALTER COLUMN max_value_acceptable DROP NOT NULL;
ALTER TABLE magnitude ALTER COLUMN min_value_bad DROP NOT NULL;


