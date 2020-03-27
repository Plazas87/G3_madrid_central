CREATE DATABASE madridcentral;

CREATE TABLE IF NOT EXISTS time(
	time_id serial PRIMARY KEY,
	hour INT NOT NULL

);
CREATE TABLE IF NOT EXISTS day(
	day_id serial PRIMARY KEY,
	day INT NOT NULL,
	month INT NOT NULL,
	year INT NOT NULL
);
CREATE TABLE IF NOT EXISTS station(
	station_id serial PRIMARY KEY,
	name VARCHAR(70),
	type VARCHAR(40),
	address VARCHAR(70),
	latitude FLOAT NOT NULL,
	longitude FLOAT NOT NULL,
	altitude FLOAT NOT NULL,
	start_date DATE
);
CREATE TABLE IF NOT EXISTS magnitude(
	magnitude_id serial PRIMARY KEY,
	name VARCHAR(50),
	abbreviation VARCHAR(10),
	unit VARCHAR(10),
	max_value_excelent FLOAT,
	min_value_good FLOAT,
	max_value_good FLOAT,
	min_value_acceptable FLOAT,
	max_value_acceptable FLOAT,
	min_value_bad FLOAT
);
CREATE TABLE IF NOT EXISTS measurement(
	station_id INT NOT NULL,
	day_id INT NOT NULL,
	time_id INT NOT NULL,
	magnitude_id INT NOT NULL,
	value FLOAT NOT NULL,
	validation BOOLEAN,
	foreign key(station_id) REFERENCES station(station_id),
	foreign key(day_id) REFERENCES day(day_id),
	foreign key(time_id) REFERENCES time(time_id), 
	foreign key(magnitude_id) REFERENCES magnitude(magnitude_id),
	primary key(station_id, day_id, time_id, magnitude_id)
);
CREATE TABLE IF NOT EXISTS traffic(
	station_id INT NOT NULL,
	day_id INT NOT NULL,
	time_id INT NOT NULL,
	magnitude_id varchar(15),
	value FLOAT NOT NULL,
	validation varchar(1),
	foreign key(day_id) REFERENCES day(day_id),
	foreign key(time_id) REFERENCES time(time_id), 
	primary key(day_id, time_id)
);