-- CREATE EXTENSION postgis;

DROP TABLE citi_bike_dump;
DROP TABLE Trips;
DROP TABLE usagebyday;
DROP TABLE usagebygender;
DROP TABLE usagebyage;
DROP TABLE usagebyzip;
DROP TABLE Stations;

create table citi_bike_dump
(
	tripduration integer,
	starttime timestamp,
	startday timestamp,
	stoptime timestamp,
	stopday timestamp,
	"start station id" integer,
	"start station name" text,
	"start station latitude" double precision,
	"start station longitude" double precision,
	"end station id" integer,
	"end station name" text,
	"end station latitude" double precision,
	"end station longitude" double precision,
	bikeid integer,
	usertype text,
	"birth year" text,
	gender integer
);

COPY citi_bike_dump
(
	tripduration,
	starttime,
	startday,
	stoptime,
	stopday,
	"start station id",
	"start station name",
	"start station latitude",
	"start station longitude",
	"end station id",
	"end station name",
	"end station latitude",
	"end station longitude",
	bikeid,
	usertype,
	"birth year",
	gender
)
    -- renamed csv for quality of life
FROM '/Users/ecr/danielv/336/citi.csv' DELIMITER ',' CSV HEADER;

-- Deal will alternative null string in birth year column
ALTER TABLE citi_bike_dump
  ALTER COLUMN "birth year" TYPE int
    USING (NULLIF("birth year", '\N')::int);

--
CREATE TABLE Stations(
    id INT PRIMARY KEY ,
    name text,
    latitude double precision,
    longitude double precision
);

-- Find all unique stations in data & insert into stations table

INSERT INTO Stations(id,name,latitude,longitude)
SELECT DISTINCT * FROM (
SELECT DISTINCT "start station id" as id,
	"start station name" as name,
	"start station latitude" as latitude,
	"start station longitude" as longitude FROM citi_bike_dump
UNION ALL
SELECT DISTINCT
"end station id" as id,
	"end station name" as name,
	"end station latitude" as latitude,
	"end station longitude" as longitude FROM citi_bike_dump) as s;

-- add "point" representation for zip code bonus later on
ALTER TABLE Stations ADD COLUMN coord point;
UPDATE Stations SET coord=point(latitude,longitude);

CREATE TABLE Trips
(
    station_id          int REFERENCES stations("id"),
    min_tripduration    int,
    max_tripduration    int,
    avg_tripduration    double precision,
    number_start_users  int,
    number_return_users int
);

CREATE OR REPLACE FUNCTION insert_trips(int[]) RETURNS bool AS $$
    DECLARE
    r int;
BEGIN
  FOREACH r IN ARRAY $1 LOOP
        INSERT INTO Trips VALUES(
                             r,
    (SELECT min(tripduration) FROM citi_bike_dump where "start station id" = r OR "end station id" = r),
    (SELECT max(tripduration) FROM citi_bike_dump where "start station id" = r OR "end station id" = r),
    (SELECT avg(tripduration)::double precision FROM citi_bike_dump where "start station id" = r OR "end station id" = r),
    (SELECT COUNT("start station id") FROM citi_bike_dump where "start station id"=r),
    (SELECT COUNT("end station id") FROM citi_bike_dump where "end station id"=r));
  END LOOP;
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- insert trip entry for every station_id
SELECT insert_trips(array_agg(id)) FROM public.stations;

-- UsageByDay(StationId, NumberWeekdayStartUsers, NumberWeekdayReturnUsers, NumberWeekendStartUsers, NumberWeekendReturnUsers)
CREATE TABLE UsageByDay(
    station_id int REFERENCES stations(id),
    number_weekday_start_users int,
    number_weekday_return_users int,
    number_weekend_start_users int,
    number_weekend_return_users int
);

CREATE OR REPLACE FUNCTION insert_tripdays(int[]) RETURNS bool AS $$
    DECLARE
    r int;
BEGIN
  FOREACH r IN ARRAY $1 LOOP
        INSERT INTO UsageByDay VALUES(
                             r,
    (SELECT COUNT(*) FROM citi_bike_dump WHERE date_part('dow',starttime) BETWEEN 1 AND 5
                                                AND "start station id" = r),
    (SELECT COUNT(*) FROM citi_bike_dump WHERE date_part('dow',starttime) BETWEEN 1 AND 5
                                                AND "end station id" = r),
    (SELECT COUNT(*) FROM citi_bike_dump WHERE (date_part('dow',starttime) = 0 OR date_part('dow',starttime) = 6)
                                                AND "start station id" = r),
    (SELECT COUNT(*) FROM citi_bike_dump WHERE (date_part('dow',starttime) = 0 OR date_part('dow',starttime) = 6)
                                                AND "end station id" = r));
  END LOOP;
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- insert weekend/weekday stats for every id
SELECT insert_tripdays(array_agg(id)) from stations;

--UsageByGender(StationId, NumberMaleStartUsers, NumberFemaleStartUsers, NumberMaleReturnUsers, NumberFemaleReturnUsers)
CREATE TABLE UsageByGender(
    station_id int REFERENCES stations(id),
    number_male_start_users int,
    number_female_start_users int,
    number_male_return_users int,
    number_female_return_users int
);

-- 1 = male, 2 = female in citibike gender data

CREATE OR REPLACE FUNCTION insert_tripgender(int[]) RETURNS bool AS $$
    DECLARE
    r int;
BEGIN
  FOREACH r IN ARRAY $1 LOOP
        INSERT INTO UsageByGender VALUES(
                             r,
            (SELECT COUNT(*) FROM citi_bike_dump WHERE "start station id" = r AND gender = 1),
            (SELECT COUNT(*) FROM citi_bike_dump WHERE "start station id" = r AND gender = 2),
            (SELECT COUNT(*) FROM citi_bike_dump WHERE "end station id" = r AND gender = 1),
            (SELECT COUNT(*) FROM citi_bike_dump WHERE "end station id" = r AND gender = 2)
                                                );
  END LOOP;
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

SELECT insert_tripgender(array_agg(id)) from stations;

-- UsageByAge(StationId, NumberMaleUsersUnder18, NumberMaleUsers18To40,
-- NumberMaleUsersOver40, NumberFemaleUsersUnder18,
-- NumberFemaleUsers18To40, NumberFemaleUsersOver40)

CREATE TABLE UsageByAge(
    station_id int REFERENCES stations(id),
    number_male_users_under_18 int,
    number_male_users_18_to_40 int,
    number_male_users_over_40 int,
    number_female_users_under_18 int,
    number_female_users_18_to_40 int,
    number_female_users_over_40 int
);

CREATE OR REPLACE FUNCTION insert_tripage(int[]) RETURNS bool AS $$
    DECLARE
    r int;
BEGIN
  FOREACH r IN ARRAY $1 LOOP
        INSERT INTO UsageByAge VALUES(
                             r,
            (SELECT COUNT(*) FROM citi_bike_dump WHERE (EXTRACT(YEAR FROM starttime) - "birth year") < 18 AND gender=1 AND ("start station id"=r OR "end station id"=r)),
            (SELECT COUNT(*) FROM citi_bike_dump WHERE ((EXTRACT(YEAR FROM starttime) - "birth year") BETWEEN 18 AND 40) AND gender=1 AND ("start station id"=r OR "end station id"=r)),
            (SELECT COUNT(*) FROM citi_bike_dump WHERE (EXTRACT(YEAR FROM starttime) - "birth year") > 40 AND gender=1 AND ("start station id"=r OR "end station id"=r)),
            (SELECT COUNT(*) FROM citi_bike_dump WHERE (EXTRACT(YEAR FROM starttime) - "birth year") < 18 AND gender=2 AND ("start station id"=r OR "end station id"=r)),
            (SELECT COUNT(*) FROM citi_bike_dump WHERE ((EXTRACT(YEAR FROM starttime) - "birth year") BETWEEN 18 AND 40) AND gender=2 AND ("start station id"=r OR "end station id"=r)),
            (SELECT COUNT(*) FROM citi_bike_dump WHERE (EXTRACT(YEAR FROM starttime) - "birth year") > 40 AND gender=2 AND ("start station id"=r OR "end station id"=r))
        );
  END LOOP;
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

SELECT insert_tripage(array_agg(id)) from stations;


-- Most frequent trips betweeen any two stations by the day of the week

SELECT ARRAY["start station id","end station id"] as trip, COUNT(*) as number_sunday FROM citi_bike_dump a
    WHERE date_part('dow',starttime)=0
    AND "start station id" != "end station id"
    GROUP BY(ARRAY["start station id","end station id"])
    ORDER BY number_sunday DESC;

SELECT ARRAY["start station id","end station id"] as trip, COUNT(*) as number_monday FROM citi_bike_dump a
    WHERE date_part('dow',starttime)=1
    AND "start station id" != "end station id"
    GROUP BY(ARRAY["start station id","end station id"])
    ORDER BY number_monday DESC;

SELECT ARRAY["start station id","end station id"] as trip, COUNT(*) as number_tuesday FROM citi_bike_dump a
    WHERE date_part('dow',starttime)=2
    AND "start station id" != "end station id"
    GROUP BY(ARRAY["start station id","end station id"])
    ORDER BY number_tuesday DESC;

SELECT ARRAY["start station id","end station id"] as trip, COUNT(*) as number_wednesday FROM citi_bike_dump a
    WHERE date_part('dow',starttime)=3
    AND "start station id" != "end station id"
    GROUP BY(ARRAY["start station id","end station id"])
    ORDER BY number_wednesday DESC;

SELECT ARRAY["start station id","end station id"] as trip, COUNT(*) as number_thursday FROM citi_bike_dump a
    WHERE date_part('dow',starttime)=4
    AND "start station id" != "end station id"
    GROUP BY(ARRAY["start station id","end station id"])
    ORDER BY number_thursday DESC;

SELECT ARRAY["start station id","end station id"] as trip, COUNT(*) as number_friday FROM citi_bike_dump a
    WHERE date_part('dow',starttime)=5
    AND "start station id" != "end station id"
    GROUP BY(ARRAY["start station id","end station id"])
    ORDER BY number_friday DESC;

SELECT ARRAY["start station id","end station id"] as trip, COUNT(*) as number_saturday FROM citi_bike_dump a
    WHERE date_part('dow',starttime)=6
    AND "start station id" != "end station id"
    GROUP BY(ARRAY["start station id","end station id"])
    ORDER BY number_saturday DESC;


--Create appropriate SQL expressions to find permanently dormant or vacant stations.
-- The following query shows the latest starting/ending trips for each station ordered by starttime
-- The longer its been since a station logged a trip, the more likely it is dormant/vacant
SELECT "start station id" as station_id,latest_start_trip,latest_end_trip FROM
(SELECT max(starttime) as latest_start_trip,"start station id" FROM citi_bike_dump GROUP BY("start station id")) q1
    FULL OUTER JOIN
(SELECT max(starttime) as latest_end_trip,"end station id" FROM citi_bike_dump GROUP BY("end station id")) q2
ON q1."start station id"=q2."end station id"
ORDER BY latest_start_trip,latest_end_trip ASC;

-- Zip Codes
CREATE TABLE zipcode_dump(
    zip char(5) PRIMARY KEY,
    latitude double precision,
    longitude double precision
);

-- Zip code coordinates csv retrieved from https://gist.github.com/abatko/ee7b24db82a6f50cfce02afafa1dfd1e
-- curl https://gist.github.com/abatko/ee7b24db82a6f50cfce02afafa1dfd1e > zipcode_coords.csv
COPY zipcode_dump(zip,latitude,longitude)
FROM '/Users/ecr/danielv/336/zipcode_coords.csv' DELIMITER ',' CSV HEADER;

ALTER TABLE zipcode_dump ADD COLUMN coord point;
UPDATE zipcode_dump SET coord=point(latitude,longitude);

CREATE UNIQUE INDEX zip_index
ON zipcode_dump (zip);


-- Find the nearest zip code to any given lat/lon pooint
CREATE OR REPLACE FUNCTION nearest_zip(p point) returns char(5) AS $$
DECLARE
    nearest_zip char(5);
BEGIN
    WITH closest_candidates AS (
      SELECT
        zip,coord
      FROM
        zipcode_dump zips
      ORDER BY
        zips.coord::geometry <->
        p::geometry
      LIMIT 100
    )
    SELECT zip INTO nearest_zip
    FROM closest_candidates
    ORDER BY
      ST_Distance(
        coord::geometry,
        p::geometry
        )
    LIMIT 1;
    RETURN nearest_zip;
END;
$$ LANGUAGE plpgsql;

-- Update stations table with zip codes
ALTER TABLE stations ADD COLUMN zip char(5) REFERENCES zipcode_dump(zip);
UPDATE stations SET zip=nearest_zip(coord);

-- Usage by zip code
CREATE TABLE UsageByZip(
    zip char(5) REFERENCES zipcode_dump(zip),
    start_station_trip_count int,
    end_station_trip_count int
);

INSERT INTO UsageByZip
SELECT q1.zip as zip, start_station_trip_count,end_station_trip_count FROM
(SELECT zip,COUNT(*) as start_station_trip_count FROM citi_bike_dump cb FULL OUTER JOIN stations st ON cb."start station id"=st.id GROUP BY zip) q1
JOIN
(SELECT zip,COUNT(*) as end_station_trip_count FROM citi_bike_dump cb FULL OUTER JOIN stations st ON cb."end station id"=st.id GROUP BY zip) q2
ON q1.zip=q2.zip
ORDER BY start_station_trip_count DESC,end_station_trip_count DESC;