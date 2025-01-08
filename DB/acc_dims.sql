CREATE DATABASE CRSS;
USE CRSS;

CREATE TABLE dim_light_cond(
    light_id INT PRIMARY KEY, 
    lgt_cond VARCHAR(25)
    )
    ;
    
INSERT INTO dim_light_cond 
VALUES
(1, "Daylight"),
(2, "Dark-No Light"),
(3, "Dark-Lighted"),
(4, "Dawn"),
(5, "Dusk"),
(6, "Dark-Unknown Lighting"),
(7, "other"),
(8, "Not Reported"),
(9, "Reported As Unknown");

CREATE TABLE dim_weather(
    weather_id INT PRIMARY KEY, 
    weather_cond VARCHAR(25)
    )
    ;

INSERT INTO dim_weather 
VALUES 
(1, "Clear"),
(2, "Rain"),
(3, "Sleet or Hail"),
(4, "Snow"),
(5, "Fog, Smog, Smoke"),
(6, "Severe Crosswinds"),
(7, "Blowing Sand, Soil Dirt"),
(8, "Other"),
(10, "Cloudy"),
(11, "Blowing Snow"),
(12, "Freezing Rain or Drizzle"),
(98, "Not Reported"),
(99, "Reported as Unknown");

CREATE TABLE dim_schoolbus(
    bus_id INT PRIMARY KEY, 
    bus_involved VARCHAR(3)
    )
    ;

INSERT INTO dim_schoolbus
VALUES
(0, "No"),
(1, "Yes");

CREATE TABLE dim_interstate_hwy(
    hwy_id INT PRIMARY KEY, 
    hwy_status VARCHAR(8)
    )
    ;

INSERT INTO dim_interstate_hwy
VALUES
(0, "No"),
(1, "Yes"),
(9, "Unknown");

CREATE TABLE dim_injury_severity(
    injury_id INT PRIMARY KEY, 
    injury_status VARCHAR(30)
    )
    ;

INSERT INTO dim_injury_severity
VALUES
(0, "No Apparent Injury"),
(1, "Possible Injury"),
(2, "Suspected Minor Injury"),
(3, "Suspected Serious Injury"),
(4, "Fatal"),
(5, "Injured, Severity Unknown"),
(6, "Died Prior to Crash"),
(8, "No Person Involved in Crash"),
(9, "Unknown/Not Reported ");

CREATE TABLE dim_alcohol(
    alcohol_id INT PRIMARY KEY, 
    alcohol_status VARCHAR(25)
    )
    ;

INSERT INTO dim_alcohol
VALUES
(1, "Alcohol Involved"),
(2, "No Alcohol Involved"),
(8, "No Applicable Person"),
(9, "Unknown");

CREATE TABLE dim_workzone(
    workzone_id INT PRIMARY KEY, 
    workzone_status VARCHAR(25)
    )
    ;

INSERT INTO dim_workzone
VALUES
(0, "None"),
(1, "Construction"),
(2, "Maintenance"),
(3, "Utility"),
(4, "Work Zone, Type Unknown");

CREATE TABLE dim_roadway_relation(
    road_id INT PRIMARY KEY, 
    road_status VARCHAR(45)
    )
    ;

INSERT INTO dim_roadway_relation
VALUES
(1, "On Roadway"),
(2, "On Shoulder"),
(3, "On Median"),
(4, "On Roadside"),
(5, "Outside Trafficway"),
(6, "Off Roadway-Location Unknown"),
(7, "In Parking Lane/Zone"),
(8, "Gore"),
(10, "Separator"),
(11, "Continuous Left Turn Lane"),
(12, "Pedestrian Refuge Island or Traffic Island"),
(98, "Not Reported"),
(99, "Reported as Unknown")
;

CREATE TABLE dim_intersection(
    intersect_id INT PRIMARY KEY, 
    intersect_status VARCHAR(30)
    )
    ;

INSERT INTO dim_intersection
VALUES
(1, "Not an Intersection"),
(2, "Four-Way Intersection"),
(3, "T-Intersection"),
(4, "Y-Intersection"),
(5, "Traffic Circle"),
(6, "Roundabout"),
(7, "Five-Point, or More"),
(10, "L-Intersection"),
(11, "Other Intersection Type"),
(98, "Not Reported"),
(99, "Reported as Unknown");

CREATE TABLE dim_relatedjunction1(
    reljct1_id INT PRIMARY KEY, 
    reljct1_status VARCHAR(25)
    )
    ;

INSERT INTO dim_relatedjunction1
VALUES
(0, "No"),
(1, "Yes"),
(8, "Not Reported"),
(9, "Reported as Unknow");

CREATE TABLE dim_relatedjunction2(
    reljct2_id INT PRIMARY KEY, 
    reljct2_status VARCHAR(50)
    )
    ;

INSERT INTO dim_relatedjunction2
VALUES
(1, "Non-Junction"),
(2, "Intersection"),
(3, "Intersection Related"),
(4, "Driveway Access"),
(5, "Entrance/Exit Ramp Related"),
(6, "Railway Grade Crossing"),
(7, "Crossover Related"),
(8, "Driveway Access Related"),
(16, "Shared-Use Path Crossing"),
(17, "Acceleration/Deceleration Lane"),
(18, "Through Roadway"),
(19, "Other Location Within Interchange Area"),
(20, "Entrance/Exit Ramp"),
(98, "Not Reported"),
(99, "Reported as Unknown");

CREATE TABLE dim_manor_collision(
    mancoll_id INT PRIMARY KEY, 
    mancoll_status VARCHAR(75)
    )
    ;

INSERT INTO dim_manor_collision
VALUES
(0, "First Harmful Event Was Not a Collision With Motor Vehicle In Transport"),
(1, "Front-to-Rear"),
(2, "Front-to-Front"),
(6, "Angle"),
(7, "Sideswipe – Same Direction"),
(8, "Sideswipe – Opposite Direction"),
(9, "Rear-to-Side"),
(10, "Rear-to-Rear"),
(11, "Other"),
(98, "Not Reported"),
(99, "Reported as Unknown")
;

CREATE TABLE dim_harmfulevent_sub(
    hes_id INT PRIMARY KEY, 
    hes_status VARCHAR(40)
    )
    ;

INSERT INTO dim_harmfulevent_sub
VALUES
(1, "Non-Collision"),
(2, "Collision with Vehicle In-Transit"),
(3, "Collision with Non-Fixed Object"),
(4, "Collision with Fixed Object")
;

CREATE TABLE dim_harmfulevent(
    harm_ev_id INT PRIMARY KEY,
    harm_ev_sub INT,
    harm_ev_status VARCHAR(250) FOREIGN KEY (harm_ev_sub) REFERENCES harm_ev_sub.hes_id
    )
    ;

INSERT INTO dim_harmfulevent
VALUES
(1, 1, "Rollover/Overturn"),
(2, 1, "Fire/Explosion"),
(3, 1, "Immersion or Partial Immersion"),
(4, 1, "Gas Inhalation"),
(5, 1, "Fell/Jumped From Vehicle"),
(6, 1, "Injured in Vehicle (Non-Collision)"),
(7, 1, "Other Non-Collision"),
(16, 1, "Thrown or Falling Object"),
(44, 1, "Pavement Surface Irregularity (Ruts, Potholes, Grates, etc.)"),
(51, 1, "Jackknife (Harmful to This Vehicle)"),
(72, 1, "Cargo/Equipment Loss, Shift, or Damage (Harmful)"),
(12, 2, "Motor Vehicle In-Transport"),
(54, 2, "Motor Vehicle In-Transport Strikes or Is Struck by Cargo, Persons or Objects Set-in-Motion From/by Another Motor Vehicle InTransport"),
(55, 2, "Motor Vehicle in Motion Outside the Trafficway"),
(8, 3, "Pedestrian"),
(9, 3, "Pedalcyclist"),
(10, 3, "Railway Vehicle"),
(11, 3, "Live Animal"),
(14, 3, "Parked Motor Vehicle"),
(15, 3, "Non-Motorist on Personal Conveyance"),
(18, 3, "Other Object Not Fixed"),
(45, 3, "Working Motor Vehicle"),
(49, 3, "Ridden Animal or Animal Drawn Conveyance"),
(73, 3, "Object That Had Fallen From Motor Vehicle In-Transport "),
(74, 3, "Road Vehicle on Rails"),
(91, 3, "Unknown Object Not Fixed"),
(17, 4, "Boulder"),
(19, 4, "Building"),
(20, 4, "Impact Attenuator/Crash Cushion"),
(21, 4, "Bridge Pier or Support"),
(23, 4, "Bridge Rail (Includes Parapet)"),
(24, 4, "Guardrail Face"),
(25, 4, "Concrete Traffic Barrier"),
(26, 4, "Other Traffic Barrier"),
(30, 4, "Utility Pole/Light Support"),
(31, 4, "Post, Pole or Other Support"),
(32, 4, "Culvert"),
(33, 4, "Curb"),
(34, 4, "Ditch"),
(35, 4, "Embankment"),
(38, 4, "Fence"),
(39, 4, "Wall"),
(40, 4, "Fire Hydrant"),
(41, 4, "Shrubbery"),
(42, 4, "Tree (Standing Only)"),
(43, 4, "Other Fixed Object"),
(46, 4, "Traffic Signal Support"),
(48, 4, "Snow Bank"),
(50, 4, "Bridge Overhead Structure"),
(52, 4, "Guardrail End"),
(53, 4, "Mail Box"),
(57, 4, "Cable Barrier"),
(58, 4, "Ground"),
(59, 4, "Traffic Sign Support"),
(93, 4, "Unknown Fixed Object"),
(98, 4, "Harmful Event, Details Not Reported (Since 2019)"),
(99, 4, "Reported as Unknown")
;

CREATE TABLE dim_dayofweek(
    day_id INT PRIMARY KEY, 
    day_name VARCHAR(10),
    day_category VARCHAR(7)
    )
    ;

INSERT INTO dim_dayofweek
VALUES
(1, "Sunday", "Weekend"),
(2, "Monday", "Weekday"),
(3, "Tuesday", "Weekday"),
(4, "Wednesday", "Weekday"),
(5, "Thursday", "Weekday"),
(6, "Friday", "Weekday"),
(7, "Saturday", "Weekend"),
(9, "Unknown", "Unknown")
;