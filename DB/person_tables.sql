USE crss;

CREATE TABLE dimper_sex(
    sex_id INT PRIMARY KEY,
    sex_name VARCHAR(12)
)
;

INSERT INTO dimper_sex
VALUES
(1, "Male"),
(2, "Female"),
(3, "Other"),
(8, "Not Reported"),
(9, "Unknown");

CREATE TABLE dimper_pertype_cat(
    prcat_id INT PRIMARY KEY,
    ptcat_name VARCHAR(26)
)
;

INSERT INTO dimper_pertype_cat
VALUES
(1, "MOTORISTS"),
(2, "NON MOTORISTS OCCUPANT"),
(3, "NON MOTORISTS NONOCCUPANT")
;

CREATE TABLE dimper_pertype(
    pt_id INT PRIMARY KEY,
    pt_type VARCHAR(70),
    pt_cat INT,
    FOREIGN KEY (pt_cat) REFERENCES dimper_pertype_cat (prcat_id)
)
;

INSERT INTO dimper_pertype
VALUES
(1, "Driver of Vehicle In-Transport", 1),
(2, "Passenger of a Motor Vehicle In-Transport", 1),
(9, "Unknown Occupant Type in a Motor Vehicle In-Transport", 1),
(3, "Occupant of a Motor Vehicle Not In-Transport", 2),
(4, "Occupant of a Non-Motor Vehicle Transport Device", 2),
(5, "Pedestrian", 3),
(6, "Bicyclist", 3),
(7, "Other Pedalcyclist", 3),
(8, "Person on a Personal Conveyance", 3),
(10, "Person In/On a Building", 3),
(19, "Unknown Type of Non-Motorist", 3)
;

CREATE TABLE dimper_injury(
    inj_id INT PRIMARY KEY,
    inj_name VARCHAR(30)
)
;

INSERT INTO dimper_injury
VALUES
(0, "No Apparent Injury (O)"),
(1, "Possible Injury (C)"),
(2, "Supsected Minor Injury (B)"),
(3, "Suspected Serious Injuery (A)"),
(4, "Fatal Injury (K)"),
(5, "Injured, Severity Unknown (U)"),
(6, "Died Prior to Crash"),
(9, "Unknown/Not Reported")
;

CREATE TABLE dimper_seat(
    seat_id INT PRIMARY KEY,
    seat_name VARCHAR(75)
)
;
INSERT INTO dimper_seat
VALUES
(0, "Not a Motor Vehicle Occupant"),
(11, "Front Seat, Left Side (Drivers Side)"),
(12, "Front Seat, Middle"),
(13, "Front Seat, Right Side"),
(18, "Front Seat, Other"),
(19, "Front Seat, Unknown"),
(21, "Second Seat, Left Side"),
(22, "Second Seat, Middle"),
(23, "Second Seat, Right Side"),
(28, "Second Seat, Other"),
(29, "Second Seat, Unknown"),
(31, "Third Seat, Left Side"),
(32, "Third Seat, Middle"),
(33, "Third Seat, Right Side"),
(38, "Third Seat, Other"),
(39, "Third Seat, Unknown"),
(41, "Fourth Seat, Left Side"),
(42, "Fourth Seat, Middle"),
(43, "Fourth Seat, Right Side"),
(48, "Fourth Seat, Other"),
(49, "Fourth Seat, Unknown"),
(50, "Sleeper Section of Cab (Truck)"),
(51, "Other Passenger in Enclosed Passenger or Cargo Area"),
(52, "Other Passenger in Unenclosed Passenger or Cargo Area"),
(53, "Other Passenger in Passenger or Cargo Area, Unknown Whether or Not Enclosed"),
(54, "Trailing Unit"),
(55, "Riding on Exterior of Vehicle"),
(56, "Appended to a Motor Vehicle for Motion"),
(98, "Not Reported"),
(99, "Unknown/Reported as Unknown")
;

CREATE TABLE dimper_safety_misuse(
    sm_id INT PRIMARY KEY,
    sm_name VARCHAR(24)
)
;
INSERT INTO dimper_safety_misuse
VALUES
(0, "No"),
(1, "Yes"),
(7, "None Used/Not Applicable"),
(8, "Not a MV Occupant")
;

CREATE TABLE dimper_airbag(
    airbag_id INT PRIMARY KEY,
    airbag_name VARCHAR(40)
)
;
INSERT INTO dimper_airbag
VALUES
(0, "Not Applicable"),
(1, "Deployed-Front"),
(2, "Deployed-Side(Door, Seat Back)"),
(3, "Deployed-Curtain/Roof"),
(7, "Deployed-Other(Knee, Air Belt, etc.)"),
(8, "Deployed-Combination"),
(9, "Deployment Unknown"),
(20, "Not Deployed"),
(28, "Switched Off"),
(97, "Not an MV Occupant"),
(98, "Not Reported"),
(99, "Reported as Unknown")
;

CREATE TABLE dimper_eject(
    eject_id INT PRIMARY KEY,
    eject_name VARCHAR(22)
)
;
INSERT INTO dimper_eject
VALUES
(0, "Not Ejected"),
(1, "Totally Ejected"),
(2, "Partially Ejected"),
(3, "Ejected-Unknown Degree"),
(7, "Not Reported"),
(8, "Not Applicable"),
(9, "Reported as Unknown")
;

CREATE TABLE dimper_alcohol(
    alc_id INT PRIMARY KEY,
    alc_name VARCHAR(25)
)
;
INSERT INTO dimper_alcohol
VALUES
(0, "No(Alcohol not Involved)"),
(1, "Yes(Alcohol Involved)"),
(8, "Not Reported"),
(9, "Reported as Unknown")
;

CREATE TABLE dimper_drugs(
    drug_id INT PRIMARY KEY,
    drug_name VARCHAR(25)
)
;
INSERT INTO dimper_drugs
VALUES
(0, "No Drugs not Involved"),
(1, "Yes Drugs Involved"),
(8, "Not Reported"),
(9, "Reported as Unknownn")
;

CREATE TABLE dimper_hospital(
    hosp_id INT PRIMARY KEY,
    hospt_name VARCHAR(30)
)
;
INSERT INTO dimper_hospital
VALUES
(0, "Not Transported"),
(1, "EMS Air"),
(2, "Law Enforcement"),
(3, "EMS Unknown Mode"),
(4, "Transported Unknown Source"),
(5, "EMS Ground"),
(6, "Other"),
(8, "Not Reported"),
(9, "Reported as Unknown")
;

CREATE TABLE dimper_location(
    loc_id INT PRIMARY KEY,
    loc_name VARCHAR(64)
)
;
INSERT INTO dimper_location
VALUES
(0, "Not Applicable-Motor Vehicle Occupant"),
(1, "At Intersection-In Marked Crosswalk"),
(2, "At Intersection-Unmarked/Unknown if Marked Crosswalk"),
(3, "At Intersection-Not in Crosswalk"),
(9, "At Intersection-Unknown Location"),
(10, "Not at Intersection-In Marked Crosswalk"),
(11, "Not at Intersection-On Roadway, Not in Marked Crosswalk Unknown"),
(13, "Not at Intersection-On Roadway, Crosswalk Availability Unknown"),
(14, "Parking Lane/Zone "),
(16, "Bicycle Lane"),
(20, "Shoulder/Roadside"),
(21, "Sidewalk"),
(22, "Median/Crossing Island"),
(23, "Driveway Access"),
(24, "Shared-Use Path"),
(25, "Non-Trafficway Area"),
(28, "Other"),
(98, "Not Reported"),
(99, "Reported as Unknown Location")
;

ALTER TABLE `crss`.`fact_person` 
ADD INDEX `per_alcohol_idx` (`alcohol` ASC) VISIBLE,
ADD INDEX `per_airbag_idx` (`airbag` ASC) VISIBLE,
ADD INDEX `per_drugs_idx` (`drugs` ASC) VISIBLE,
ADD INDEX `per_eject_idx` (`eject` ASC) VISIBLE,
ADD INDEX `per_hospital_idx` (`hospital` ASC) VISIBLE,
ADD INDEX `per_location_idx` (`location` ASC) VISIBLE,
ADD INDEX `per_type_idx` (`per_type` ASC) VISIBLE,
ADD INDEX `per_safemisuse_idx` (`safety_misuse` ASC) VISIBLE,
ADD INDEX `per_seat_idx` (`seat` ASC) VISIBLE,
ADD INDEX `per_sex_idx` (`sex` ASC) VISIBLE;
;
ALTER TABLE `crss`.`fact_person` 
ADD CONSTRAINT `per_airbag`
  FOREIGN KEY (`air_bag`)
  REFERENCES `crss`.`dimper_airbag` (`airbag_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_alcohol`
  FOREIGN KEY (`alcohol`)
  REFERENCES `crss`.`dimper_alcohol` (`alc_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_drugs`
  FOREIGN KEY (`drugs`)
  REFERENCES `crss`.`dimper_drugs` (`drug_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_eject`
  FOREIGN KEY (`eject`)
  REFERENCES `crss`.`dimper_eject` (`eject_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_hospital`
  FOREIGN KEY (`hospital`)
  REFERENCES `crss`.`dimper_hospital` (`hosp_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_location`
  FOREIGN KEY (`location`)
  REFERENCES `crss`.`dimper_location` (`loc_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_type`
  FOREIGN KEY (`per_type`)
  REFERENCES `crss`.`dimper_pertype` (`pt_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_safemisuse`
  FOREIGN KEY (`safety_misuse`)
  REFERENCES `crss`.`dimper_safety_misuse` (`sm_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_seat`
  FOREIGN KEY (`seat`)
  REFERENCES `crss`.`dimper_seat` (`seat_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `per_sex`
  FOREIGN KEY (`sex`)
  REFERENCES `crss`.`dimper_sex` (`sex_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;
  
