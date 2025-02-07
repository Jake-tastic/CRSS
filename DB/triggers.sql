-- values 11, 12 and 13 were only used in years 2020 and 2021
-- can be updated to 8 for a generalized description
DELIMITER $$

CREATE TRIGGER persontype 
BEFORE INSERT ON fact_person
FOR EACH ROW
BEGIN
    IF NEW.per_type IN (11, 12, 13)
        THEN SET NEW.per_type = 8;
    END IF;
END$$

DELIMITER ;

DELIMITER $$

CREATE TRIGGER veh_body
BEFORE INSERT ON fact_vehicle
FOR EACH ROW
BEGIN
	IF NEW.veh_body IN (30, 31)
		THEN SET NEW.veh_body = 34;
    END IF;
END$$

DELIMITER ;


UPDATE fact_vehicle
SET towed = 6
WHERE towed IN (2, 3, 7);