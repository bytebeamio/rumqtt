-- DROP TABLE IF EXISTS demo;
-- CREATE DATABASE demo;

CREATE TABLE IF NOT EXISTS metrics
(	id text,
	sequence int,
	"timestamp" timestamp NOT NULL DEFAULT now(),
	"date" date GENERATED ALWAYS AS (date(timestamp)) STORED,
	total_sent_size int,
	total_disk_size int,
	error_count int,
	errors text,
	lost_segments int
);
SELECT create_hypertable('metrics', 'timestamp');


CREATE TABLE IF NOT EXISTS gps
(	id text,
	sequence int,
	"timestamp" timestamp NOT NULL DEFAULT now(),
	"date" date GENERATED ALWAYS AS (date(timestamp)) STORED,
	latitude decimal,
	longitude decimal
);
SELECT create_hypertable('gps', 'timestamp');	


CREATE TABLE IF NOT EXISTS imu 
(	id text,
	sequence int,
	"timestamp" timestamp NOT NULL DEFAULT now(),
	"date" date GENERATED ALWAYS AS (date(timestamp)) STORED,
	ax decimal,
	ay decimal,
	az decimal,
	pitch decimal,
	roll decimal,
	yaw decimal,
	magx decimal,
	magy decimal,
	magz decimal
);
SELECT create_hypertable('imu', 'timestamp');	



CREATE TABLE IF NOT EXISTS device_shadow
(	id text,
	sequence int,
	"timestamp" timestamp NOT NULL DEFAULT now(),
	"date" date GENERATED ALWAYS AS (date(timestamp)) STORED,
	mode text,
	status text,
	firmware_version text,
	config_version text,
	distance_travelled int,
	range int,
	SOC decimal
);
SELECT create_hypertable('device_shadow', 'timestamp');	


CREATE TABLE IF NOT EXISTS peripheral_state
(	id text,
	sequence int,
	"timestamp" timestamp NOT NULL DEFAULT now(),
	"date" date GENERATED ALWAYS AS (date(timestamp)) STORED,
	gps_status text,
	gsm_status text,
	imu_status text,
	left_indicator text,
	right_indicator text,
	headlamp text,
	horn text,
	left_brake text,
	right_brake text
);
SELECT create_hypertable('peripheral_state', 'timestamp');	


CREATE TABLE IF NOT EXISTS motor
(	id text,
	sequence int,
	"timestamp" timestamp NOT NULL DEFAULT now(),
	"date" date GENERATED ALWAYS AS (date(timestamp)) STORED,
	motor_temperature1 decimal,
	motor_temperature2 decimal,
	motor_temperature3 decimal,
	motor_voltage decimal,
	motor_current decimal,
	motor_rpm int
);
SELECT create_hypertable('motor', 'timestamp');	


CREATE TABLE IF NOT EXISTS bms 
(	id text,
	sequence int,
	"timestamp" timestamp NOT NULL DEFAULT now(),
	"date" date GENERATED ALWAYS AS (date(timestamp)) STORED,
	periodicity_ms int,
	mosfet_temperature decimal,
	ambient_temperature decimal,
	mosfet_status int,
	cell_voltage_count int,
	cell_voltage_1 decimal,
	cell_voltage_2 decimal,
	cell_voltage_3 decimal,
	cell_voltage_4 decimal,
	cell_voltage_5 decimal,
	cell_voltage_6 decimal,
	cell_voltage_7 decimal,
	cell_voltage_8 decimal,
	cell_voltage_9 decimal,
	cell_voltage_10 decimal,
	cell_voltage_11 decimal,
	cell_voltage_12 decimal,
	cell_voltage_13 decimal,
	cell_voltage_14 decimal,
	cell_voltage_15 decimal,
	cell_voltage_16 decimal,
	cell_thermistor_count int,
	cell_temp_1 decimal,
	cell_temp_2 decimal,
	cell_temp_3 decimal,
	cell_temp_4 decimal,
	cell_temp_5 decimal,
	cell_temp_6 decimal,
	cell_temp_7 decimal,
	cell_temp_8 decimal,
	cell_balancing_status int,
	pack_voltage decimal,
	pack_current decimal,
	pack_soc decimal,
	pack_soh decimal,
	pack_sop decimal,
	pack_cycle_count decimal,
	pack_available_energy decimal,
	pack_consumed_energy decimal,
	pack_fault int,
	pack_status int
);
SELECT create_hypertable('bms', 'timestamp');	
