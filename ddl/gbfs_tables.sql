CREATE TABLE free_bike_status (
  accessed_on TIMESTAMP(0),
  baseurl TEXT ENCODING DICT(32),
  bike_id TEXT ENCODING DICT(32),
  is_disabled BOOLEAN,
  is_reserved BOOLEAN,
  jump_ebike_battery_level INTEGER,
  lat DOUBLE,
  lon DOUBLE,
  name TEXT ENCODING DICT(32),
  vehicle_type TEXT ENCODING DICT(8))
WITH (MAX_ROWS = 1000000000)

CREATE TABLE station_status (
  accessed_on TIMESTAMP(0),
  baseurl TEXT ENCODING DICT(32),
  eightd_action_station_services_id TEXT ENCODING DICT(32),
  eightd_has_available_keys BOOLEAN,
  is_charging_station BOOLEAN,
  is_installed BOOLEAN,
  is_renting BOOLEAN,
  is_returning BOOLEAN,
  last_reported TIMESTAMP(0),
  num_bikes_available INTEGER,
  num_bikes_available_types_classic INTEGER,
  num_bikes_available_types_ebike INTEGER,
  num_bikes_available_types_electric INTEGER,
  num_bikes_available_types_mechanical INTEGER,
  num_bikes_available_types_smart INTEGER,
  num_bikes_disabled INTEGER,
  num_docks_available INTEGER,
  num_docks_disabled INTEGER,
  num_ebikes_available INTEGER,
  station_id TEXT ENCODING DICT(32))
WITH (MAX_ROWS = 1000000000)
