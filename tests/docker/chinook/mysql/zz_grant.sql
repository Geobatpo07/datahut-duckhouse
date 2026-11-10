-- Runs after Chinook_MySql.sql (initdb.d scripts execute in filename order).
-- MYSQL_USER/MYSQL_PASSWORD only grants privileges on MYSQL_DATABASE, which
-- this stack doesn't set (Chinook_MySql.sql creates its own `Chinook` db) --
-- grant the dlt-facing `chinook` user access to it explicitly.
GRANT ALL PRIVILEGES ON `Chinook`.* TO 'chinook'@'%';
FLUSH PRIVILEGES;
