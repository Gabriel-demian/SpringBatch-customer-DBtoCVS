CREATE USER 'gabriel'@'localhost' IDENTIFIED BY 'springstudent';

GRANT ALL PRIVILEGES ON * . * TO 'gabriel'@'localhost';

ALTER USER 'gabriel'@'localhost' IDENTIFIED WITH mysql_native_password BY 'gabriel';