-- 
-- Update and run this script before kicking things off
--

create schema cosmos;

create user cosmos identified by '<!CHANGE_ME!>';

grant all privileges on cosmos.* to 'cosmos'@'%'; 

grant all privileges on *.* to 'cosmos'@'%'; 

flush privileges;




