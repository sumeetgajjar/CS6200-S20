CREATE DATABASE IF NOT EXISTS CS6200;
CREATE USER IF NOT EXISTS 'cs6200'@'localhost' IDENTIFIED BY 'cs6200';
GRANT ALL PRIVILEGES ON cs6200.* TO 'cs6200'@'localhost';

drop table if exists cs6200.url_ids;
create table if not exists cs6200.url_ids
(
    id        int primary key auto_increment,
    url       nvarchar(1000) not null,
    created   timestamp default CURRENT_TIMESTAMP,
    updated   timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    is_active tinyint   default 1,
    index (url)
);

DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_insert_url` $$
CREATE
    PROCEDURE `sp_insert_url`(IN url nvarchar(1000))
BEGIN
    /*
    call sp_insert_url('testing_sumeet.com');
    */
    insert ignore into cs6200.url_ids(url)
    select url
    where not exists(select 1 from cs6200.url_ids as a where a.url = url);

    select LAST_INSERT_ID() as url_id;
END $$
DELIMITER ;

call sp_insert_url('testing_sumeet.com');
select *
from cs6200.url_ids;


# drop table if exists cs6200.crawled_urls;
create table if not exists cs6200.crawled_urls
(
    id                int primary key auto_increment,
    canonical_url_ids int not null,
    created           timestamp default CURRENT_TIMESTAMP,
    updated           timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    is_active         tinyint   default 1,
    index (canonical_url_ids)
);

