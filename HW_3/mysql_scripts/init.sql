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
    index (url),
    unique (url, is_active)
);

DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_insert_url` $$
CREATE
    PROCEDURE `sp_insert_url`(IN url nvarchar(1000))
BEGIN
    /*
    call sp_insert_url(@url:='testing_sumeet.com');
    */
    insert into cs6200.url_ids(url)
    select url
    where not exists(select 1 from cs6200.url_ids as a where a.url = url);

    select LAST_INSERT_ID() as url_id;
END $$
DELIMITER ;

call sp_insert_url(@url := 'testing_sumeet.com');;
select *
from cs6200.url_ids;


drop table if exists cs6200.crawled_urls;
create table if not exists cs6200.crawled_urls
(
    id        int primary key auto_increment,
    url_id    int not null,
    created   timestamp default CURRENT_TIMESTAMP,
    updated   timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    is_active tinyint   default 1,
    index (url_id),
    unique (url_id, is_active)
);

DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_insert_crawled_url` $$
CREATE
    PROCEDURE `sp_insert_crawled_url`(IN url_id int)
BEGIN
    /*
    call sp_insert_crawled_url(@url_id:=1);
    */
    insert into cs6200.crawled_urls(url_id)
    values (url_id);

    select LAST_INSERT_ID() as id;
END $$
DELIMITER ;

call sp_insert_crawled_url(@url_id := 1);
call sp_insert_crawled_url(@url_id := 2);
call sp_insert_crawled_url(@url_id := 3);
select *
from cs6200.crawled_urls;

DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_get_crawled_urls` $$
CREATE
    PROCEDURE `sp_get_crawled_urls`(IN url_id int)
BEGIN
    /*
    call sp_get_crawled_urls(@url_id:=1);
    */
    select a.url_id
    from crawled_urls as a
    where a.url_id = url_id;
END $$
DELIMITER ;

call sp_get_crawled_urls(@url_id := 1);
call sp_get_crawled_urls(@url_id := 2);
call sp_get_crawled_urls(@url_id := 3);
