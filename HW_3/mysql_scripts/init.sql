CREATE DATABASE IF NOT EXISTS CS6200;
CREATE USER IF NOT EXISTS 'cs6200'@'localhost' IDENTIFIED BY 'cs6200';
GRANT ALL PRIVILEGES ON cs6200.* TO 'cs6200'@'localhost';

drop table if exists cs6200.link_graph_edges;
create table if not exists cs6200.link_graph_edges
(
    id        int primary key auto_increment,
    src       nvarchar(1000) not null,
    src_hash  varchar(40)    not null,
    dest      nvarchar(1000) not null,
    dest_hash varchar(40)    not null,
    created   timestamp default CURRENT_TIMESTAMP,
    updated   timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    is_active tinyint   default 1,
    index (src_hash, dest_hash)
);

DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_insert_link_graph_edges` $$
CREATE
    PROCEDURE `sp_insert_link_graph_edges`(IN var_edges_xml nvarchar(1000))
BEGIN
    /*
    call sp_insert_link_graph_edges(@var_edges_xml:='
    <rt>
        <r>
            <s><![CDATA[src-1]]></s>
            <d><![CDATA[des-1]]></d>
        </r>
        <r>
            <s><![CDATA[src-1]]></s>
            <d><![CDATA[des-2]]></d>
        </r>
        <r>
            <s><![CDATA[des-2]]></s>
            <d><![CDATA[des-3]]></d>
        </r>
    </rt>
    ');
    */
    #TODO write code to parse xml

    create temporary table TMP
    (
        src       nvarchar(1000) not null,
        src_hash  varchar(40)    not null,
        dest      nvarchar(1000) not null,
        dest_hash varchar(40)    not null
    );

    insert into cs6200.link_graph_edges(src, src_hash, dest, dest_hash)
    select src, src_hash, dest, dest_hash
    from TMP
    where not exists(select 1 from link_graph_edges as a where a.src_hash = src_hash and a.dest_hash = dest_hash);
END $$
DELIMITER ;

call sp_insert_link_graph_edges(@url := '1.testing_sumeet.comasd');
select *
from cs6200.url_ids;


#####################################################################
#####################################################################
#######################                  ############################
#######################     Not Used     ############################
#######################                  ############################
#####################################################################
#####################################################################

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

    select a.id as url_id
    from cs6200.url_ids as a
    where a.url = url;
END $$
DELIMITER ;

call sp_insert_url(@url := '1.testing_sumeet.comasd');
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
