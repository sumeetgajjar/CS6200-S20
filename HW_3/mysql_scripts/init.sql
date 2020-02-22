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
    declare v_row_index int unsigned default 0;
    declare v_row_count int unsigned;
    declare v_xpath_row varchar(255);
    declare v_src nvarchar(1000);
    declare v_dest nvarchar(1000);

    drop table if exists tmp;
    create temporary table tmp
    (
        src       nvarchar(1000) not null,
        src_hash  varchar(40)    not null,
        dest      nvarchar(1000) not null,
        dest_hash varchar(40)    not null
    ) ENGINE = MEMORY;

    set v_row_count = extractValue(var_edges_xml, concat('count(/rt/r)'));

    while v_row_index < v_row_count
        do
            set v_row_index = v_row_index + 1;
            set v_xpath_row = concat('/rt/r[', v_row_index, ']');
            set v_src = extractValue(var_edges_xml, concat(v_xpath_row, '/s'));
            set v_dest = extractValue(var_edges_xml, concat(v_xpath_row, '/d'));
            insert into tmp (src, src_hash, dest, dest_hash)
            values (v_src, MD5(v_src), v_dest, MD5(v_dest));
        end while;

    insert into cs6200.link_graph_edges(src, src_hash, dest, dest_hash)
    select src, src_hash, dest, dest_hash
    from tmp a
    where not exists(select 1
                     from cs6200.link_graph_edges as b
                     where b.src_hash = a.src_hash
                       and b.dest_hash = a.dest_hash);

END $$
DELIMITER ;

call sp_insert_link_graph_edges(@var_edges_xml := '
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
            <d><![CDATA[des-1]]></d>
        </r>
    </rt>
    ');
select *
from cs6200.link_graph_edges;


drop table if exists cs6200.crawled_urls;
create table if not exists cs6200.crawled_urls
(
    id        int primary key auto_increment,
    url       nvarchar(1000) not null,
    url_hash  varchar(40)    not null,
    created   timestamp default CURRENT_TIMESTAMP,
    updated   timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    is_active tinyint   default 1,
    index (url_hash)
);

DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_insert_crawled_urls` $$
CREATE
    PROCEDURE `sp_insert_crawled_urls`(IN var_urls_xml nvarchar(1000))
BEGIN
    /*
    call sp_insert_crawled_urls(@var_urls_xml:='
    <rt>
        <r>
            <s><![CDATA[url-1]]></s>
        </r>
        <r>
            <s><![CDATA[url-2]]></s>
        </r>
        <r>
            <s><![CDATA[url-3]]></s>
        </r>
    </rt>
    ');
    */
    declare v_row_index int unsigned default 0;
    declare v_row_count int unsigned;
    declare v_xpath_row varchar(255);
    declare v_url nvarchar(1000);

    drop table if exists tmp;
    create temporary table tmp
    (
        url      nvarchar(1000) not null,
        url_hash varchar(40)    not null
    ) ENGINE = MEMORY;

    set v_row_count = extractValue(var_urls_xml, concat('count(/rt/r)'));

    while v_row_index < v_row_count
        do
            set v_row_index = v_row_index + 1;
            set v_xpath_row = concat('/rt/r[', v_row_index, ']');
            set v_url = extractValue(var_urls_xml, concat(v_xpath_row, '/u'));
            insert into tmp (url, url_hash)
            values (v_url, MD5(v_url));
        end while;

    insert into cs6200.crawled_urls(url, url_hash)
    select url, url_hash
    from tmp a
    where not exists(select 1 from cs6200.crawled_urls as b where b.url_hash = a.url_hash);

END $$
DELIMITER ;

call sp_insert_crawled_urls(@var_urls_xml := '
    <rt>
        <r>
            <u><![CDATA[url-1]]></u>
        </r>
        <r>
            <u><![CDATA[url-2]]></u>
        </r>
        <r>
            <u><![CDATA[url-3]]></u>
        </r>
    </rt>
    ');
select *
from cs6200.crawled_urls;


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