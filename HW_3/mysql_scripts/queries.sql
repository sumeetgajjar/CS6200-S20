select c.inlinks, b.url
from cs6200.crawled_urls as b
         inner join (select a.dest_hash, count(a.src_hash) as inlinks
                     from cs6200.link_graph_edges as a
                     where a.is_active = 1
                     group by a.dest_hash) as c
                    on b.url_hash = c.dest_hash
order by c.inlinks desc;