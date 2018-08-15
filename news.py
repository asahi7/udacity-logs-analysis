#!/usr/bin/env python3

import psycopg2

conn = psycopg2.connect("dbname=news")

cur = conn.cursor()

query1 = """
select articles.title, count(articles.title) from articles join log on
log.path = ('/article/' || articles.slug) group by articles.title
order by count(articles.title) desc limit 3
"""

query2 = """
select authors.name, sum(table2.cnt) as sm from (select articles.author,
articles.title, count(articles.title) as cnt from articles join log
on log.path = ('/article/' || articles.slug) group by articles.title,
articles.author order by count(articles.title) desc) as table2 join authors on
authors.id = table2.author group by authors.id order by sm desc
"""

query3 = """
select to_char(bad.day, 'Month DD, YYYY'), round(cast((cast(bad.cnt as decimal)
* 100 / rall.cnt) as numeric), 2) as percent from (select
date_trunc('day', time) as day, count(status) as cnt from log where
status != '200 OK' group by 1) as bad, (select date_trunc('day', time) as day,
count(status) as cnt from log group by 1) as rall where bad.day = rall.day
and (cast(bad.cnt as decimal) * 100 / rall.cnt) > 1
"""

cur.execute(query1)
results = cur.fetchall()
for result in results:
    print(result[0] + " — " + str(result[1]) + " views")
print("-----")

cur.execute(query2)
results = cur.fetchall()
for result in results:
    print(result[0] + " — " + str(result[1]) + " views")
print("-----")

cur.execute(query3)
results = cur.fetchall()
for result in results:
    print(result[0] + " — " + str(result[1]) + "% errors")
