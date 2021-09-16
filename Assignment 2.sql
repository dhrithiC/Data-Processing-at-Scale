--Query1
DROP TABLE IF EXISTS query1 CASCADE;
Create table query1 as 
Select g.name, count(m.movieid) as moviecount from genres as g, movies as m, hasagenre as h where 
h.movieid = m.movieid and g.genreid = h.genreid group by g.name;


--Query2
DROP TABLE IF EXISTS query2 CASCADE;
Create table query2 as 
Select g.name, avg(r.rating) as rating from genres as g, hasagenre as h, ratings as r where
g.genreid = h.genreid and h.movieid=r.movieid group by(g.name);


--Query3
DROP TABLE IF EXISTS query3 CASCADE;
Create table query3 as 
Select m.title, count(r.rating) as countofratings from movies as m, ratings as r where m.movieid = r.movieid 
group by m.title having count(r.rating) >= 10 order by m.title;

--Query4
DROP TABLE IF EXISTS query4 CASCADE;
Create table query4 as
Select m.movieid, m.title from movies as m, hasagenre as h, genres as g where
m.movieid = h.movieid and h.genreid=g.genreid and g.name='Comedy';


--Query5
DROP TABLE IF EXISTS query5 CASCADE;
Create table query5 as
Select m.title, avg(r.rating) as average from movies as m, ratings as r where m.movieid = r.movieid 
group by m.title order by m.title;


--Query6
DROP TABLE IF EXISTS query6 CASCADE;
Create table query6 as
select avg(r.rating) as average from ratings as r, hasagenre as h, genres as g where 
h.genreid = g.genreid and h.movieid=r.movieid and g.name = 'Comedy';


--Query7
DROP TABLE IF EXISTS query7 CASCADE;
Create table query7 as
Select avg(r.rating) as average from ratings as r, genres as g1, genres as g2, hasagenre as h1, hasagenre as h2,
movies as m1, movies as m2 where r.movieid = m1.movieid and r.movieid = m2.movieid and m1.movieid = h1.movieid
and m2.movieid = h2.movieid and h1.genreid = g1.genreid and h2.genreid = g2.genreid and 
g1.name = 'Comedy' and g2.name='Romance';


--Query8
DROP TABLE IF EXISTS query8 CASCADE;
Create table query8 as
SELECT AVG(r.rating) AS average 
FROM ratings as r WHERE r.movieid IN 
((SELECT h.movieid FROM genres as g, hasagenre as h   
  WHERE g.genreid = h.genreid AND g.name = 'Romance') 
 EXCEPT 
 (SELECT h.movieid FROM genres as g, hasagenre as h   
  WHERE g.genreid = h.genreid AND g.name = 'Comedy'));
  

--Query9  
DROP TABLE IF EXISTS query9 CASCADE;
Create table query9 as
select r.movieid, r.rating from ratings as r  where r.userid = :v1;




--Query 10 Recommendation  table part 1 to find the average rating
DROP TABLE IF EXISTS averagetable CASCADE;
CREATE TABLE averagetable AS 
SELECT movieid, AVG(rating) AS average
FROM ratings
GROUP BY movieid;

--The similarity table
DROP TABLE IF EXISTS similarity CASCADE; 
CREATE TABLE similarity as (
SELECT i.movieid as movieid1, l.movieid as movieid2, ( 1 - ( ABS ( i.average - l.average)) / 5) as sim, q.rating, m.title
FROM averagetable i, averagetable l,  movies m, query9 q
WHERE i.movieid NOT IN (SELECT movieid FROM query9) 
AND l.movieid IN (SELECT movieid FROM query9)
AND l.movieid = q.movieid 
AND i.movieid = m.movieid);

--The final Recommendation table for the movie title
DROP TABLE IF EXISTS recommendation CASCADE;
CREATE TABLE recommendation as (
SELECT title FROM similarity GROUP BY title
HAVING (SUM (sim * rating) /SUM(sim)) > 3.9);


 