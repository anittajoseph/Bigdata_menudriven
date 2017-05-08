
--2011
a = load '/user/hive/warehouse/niit1.db/h1b_final' using PigStorage() as(sno:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray,full_time_position:chararray,prevailining_wage:double, year:chararray, worksite:chararray, longitude:int, latitude:int);
b = filter a by year=='2011';
c = foreach b generate $4, $5, $6, $7;
d = group c by ($0, $1);
e = foreach d generate group as job_title, COUNT(c),SUM(c.prevailining_wage);
f = foreach e generate $0, ($2/$1)as avg;
g = order f by $0 desc;
h = limit g 10;
--dump h;
Store h into '/anitta/query8/year11' using PigStorage();
--2012
b1 = filter a by year=='2012';
c1 = foreach b1 generate $4, $5, $6, $7;
d1 = group c1 by ($0, $1);
e1 = foreach d1 generate group as job_title, COUNT(c1),SUM(c1.prevailining_wage);
f1 = foreach e1 generate $0, ($2/$1)as avg;
g1 = order f1 by $0 desc;
h1 = limit g1 10;
Store h into '/anitta/query8/year12' using PigStorage();

--2013
b2 = filter a by year=='2013';
c2 = foreach b2 generate $4, $5, $6, $7;
d2 = group c2 by ($0, $1);
e2 = foreach d2 generate group as job_title, COUNT(c2),SUM(c2.prevailining_wage);
f2 = foreach e2 generate $0, ($2/$1)as avg;
g2 = order f2 by $0 desc;
h2 = limit g2 10;
Store h into '/anitta/query8/year13' using PigStorage();

--2014

b3 = filter a by year=='2014';
c3 = foreach b3 generate $4, $5, $6, $7;
d3 = group c3 by ($0, $1);
e3 = foreach d3 generate group as job_title, COUNT(c3),SUM(c3.prevailining_wage);
f3 = foreach e3 generate $0, ($2/$1)as avg;
g3 = order f3 by $0 desc;
h3 = limit g3 10;
Store h into '/anitta/query8/year14' using PigStorage();


--2015
b4 = filter a by year=='2015';
c4 = foreach b4 generate $4, $5, $6, $7;
d4 = group c4 by ($0, $1);
e4 = foreach d4 generate group as job_title, COUNT(c4),SUM(c4.prevailining_wage);
f4 = foreach e4 generate $0, ($2/$1)as avg;
g4 = order f4 by $0 desc;
h4 = limit g4 10;
Store h into '/anitta/query8/year15' using PigStorage();

--2016
b5 = filter a by year=='2016';
c5 = foreach b5 generate $4, $5, $6, $7;
d5 = group c5 by ($0, $1);
e5 = foreach d5 generate group as job_title, COUNT(c5),SUM(c5.prevailining_wage);
f5 = foreach e5 generate $0, ($2/$1)as avg;
g5 = order f5 by $0 desc;
h5 = limit g5 10;
Store h into '/anitta/query8/year16' using PigStorage();




