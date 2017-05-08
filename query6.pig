--certified
h1b = load '/user/hive/warehouse/niit1.db/h1b_final' using PigStorage() as(sno:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray,full_time_position:chararray,prevailining_wage:double, year:chararray, worksite:chararray, longitude:int, latitude:int);

new = foreach h1b generate $7, $1;

groupbyyear = group new by year;

newgroup = foreach groupbyyear generate group as year, COUNT(new)as total;

filterbycase= filter new by case_status=='CERTIFIED';

filteryear= group filterbycase by year;

certified = foreach filteryear generate group as year, COUNT(filterbycase) as certified;

joindata = join newgroup by $0,certified by $0;

 data = foreach joindata generate $0,$1,$3,((double)$3*100/(double)$1) as percent;

Store data into '/anitta/query6/certified' using PigStorage();

--certified-with
newgroup1= foreach groupbyyear generate group as year, COUNT(new)as total;

filterbycase1= filter new by case_status=='CERTIFIED-WITHDRAWN';

filteryear1= group filterbycase1 by year;

certified1 = foreach filteryear1 generate group as year, COUNT(filterbycase1) as certified;

joindata1 = join newgroup1 by $0,certified1 by $0;

 data1 = foreach joindata1 generate $0,$1,$3,((double)$3*100/(double)$1) as percent;

Store data1 into '/anitta/query6/certified-with' using PigStorage();



--withdrawn
newgroup2 = foreach groupbyyear generate group as year, COUNT(new)as with;

filterbycase2= filter new by case_status=='WITHDRAWN';

filteryear2= group filterbycase2 by year;

certified2 = foreach filteryear2 generate group as year, COUNT(filterbycase2) as certified;

joindata2 = join newgroup2 by $0,certified by $0;

data2 = foreach joindata2 generate $0,$1,$3,((double)$3*100/(double)$1) as percent;

Store data1 into '/anitta/query6/withd' using PigStorage();

--Denied

newgroup3 = foreach groupbyyear generate group as year, COUNT(new)as total;

filterbycase3= filter new by case_status=='DENIED';

filteryear3= group filterbycase3 by year;

denied3= foreach filteryear3 generate group as year, COUNT(filterbycase3) as certified;

joindata3 = join newgroup3 by $0,certified by $0;

data3 = foreach joindata3 generate $0,$1,$3,((double)$3*100/(double)$1) as percent;

Store data1 into '/anitta/query6/denied' using PigStorage();





