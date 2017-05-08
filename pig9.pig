h1b = load '/user/hive/warehouse/niit1.db/h1b_final' using PigStorage() as(sno:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray,full_time_position:chararray,prevailining_wage:double, year:chararray, worksite:chararray, longitude:int, latitude:int);


new = foreach h1b generate employer_name,case_status;
filterbywithdrawn = filter new by case_status=='CERTIFIED-WITHDRAWN';
filterbycertified = filter new by case_status=='CERTIFIED';

newgroup = group new by employer_name;

groupcertified = group filterbycertified by employer_name;
groupwithdrawn = group filterbywithdrawn by employer_name;

petitions = foreach newgroup generate group as employer,COUNT(new) as cnt;
certifiedpetition= foreach groupcertified generate group as employer,COUNT(filterbycertified) as cnt;
withdrawnpetition = foreach groupwithdrawn generate group as employer,COUNT(filterbywithdrawn) as cnt;

joindata = join petitions by $0, certifiedpetition by $0, withdrawnpetition by $0;

value1 = foreach joindata generate $0,$1,($3+$5);

success = foreach value1 generate $0,$1,((double)$2*100/(double)$1) as success;

filterbycondition = filter success by $1>=10000 and $2>70.0;

orderdata = order filterbycondition by $2 desc;

final =limit orderdata 10;

--dump final;
Store final into '/anitta/query9' using PigStorage();
