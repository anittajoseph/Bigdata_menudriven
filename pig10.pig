h1b = load '/user/hive/warehouse/niit1.db/h1b_final' using PigStorage() as(sno:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray,full_time_position:chararray,prevailining_wage:double, year:chararray, worksite:chararray, longitude:int, latitude:int);

new = foreach h1b generate $4,$1;
filterbycertified = filter new by case_status=='CERTIFIED';
filterbywithdrawn = filter new by case_status=='CERTIFIED-WITHDRAWN';

newgroup = group new by job_title;
groupcertified = group filterbycertified by job_title;
groupwithdrawn = group filterbywithdrawn by job_title;

petitions = foreach newgroup generate group as job_position,COUNT(new) as cnt;

certifiedpetition = foreach groupcertified generate group as job_Position,COUNT(filterbycertified ) as cnt;
withdrawnpetition = foreach groupwithdrawn generate group as job_Position,COUNT(filterbywithdrawn) as cnt;

joindata = join petitions by $0, certifiedpetition by $0, withdrawnpetition by $0;

value1 = foreach joindata generate $0,$1,($3+$5);

success = foreach value1 generate $0,$1,((double)$2*100/(double)$1) as success;

filterbycondition = filter success by $1>=10000 and $2>70.0;

orderdata = order filterbycondition by $2 desc;

result =limit orderdata 10;

--dump result;

Store result into '/anitta/query10' using PigStorage();
