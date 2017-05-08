#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************APP MENU***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1)${MENU} 1.a IS the no of petitions for data engineer job incerasing over time ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2)${MENU} 1.b Find top 5 job titles who are having highest growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} 2.a Which part of the US has the most Data Engineer jobs for each year?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} 2.b find top 5 locations in the US who have got certified visa for each year.  ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU} 3.Which industry has the most number of Data Scientist positions? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6)${MENU} 4.Which top 5 employers file the most petitions each year?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7)${MENU} 5.Find the most popular top 10 job positions for H1B visa applications for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8)${MENU} 6.Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9)${MENU} 7.Create a bar graph to depict the number of applications for each year ?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10)${MENU} 8.Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) ?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11${MENU}  9.Which are the top 10 employer which have the highest success rate more than 70%  in petitions and file petitions more than 10000 ?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12)${MENU} 10.Which are the top 10 job positions which have the highest success rate more than 70% in petitions and file petitions more than 10000?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13)${MENU} 11.Export result for question no 10 to MySql database.${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

function output_directory() 
{
    COLOR='\033[01;31m' # bold green
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
        option_picked "1-a Is the number of petitions with Data Engineer job title increasing over time?";
        hadoop jar /home/cloudera/DataEngineer.jar /user/hive/warehouse/niit1.db/h1b_final/ /anitta/opp1a
	output_directory "OUTPUT  IS SUCCESSFULLY STORED INTO /anitta/opp1a";
        show_menu;
        ;;

        2) clear;
        option_picked " 1-b Find top 5 job titles who are having highest growth in applications. ";
        hadoop jar /home/cloudera/JobTitles.jar /user/hive/warehouse/niit1.db/h1b_final/ /anitta/op1b
	output_directory "OUTPUT  IS SUCCESSFULLY STORED INTO  /presentation/output1b";
        show_menu;
        ;;
            
        3) clear;
        option_picked " 2-a Which part of the US has the most Data Engineer jobs for each year?";
        hadoop jar /home/cloudera/DataEngineerUS.jar /user/hive/warehouse/niit1.db/h1b_final/ /anitta/op2a
	output_directory "OUTPUT   IS SUCCESSFULLY STORED INTO  /anitta/op2a";
        show_menu;
        ;;
	
        4) clear;
        option_picked " 2-b find top 5 locations in the US who have got certified visa for each year.";
       hadoop jar /home/cloudera/TopLocation.jar /user/hive/warehouse/niit1.db/h1b_final/ /anitta/op2b
	output_directory "OUTPUT FOR  IS SUCCESSFULLY STORED INTO  /anitta/op2b";
        show_menu;
        ;;
            
	 5) clear;
        option_picked "3-Which industry has the most number of Data Scientist positions?";
     
	hive -e "select soc_name, COUNT(job_title) as count  from niit1.h1b_final where job_title like '%DATA SCIENTIST%' group by soc_name order by count desc limit 10;"
	output_directory "OUTPUT IS: ";
        show_menu;
                    ;;
          6) clear;
        option_picked " 4-find top 5 locations in the US who have got certified visa for each year.";
     	hive -e "select year, employer_name, cnt ,rank from(select year, employer_name, rank() over (partition by year order by cnt desc) as rank,cnt from niit1.topemp) ranked_table where ranked_table.rank <=5;"
	
	output_directory "OUTPUT IS: ";
        show_menu;
                    ;;           
      7) clear;
        option_picked " 5-Find the most popular top 10 job positions for H1B visa applications for each year?";
     hive -e "select year,  job_title, cnt ,rank from(select year, job_title, rank() over (partition by year order by cnt desc) as rank,cnt from niit1.topjob) ranked_table where ranked_table.rank <=10;"

	output_directory "OUTPUT IS: ";
        show_menu;
                    ;; 
	8) clear;
        option_picked " 6-Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time";
     pig -f query6.pig
	
	output_directory "OUTPUT IS STORED IN /anitta/query6/";
        show_menu;
                    ;; 

	                        
      9) clear;
        option_picked " 7-Create a bar graph to depict the number of applications for each year ??";
     	hive -e "select  year, COUNT(case_status) from niit1.h1b_final  group by year order by year desc;"
	output_directory "OUTPUT IS: ";
        show_menu;
                    ;; 

	                       
      10) clear;
        option_picked " 8-Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) ?";
     	 pig -f query8.pig
	
	output_directory "OUTPUT IS INTO /anitta/query8/";
        show_menu;
                    ;; 

	                        
      11) clear;
        option_picked " 9-Which are the top 10 employer which have the highest success rate more than 70%  in petitions and file petitions more than 10000 ?";
     	 pig -f pig9.pig
	
	output_directory "OUTPUT IS OUTPUT IS INTO /anitta/query9: ";
        show_menu;
                    ;; 
                        
      12) clear;
        option_picked " 10-Which are the top 10 job positions which have the highest success rate more than 70% in petitions and file petitions more than 10000?";
      pig -f pig10.pig
	
	output_directory "OUTPUT IS OUTPUT IS INTO /anitta/query10: ";
        show_menu;
                    ;; 
	
	13) clear;
        option_picked "11-Export result for question nnumber10 to MySql database.";
        sqoop export -- connect jdbc:mysql://localhost/h1b_app -- username root -- password cloudera --table answer10 -- export-dir /success_rate2.txt -m 1;

        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done


