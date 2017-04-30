*****  Generate Indicative file for wsj active ******;

proc sql;
create table indicative_file_active as
select Acct_No	,
Zipcode	,
Sub_Start_Date	,
Renewal_Date	,
Expire_Date	,
Stop_Date	,
cumm_sub_amt	,
subscription_Type	,
AR_flag	,
_ORI_TENURE_ys	,
price	,
Payment_Type	,
Frequency_Desc	,
new_print_stat2	,
Cust_Status3	,
UU_ID	,
source	,
WSJPLUSsub_Type	,
marketing_program	,
registration	,
FILENAME2	,
cur_Src_grp3	,
SBSCR_STATE_CODE	,
_SBSCR_ID	,
todaydate	,
date3	,
_CUR_TENURE_yrs	,
_CUR_TENURE_mths,
offer_type     /*** 04/06/17 : added new field per Sharon's request**/
from keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.;

quit;


proc export data=indicative_file_active 
            outfile="&root./&user./&env./&project./raw-data/indicative_active_file_&date_mmddyy..csv"
			dbms=csv replace;
run;


***** check the record counts ******;

x "wc -l &root./&user./&env./&project./raw-data/indicative_active_file_&date_mmddyy..csv|awk -F ' ' '{print $1}' > &root./&user./&env./&project./raw-data/act_rec_cnt.txt";

data act_rec_cnt;
infile "&root./&user./&env./&project./raw-data/act_rec_cnt.txt" ;
input rec_cnt :$50.;
rec_cnt_n =rec_cnt+0;
run;


%macro run_freqcnt(filename);

 /*** create table to collect the collect for 4 main transformed variables ****/
 
Proc sql;
 
create table &filename._counts as
select 'Subscription_Type' as name, subscription_type as value, count(*) as cnt,
&date_run. format =mmddyy10. as asofdate from indicative_file_&filename.
group by  subscription_type
union

select 'AR Flag' as name, ar_flag as value, count(*) as cnt,
&date_run. format =mmddyy10. as asofdate from indicative_file_&filename.
group by  ar_flag
union
select 'New_Print_stat2' as name, New_Print_stat2 as value, count(*) as cnt,
&date_run. format =mmddyy10. as asofdate from indicative_file_&filename.
group by  New_Print_stat2
union
select 'filename2' as name, filename2 as value, count(*) as cnt,
&date_run. format =mmddyy10. as asofdate from indicative_file_&filename.
group by  filename2;

quit;

/***clean entries from previous run if running again for same date ***/
proc sql;
delete from SLOW_CHG.wjs_&filename._counts_hist
where asofdate in (select distinct asofdate from &filename._counts);
quit;

/****create table to get counts from last run ****/
proc sql;
Create table &filename._last_run as
select * from SLOW_CHG.wjs_&filename._counts_hist 
where asofdate =(select max(asofdate) as asofdate from SLOW_CHG.wjs_&filename._counts_hist );

quit;


proc append base=SLOW_CHG.wjs_&filename._counts_hist data=&filename._counts  force;
run;


/****calculate percent change in counts between last and current run ****/

proc sql;

create table count_diff_from_last_run as
select a.name,a.value, (a.cnt-b.cnt) as diff,
((b.cnt-a.cnt)/b.cnt) format =percent10.2 as per_chng_from_lastrun,
case when (((b.cnt-a.cnt)/b.cnt) *100)>5 then 'Count increased >5%,review the data'
     when (((b.cnt-a.cnt)/b.cnt) *100)<-5 then 'Count dropped >5%,review the data'
else 'Good' end as qc_status 
from &filename._last_run  a
left outer join &filename._counts b
on a.name =b.name
and a.value =b.value;

Quit;


proc sort data=SLOW_CHG.wjs_&filename._counts_hist;
by name  value asofdate;
run;


proc transpose data= SLOW_CHG.wjs_&filename._counts_hist (where=(asofdate >= today()-10)) out=&filename._out prefix=cnt;
by name  value;
id asofdate;
var cnt;
run;

/**** Append count change from last run and create report****/

proc sql;
Create table  &filename._report as
select a.*, b.per_chng_from_lastrun,b.qc_status
from &filename._out a
left outer join count_diff_from_last_run b
on a.name =b.name
and a.value =b.value;

options orientation=landscape;
ods pdf body ="&root./&user./&env./&project./report/indicative_&filename._counts_&today_yyyymmdd..pdf";	
	proc print data=&filename._report (drop= _NAME_) noobs;
			title "INDICATIVE &filename. Counts from Last 5 runs";
	run;

	title '';
ods pdf close;


%mend  run_freqcnt;

***%run_freqcnt(Active);

***%run_freqcnt(fmr);

proc sql;
select max(rec_cnt_n) into :cnt from act_rec_cnt;

quit;

%macro send_mail_act;

%if &cnt.<=1 %then %do;
%put &cnt.;


filename mymail email 
      to = &to_email.
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alert! NO DATA in INDICATIVE Active file for asofdate &today_MMDDYY10."
			replyto=&reply_to_email; 
			
		data _null_;
			file mymail;
		
		 put "INDICATIVE Active File - indicative_active_file_&date_mmddyy..csv does not have any data today. Please check the log for any issues.";
		 put /"Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";
         
		run; 	
%end;

%else %do;
 
%run_freqcnt(active);

filename mymail email 
      to = &to_email.
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc =  &cc_email.
			subject="SASCDM &env_email. Alerts: SUCCESS -INDICATIVE Active file for asofdate &today_MMDDYY10. has been created"
			replyto=&reply_to_email
            attach="&root./&user./&env./&project./report/indicative_active_counts_&today_yyyymmdd..pdf";
			
		data _null_;
			file mymail;

		 put "INDICATIVE Active File - indicative_active_file_&date_mmddyy..csv has been created. It has &cnt. records. Please review attached report for QC";
		 put /"Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";

       run; 	
%end ;  

%mend send_mail_act;
%send_mail_act;


*****  Generate Indicative file for wsj former ******;

proc sql;
create table indicative_file_fmr as
select Acct_No	,
Zipcode	,
Sub_Start_Date	,
Renewal_Date	,
Expire_Date	,
Stop_Date	,
cumm_sub_amt	,
subscription_Type	,
AR_flag	,
NUMMTH as _ORI_TENURE_ys	,
price	,
Payment_Type	,
Frequency_Desc	,
new_print_stat2	,
Cust_Status3	,
UU_ID	,
source	,
WSJPLUSsub_Type	,
marketing_program	,
registration	,
FILENAME2	,
cur_Src_grp3	,
SBSCR_STATE_CODE	,
_SBSCR_ID	,
&today_yyyymmdd as todaydate	,
&today_date as date3 format=date9.	,
 _CUR_TENURE_yrs	,
 _CUR_TENURE_mths,
 offer_type /*** 04/06/17 : added new field per Sharon's request**/

from fmr.WSJIP_FORMER_&date_mmddyy._Custview_ind;;
quit;



proc export data=indicative_file_fmr 
            outfile="&root./&user./&env./&project./raw-data/indicative_formers_file_&date_mmddyy..csv"
			dbms=csv replace;
run;



***** check the record counts ******;

x "wc -l &root./&user./&env./&project./raw-data/indicative_formers_file_&date_mmddyy..csv|awk -F ' ' '{print $1}' > &root./&user./&env./&project./raw-data/fmr_rec_cnt.txt";

data fmr_rec_cnt;
infile "&root./&user./&env./&project./raw-data/fmr_rec_cnt.txt" ;
input rec_cnt :$50.;
rec_cnt_n =rec_cnt+0;
run;
proc sql;
select max(rec_cnt_n) into :fmr_cnt from fmr_rec_cnt;

quit;

%macro send_mail_fmr;

%if &fmr_cnt.<=1 %then %do;
%put &fmr_cnt.;

filename mymail email 
      to = &to_email.
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alert! NO DATA in INDICATIVE Formers file for asofdate &today_MMDDYY10."
			replyto=&reply_to_email;
			
		data _null_;
			file mymail;
		
		 put "INDICATIVE Formers File- indicative_formers_file_&date_mmddyy..csv does not have any data today. Please check the log for any issues";
		 put /"Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";
         
		run; 	
%end;

%else %do;

%run_freqcnt(fmr);

filename mymail email 
      to = &to_email.
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alerts: SUCCESS -INDICATIVE Formers File for asofdate &today_MMDDYY10. has been created"
			replyto=&reply_to_email
            attach="&root./&user./&env./&project./report/indicative_fmr_counts_&today_yyyymmdd..pdf"; 
			
		data _null_;
			file mymail;

		 put "INDICATIVE Formers File - indicative_formers_file_&date_mmddyy..csv has been created. It has &fmr_cnt. records. Please review attached report for QC." ; 
		 put /"Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";

       run;  
%end ;  

%mend send_mail_fmr;
%send_mail_fmr;


*******************************************************************************************;
