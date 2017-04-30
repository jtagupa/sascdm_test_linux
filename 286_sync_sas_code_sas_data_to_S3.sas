


x "aws s3 sync  /&root./&user./&env./&project./sas-code/  s3://djis-dev-sascdm/&project./&env./sas-code/   --sse 'aws:kms' --sse-kms-key-id 'alias/SASIMPORTCMKTEST'";
x "aws s3 sync  /&root./&user./&env./&project./sas-data/slow_change/  s3://djis-dev-sascdm/&project./&env./sasdata/slow_change/  --sse 'aws:kms' --sse-kms-key-id 'alias/SASIMPORTCMKTEST'";


%macro folder_copy(folder_name);
x "aws s3 sync   /&root./&user./&env./&project./sas-data/&folder_name/  s3://djis-dev-sascdm/&project./&env./sasdata/&folder_name/  --sse 'aws:kms' --sse-kms-key-id 'alias/SASIMPORTCMKTEST'";


x "aws s3 ls s3://djis-dev-sascdm/sascdm/prod/sasdata/&folder_name./ > /&root./&user./&env./&project./report/&env._s3_&folder_name._list_&date_mmddyy._&start_dttm..txt";
x "ls -l /&root./&user./&env./&project./sas-data/&folder_name./ > /&root./&user./&env./&project./report/&env._ec2_&folder_name._&date_mmddyy._&start_dttm..txt";

filename s3_list "/&root./&user./&env./&project./report/&env._s3_&folder_name._list_&date_mmddyy._&start_dttm..txt";
filename s3_list2 "/&root./&user./&env./&project./report/&env._ec2_&folder_name._&date_mmddyy._&start_dttm..txt";

data today_list;
	infile s3_list dlm='0A'x truncover dsd;
	input aline $300. ;

	file_name = scan (aline, -1, ' ');
	
	file_size=input(scan(aline, -2, ' '),8.);
	
	file_time=input(scan(aline, -3, ' '),time8.);
	file_date=input(scan(aline,-4, ' '),YYMMDD10.);
format file_date mmddyy10. file_time time8.;

run;

proc print data=today_list;
run;



data today_list2;
	infile s3_list2 dlm='0A'x truncover dsd firstobs=2;
	input aline $300. ;

	file_name = scan (aline, -1, ' ');
	
	file_size=input(scan(aline, -5, ' '),8.);
	
	file_time=input(scan(aline, -2, ' '),time8.);
	file_date=input(scan(aline,-3, ' '),YYMMDD10.);
format file_date mmddyy10. file_time time8.;

run;

proc print data=today_list2;
run;

proc sql;
create table &folder_name._today_list3 as
select *,case when a.file_name=b.file_name and a.file_size=b.file_size then 1 else 0 end as matched
from today_list2 a left join 
	 today_list b on a.file_name=b.file_name and  
	 				 a.file_size=b.file_size ;
quit;

proc sql;
select count(*)  into:  list3
from  &folder_name._today_list3
where matched=0;
quit;

proc export data=&folder_name._today_list3 outfile="/&root./&user./&env./&project./report/&env._s3_ec2_&folder_name._list_&date_mmddyy._&start_dttm..txt" dbms=csv replace;
run;


%put &list3;

%mend folder_copy;

%folder_copy(cust_view);
%folder_copy(cust_view_fmr);
%folder_copy(wsjplus);
%folder_copy(wsjplus_archive);
%folder_copy(Stickrate);









