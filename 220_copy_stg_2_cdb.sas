*loading staging tables to production;

data as_of_data_file;
	set wsjplus.stg_table_load_status_&date_mmddyy;
	if Redshift_Schema_Name="&stg." and Refresh_incremental ne 'Cumulative';
	file_name=data_file_name;
run;

proc sql;
	create table table_list as
	select all.file_name
		,all.Refresh_incremental
		,all.table_name_in_aws_staging
		,td.*
	from as_of_data_file as all
		join wsjplus.qualified_ctl_data as td
		on all.file_name = td.file_name;
quit;

proc sql;
	select count(*) into: ttl_files_2_load
	from table_list;
quit;
%put ttl_files_2_load=&ttl_files_2_load;

* %let ttl_files_2_load=1;  * for testing;
%macro copy_2_cdb;

	%do ith=1 %to &ttl_files_2_load;
		data data_file_1;;
			set as_of_data_file;
			 if _n_ = &ith;
			
			call symput("table_name", strip(table_name_in_aws_staging));
			call symput("data_file_name", strip(file_name));
			call symput("file_name_base", strip(file_name_base));
			call symput("file_name_main", strip(file_name_main));
			call symput("Refresh_incremental", strip(upcase(Refresh_incremental)));
		run;	
		
		%put data: &data_file_name;
			
           %if &compressed_files=1 %then %do;
  
      
		data construct_copy_string;
			format copy_csv_string $500.;
			copy_csv_string = "copy &cdb..z&table_name from 's3://&s3_bucket./&data_file_name'" || '0a'x ||
				   "credentials 'aws_iam_role=&aws_arn./&rs_2_s3_role'"  || '0a'x ||
				   "delimiter '|'" || '0a'x ||
				   "statupdate on" || '0a'x ||
				   "removequotes" || '0a'x ||
				   "emptyasnull" || '0a'x ||
				   "blanksasnull" || '0a'x ||
				   "gzip" || '0a'x ||
				   "acceptanydate" || '0a'x ||
           "COMPUPDATE on" || '0a'x ||
				   "dateformat 'auto'" || '0a'x ||
				   "acceptinvchars" || '0a'x ||
				   "maxerror 100";
							
			call symput ("copy_csv_string", strip(copy_csv_string));
		run;
				
   %end;
   
   %else %do;
   	data construct_copy_string;
			format copy_csv_string $500.;
			copy_csv_string = "copy &cdb..z&table_name from 's3://&s3_bucket./&data_file_name'" || '0a'x ||
				   "credentials 'aws_iam_role=&aws_arn./&rs_2_s3_role'"  || '0a'x ||
				   "delimiter '|'" || '0a'x ||
				   "statupdate on" || '0a'x ||
				   "removequotes" || '0a'x ||
				   "emptyasnull" || '0a'x ||
				   "blanksasnull" || '0a'x ||
				   "acceptanydate" || '0a'x ||
				   "COMPUPDATE on" || '0a'x ||
				   "dateformat 'auto'" || '0a'x ||
				   "acceptinvchars" || '0a'x ||
				   "maxerror 100";
							
			call symput ("copy_csv_string", strip(copy_csv_string));
		run;
  %end;
  
        
        
        
		proc sql noerrorstop noprint;
			connect using my_cdb;

			%if "&Refresh_incremental" ne "CUMULATIVE" %then execute("truncate table &cdb..z&table_name") by my_cdb;;
			
			select count(*) into: Row_count_before_copy
			from cdb.z&table_name;

			execute(&copy_csv_string) by my_cdb;
		quit;
		
		%let row_count_loaded=0;
		proc sql noprint;
			select count(*) into: row_count_prod
			from cdb.z&table_name;
		quit;
   
   %put &row_count_prod;
   
	
		proc sql;
			select row_count_in_control into: row_count_in_control
			from wsjplus.stg_table_load_status_&date_mmddyy
			where upcase(table_name_in_aws_staging)=upcase("&table_name");
		quit;
					
		data wsjplus.stg_table_load_status_&date_mmddyy;
			set wsjplus.stg_table_load_status_&date_mmddyy;
			
			format data_file_name control_file_name $60.;

			if upcase(table_name_in_aws_staging)=upcase("&table_name") then do;
				row_count_prod=&row_count_prod;
			end;
		run;		
	%end;
%mend copy_2_cdb;
%copy_2_cdb;

*load omniture history;
data a;
	format unload_string unload_stringload_string $5000.;
	
	unload_string = "unload ('select * from &stg..TMP_WSJPLUS_OMNITURE')" || '0a'x ||
			"to 's3://djis-dev-sascdm/sfp-drop/redshift_TMP_WSJPLUS_OMNITURE.'" || '0a'x ||
			"credentials 'aws_iam_role=arn:aws:iam::550707989853:role/djis-dev-sascdm'" || '0a'x ||
			"allowoverwrite" || '0a'x ||
			"ADDQUOTES" || '0a'x ||
			"manifest" || '0a'x ||
			"gzip";
			

	load_string  = "truncate table &cdb..zTMP_WSJPLUS_OMNITURE;" || '0a'x ||
			"copy &cdb..zTMP_WSJPLUS_OMNITURE" || '0a'x ||
			"from 's3://djis-dev-sascdm/sfp-drop/redshift_TMP_WSJPLUS_OMNITURE.manifest' " || '0a'x ||
			"credentials 'aws_iam_role=arn:aws:iam::550707989853:role/djis-dev-sascdm.S3RO'" || '0a'x ||
			"statupdate   on" || '0a'x ||
			"removequotes" || '0a'x ||
			"emptyasnull" || '0a'x ||
		  "COMPUPDATE on" || '0a'x ||
			"blanksasnull" || '0a'x ||
			"gzip" || '0a'x ||
			"manifest" || '0a'x ||
			"maxerror 10";		
			
	call symput ("unload_string", strip(unload_string));
	call symput ("load_string", strip(load_string));
run;
	
	
x "aws s3 rm s3://djis-dev-sascdm/sfp-drop/redshift_TMP_WSJPLUS_OMNITURE.0000_part_00.gz";
x "aws s3 rm s3://djis-dev-sascdm/sfp-drop/redshift_TMP_WSJPLUS_OMNITURE.0001_part_00.gz";
x "aws s3 rm s3://djis-dev-sascdm/sfp-drop/redshift_TMP_WSJPLUS_OMNITURE.0002_part_00.gz";
x "aws s3 rm s3://djis-dev-sascdm/sfp-drop/redshift_TMP_WSJPLUS_OMNITURE.0003_part_00.gz";

	
proc sql noerrorstop;
   connect using myred;
	execute (&unload_string) by myred;
	disconnect from myred;
quit;

proc sql noerrorstop;
   connect using my_cdb;
	execute (&load_string) by my_cdb;
	disconnect from my_cdb;
quit;

proc sql;
	select count(*) into: after_today_omniture
	from cdb.ztmp_wsjplus_omniture;
quit;
%put &after_today_omniture;

data wsjplus.stg_table_load_status_&date_mmddyy;
	set wsjplus.stg_table_load_status_&date_mmddyy;

	if table_name_in_aws_staging="TMP_WSJPLUS_OMNITURE" then do;
		row_count_prod=&after_today_omniture;
	end;
run;

*overall status loading stg tables to cdb;
%macro check_stg_2_cdb;
	*check to see if this is re-run, make sure code is re-runnable;
	data a;
		ran_already=%varexist (ds = wsjplus.stg_table_load_status_&date_mmddyy, var =overall_stg_2_cdb_status);
		call symput("ran_already", strip(ran_already));
	run;
	
	data wsjplus.stg_table_load_status_&date_mmddyy;
		set wsjplus.stg_table_load_status_&date_mmddyy
			(%if &ran_already eq 1 %then drop=overall_stg_2_cdb_status;)
			end=eof;

		format overall_stg_2_cdb_status $5.;

		if _n_ eq 1 then overall_stg_2_cdb_status ='good';

		retain overall_stg_2_cdb_status;

		if Redshift_Schema_Name ="&stg."
			and (row_count_loaded ne row_count_prod)
			and overall_stg_2_cdb_status ='good' then overall_stg_2_cdb_status ='error';
			
		if eof then do;
			call symput ("overall_stg_2_cdb_status", strip(overall_stg_2_cdb_status));
		end;
	run;
%mend check_stg_2_cdb;
%check_stg_2_cdb;

%put overall_stg_2_cdb_status=&overall_stg_2_cdb_status;

ods pdf body ="&root./&user./&env./&project./report/production_loading_status_&today_yyyymmdd..pdf";	
	proc print data=wsjplus.stg_table_load_status_&date_mmddyy;
		title "Loading staging tables to production on &date_mmddyy";
		var table_name_in_aws_staging
			update_frequency refresh_incremental process_date
			row_count_loaded row_count_in_control row_count_before_copy row_count_prod
			data_file_name control_file_name
			overall_stg_2_cdb_status
			;
		format process_start_timestamp datetime22.5;
	run;

	title '';
ods pdf close;

* only when there is missing files, the following macro will be run;
%macro report_stg_2_cdb;
	%if &overall_stg_2_cdb_status ne good %then %do;
				
    
		filename mymail email 
      to = &to_email. 
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alerts: ERROR - &env_email. Loading data to CDB for the as of date:  &today_MMDDYY10"
			replyto=&reply_to_email
			attach="&root./&user./&env./&project./report/production_loading_status_&today_yyyymmdd..pdf";
      
			
		data _null_;
			file mymail;
			put ;
			put "Loading tables from staging to CDB was not successful. SAS session has stopped.";
			put "Please review attached report.";
			put ;
			*put "Once issues are resolved, you may re-run partial of the updates.";
			put ;
		run; 	
		
		%macro temp_out;
			data _null_;
				abort abend ;;   *this is powerful, SAS session will end;
			run;
		%mend temp_out;
	%end;
%mend report_stg_2_cdb;
%report_stg_2_cdb;
