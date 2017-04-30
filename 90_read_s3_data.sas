*
This program will:
1) create today's staging tables loading tracking table: wsjplus.stg_table_load_status_&date_mmddyy
2) list all files in S3 using CLI
3) if control file (.ctl) exists, to find the data file,
4) loop through all files, copy them to corresponding tables
5) check row count loaded and row count is control file, if same then load_status=good
6) update wsjplus.stg_table_load_status_&date_mmddyy with status
7) until loop ends
8) handle cumulative omniture loading separately;
;

** the program is re-runnable;

*%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";
%include "&root./&user./&env./&project./sas-code/215_create_tmp_cdb_tables.sas";

%create_tmp_tables(myred,&schema,,N); 

 		filename mymail email 
      to = &to_email. 
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alerts: SUCCESS - &env_email. SAS/Redshift Update has begun for the as of date:  &today_MMDDYY10"
			replyto=&reply_to_email;
			
		data _null_;
			file mymail;

			dt=  put(&run_date, MMDDYY10.) || ' ' || put ((&start_time), timeampm11.);

	
        put "Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";
		run; 	



data control_file data_file;
	set wsjplus.qualified_ctl_data;
	
	if load_status ne 'good';
	
	if file_type = 'ctl' then output control_file;
	else if file_type= 'csv' then output data_file;
run;

proc sql;
	select count(*) into: ttl_files_2_load
	from control_file;
quit;
%put ttl_files_2_load=&ttl_files_2_load;

* %let ttl_files_2_load=1;  * for testing purpose;

%macro copy_2_redshift;

	%do ith=1 %to &ttl_files_2_load;
		data control_file_1;;
			set control_file;
			 if _n_ = &ith;
			
			call symput("table_name", strip(table_name_in_aws_staging));
			call symput("control_file_name", strip(file_name));
			call symput("file_name_base", strip(file_name_base));
			call symput("file_name_main", strip(file_name_main));
			call symput("Refresh_incremental", strip(upcase(Refresh_incremental)));
		run;	
		%put control: &control_file_name;
		
		data data_file_1;
			set data_file;
			if file_name_base="&file_name_base";
			call symput("data_file_name", strip(file_name));
		run;
		%put data: &data_file_name;
		
		%let Process_start_timestamp = "%sysfunc(datetime(),datetime19.)"dt;
	
    %if &compressed_files=1 %then %do;

		data construct_copy_string;
			format copy_csv_string copy_ctl_string $500.;
			copy_csv_string = "copy &stg..&table_name from 's3://&s3_bucket./&data_file_name'" || '0a'x ||
				   "credentials 'aws_iam_role=&aws_arn./&rs_2_s3_role'"  || '0a'x ||
				   "delimiter '|'" || '0a'x ||
				   "statupdate on" || '0a'x ||
				   "removequotes" || '0a'x ||
				   "emptyasnull" || '0a'x ||
				   "blanksasnull" || '0a'x ||
				   "acceptanydate" || '0a'x ||
 				   "gzip" || '0a'x ||
				   "COMPUPDATE on" || '0a'x ||
 				   "dateformat 'auto'" || '0a'x ||
				   "acceptinvchars" || '0a'x ||
				   "maxerror 100";
			
			copy_ctl_string = "copy &stg..staging_load_control from 's3://&s3_bucket./&control_file_name'" || '0a'x ||
				   "credentials 'aws_iam_role=&aws_arn./&rs_2_s3_role'"  || '0a'x ||
				   "delimiter '|'" || '0a'x ||
				   "statupdate on" || '0a'x ||
				   "removequotes" || '0a'x ||
				   "emptyasnull" || '0a'x ||
				   "blanksasnull" || '0a'x ||
				   "acceptanydate" || '0a'x ||
				   "dateformat 'auto'" || '0a'x ||
				   "acceptinvchars" || '0a'x ||
				   "maxerror 100";
				
			call symput ("copy_csv_string", strip(copy_csv_string));
			call symput ("copy_ctl_string", strip(copy_ctl_string));
		run;
	  %end;
     
     %else %do;
     
     		data construct_copy_string;
			format copy_csv_string copy_ctl_string $500.;
			copy_csv_string = "copy &stg..&table_name from 's3://&s3_bucket./&data_file_name'" || '0a'x ||
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
			
			copy_ctl_string = "copy &stg..staging_load_control from 's3://&s3_bucket./&control_file_name'" || '0a'x ||
				   "credentials 'aws_iam_role=&aws_arn./&rs_2_s3_role'"  || '0a'x ||
				   "delimiter '|'" || '0a'x ||
				   "statupdate on" || '0a'x ||
				   "removequotes" || '0a'x ||
				   "emptyasnull" || '0a'x ||
				   "blanksasnull" || '0a'x ||
				   "acceptanydate" || '0a'x ||
				   "dateformat 'auto'" || '0a'x ||
				   "acceptinvchars" || '0a'x ||
				   "maxerror 100";
				
			call symput ("copy_csv_string", strip(copy_csv_string));
			call symput ("copy_ctl_string", strip(copy_ctl_string));
		run;
   %end;
   
     
			
		proc sql noerrorstop noprint;
			connect using myred;

			%if "&Refresh_incremental" ne "CUMULATIVE" %then execute("truncate table &stg..&table_name") by myred;;
			
			select count(*) into: Row_count_before_copy
			from stg.&table_name;

			execute(&copy_csv_string) by myred;

			execute("truncate table &stg..staging_load_control") by myred;

			execute(&copy_ctl_string) by myred;
		quit;
		
		%let row_count_loaded=0;
		proc sql noprint;
			select count(*) into: row_count_loaded
			from stg.&table_name;
		quit;
	
		%let Process_end_timestamp = "%sysfunc(datetime(),datetime19.)"dt;
	
		data count_in_control;
			set stg.staging_load_control;
			
			if index(control_line, 'ROWCOUNT');
			
			row_count_in_control = compress(scan(control_line, 2, ' '),,'kd')+0;
			
			format load_status $5.;
			
			load_status='bad';
			if row_count_in_control=&row_count_loaded then load_status='good';
			
			call symput("row_count_in_control", strip(row_count_in_control));
			call symput("load_status", strip(load_status));
		run;
				
		data wsjplus.stg_table_load_status_&date_mmddyy;
			set wsjplus.stg_table_load_status_&date_mmddyy;
			
			format data_file_name control_file_name $60.;

			if table_name_in_aws_staging="&table_name" then do;
				load_status="&load_status";
				Row_count_before_copy=&Row_count_before_copy;
				Row_count_loaded=&Row_count_loaded;
				Row_count_in_control=&Row_count_in_control;
				Process_date=&today_date;
				Process_end_timestamp=&Process_end_timestamp;
				Process_start_timestamp=&Process_start_timestamp;
				data_file_name="&data_file_name";
				control_file_name="&control_file_name";
				
				time_taken_in_mins = round((Process_end_timestamp - Process_start_timestamp)/60, 0.01);
			end;
		run;		
	%end;
%mend copy_2_redshift;

%copy_2_redshift;

*omniture data is cumulative, needs to add today's file
* delete in case it was loaded once;

proc sql;
	select max(access_dt) into: max_daily_date
	from stg.omniture_data;
quit;
%put &max_daily_date;

data _null_;
	omniture_day="&max_daily_date"d;
	
	omniture_delta_date = put(year(omniture_day), z4.) || '-' || 
				put(month(omniture_day), z2.) || '-' || 
				put(day(omniture_day), z2.);
	
	call symput ("omniture_delta_date", strip(omniture_delta_date));
run;
%put &omniture_delta_date;

data append_str;
	appd_string = "delete from &stg..tmp_wsjplus_omniture" || '0a'x ||
		      "where access_dt ='&omniture_delta_date'; " || '0a'x ||
		      "                           " || '0a'x ||
		      "insert into &stg..tmp_wsjplus_omniture (access_dt, vxid_encrypt, site_section, headline_article, visits" || '0a'x ||
		     "                                      ,unique_visitors, pageviews, device, last_rec_dt, uuid_upd_dt,run_dt)" || '0a'x ||
		     "select access_dt, vxid_encrypt, site_section, headline_article, visits" || '0a'x ||
		     "	     ,unique_visitors, pageviews, 'ONLINE', '&omniture_delta_date', null, '&run_date_aws'" || '0a'x ||
		     "from &stg..omniture_data";

	call symput ("appd_string", strip(appd_string));
run;			

*append today omniture delta file;
proc sql noerrorstop;
   connect using myred;
	execute (&appd_string) by myred;
	disconnect from myred;
quit;

proc sql;
	select count(*) into: omniture_row_count_loaded
	from stg.tmp_wsjplus_omniture;
quit;

data wsjplus.stg_table_load_status_&date_mmddyy;
	set wsjplus.stg_table_load_status_&date_mmddyy;

	if table_name_in_aws_staging="TMP_WSJPLUS_OMNITURE" then do;
		Row_count_loaded=&omniture_row_count_loaded;
		Process_date=&today_date;
	end;
run;

