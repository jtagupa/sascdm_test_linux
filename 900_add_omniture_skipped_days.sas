*
This program will:
1) create today's staging tables loading tracking table: omniture_&date_mmddyy
2) list all files in S3 using CLI
3) if control file (.ctl) exists, to find the data file,
4) loop through all files, copy them to corresponding tables
5) check row count loaded and row count is control file, if same then load_status=good
6) update omniture_&date_mmddyy with status
7) until loop ends
8) handle cumulative omniture loading separately;
;

** the program is re-runnable;

%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";

%macro copy_omniture;

	data construct_copy_string;
		format copy_csv_string copy_ctl_string $500.;
		copy_csv_string = "copy &stg..OMNITURE_DATA from 's3://&s3_bucket./WSJPLUS_DAILYREPORT_&today_yyyymmdd..csv'" || '0a'x ||
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

		copy_ctl_string = "copy &stg..staging_load_control from 's3://&s3_bucket./WSJPLUS_DAILYREPORT_&today_yyyymmdd..ctl'" || '0a'x ||
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

	proc sql noerrorstop noprint;
		connect using myred;

		execute("truncate table &stg..OMNITURE_DATA") by myred;;

		select count(*) into: Row_count_before_copy
		from &stg..TMP_WSJPLUS_OMNITURE;

		execute(&copy_csv_string) by myred;

		execute("truncate table &stg..staging_load_control") by myred;

		execute(&copy_ctl_string) by myred;
	quit;

	%let row_count_loaded=0;
	proc sql noprint;
		select count(*) into: row_count_loaded
		from &stg..OMNITURE_DATA;
	quit;

	data count_in_control;
		set &stg..staging_load_control;

		if index(control_line, 'ROWCOUNT');

		row_count_in_control = compress(scan(control_line, 2, ' '),,'kd')+0;

		format load_status $5.;

		load_status='bad';
		if row_count_in_control=&row_count_loaded then load_status='good';

		call symput("row_count_in_control", strip(row_count_in_control));
		call symput("load_status", strip(load_status));
	run;

	data omniture_&date_mmddyy;
		set omniture_&date_mmddyy;

		format data_file_name control_file_name $60.;
		load_status="&load_status";
		Row_count_before_copy=&Row_count_before_copy;
		Row_count_loaded=&Row_count_loaded;
		Row_count_in_control=&Row_count_in_control;
		Process_date=&today_date;
		data_file_name="WSJPLUS_DAILYREPORT_&today_yyyymmdd..csv";
		control_file_name="WSJPLUS_DAILYREPORT_&today_yyyymmdd..ctl";

		time_taken_in_mins = round((Process_end_timestamp - Process_start_timestamp)/60, 0.01);
	run;	

	proc sql;
		select max(access_dt) into: max_daily_date
		from &stg..omniture_data;
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
		from &stg..tmp_wsjplus_omniture;
	quit;
		
	data omniture_&date_mmddyy;
		Row_count_loaded=&omniture_row_count_loaded;
		Process_date=&today_date;
	run;
%mend copy_omniture;

%macro add_daily_omniture;
	%let mth=1;
	%do %while (%scan(&missed_days, &mth, ' ') ne );
		%let missed_day =%scan(&missed_days, &mth, ' ');

		data a;
			format today_date date9.;
			*today_date=today() - 2 ;  * data as-of-date, a complete set of data is 2 days behind;
			today_date="&missed_day"d;  * remove in production;
			date_mmddyy=put(today_date, mmddyyn6.);
			today_yyyymmdd=year(today_date)*10000 + month(today_date) * 100 + day(today_date);
			today_aws = put(year(today_date), z4.) || '-' || put(month(today_date),z2.) || '-' || put(day(today_date), z2.);
			today_of_weekday = weekday(today_date);

			tm=timepart(datetime());
			start_dttm=today_aws  || '_' || put(hour(tm),z2.) || '-' || put(minute(tm),z2.) || '-' || put(second(tm),z2.);

			*the run_date is the actural program running date, used to pull error log from Redshift;
			run_date = today();
			run_date_aws = put(year(run_date), z4.) || '-' || put(month(run_date),z2.) || '-' || put(day(run_date), z2.);

			call symput ("today_date", strip(today_date));
			call symput ("date_mmddyy", strip(date_mmddyy));
			call symput ("today_yyyymmdd", strip(today_yyyymmdd));
			call symput ("today_aws", strip(today_aws));
			call symput ("start_dttm", strip(start_dttm));
			call symput ("today_of_weekday", strip(today_of_weekday));

			call symput ("run_date", strip(run_date));
			call symput ("run_date_aws", strip(run_date_aws));
		run;
		
		%put &today_yyyymmdd;
		%copy_omniture;
		
		%let mth=%eval(&mth +1);
	%end;
%mend;

%let missed_days = 30oct2016 31oct2016 01nov2016 02nov2016;
%add_daily_omniture;


proc sort data=&stg..tmp_wsjplus_omniture (keep=access_dt)
	out=acce_dt nodupkey;
	by descending access_dt;
run;