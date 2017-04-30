*
This program will:
1) query system table of stl_replacements, to get records with the invalid utf-8 chars replaced
2) query system table of stl_load_errors, to get records with errors
3) print the reports
4) check record counts and see if row count descrepency is more than 100 per table, if yes, stop process
5) Validate omniture loading separately, TMP_WSJPLUS_OMNITURE table is cumulative by appending daily delta file
;

** the program is re-runnable;

*%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";

data construct_query_string;
	format query_invalid_char_str query_err_records $5000.;
	
	query_invalid_char_str="select rs.userid, rs.slice, rs.tbl, rs.starttime, rs.session, rs.query, rs.filename, "  || '0a'x ||
				"	rs.line_number, rs.colname, "  || '0a'x ||
				"	rs.raw_line"  || '0a'x ||
				"from stl_replacements rs " || '0a'x ||
				"join " || '0a'x ||
				"	(select session, query, filename " || '0a'x ||
				  " 	from ( select *, row_number() over (partition by filename order by starttime desc) as rank " || '0a'x ||
				  "	       from stl_replacements " || '0a'x ||
				  "	       where trunc(starttime) >= '&run_date_aws' ) a " || '0a'x ||
				  "	where a.rank = 1) ls" || '0a'x ||
				  "	on rs.session = ls.session and rs.query=ls.query and rs.filename = ls.filename" || '0a'x ||
				  "	order by rs.filename, rs.session, rs.query;"
				  ;;
	
	query_err_records="select rs.userid, rs.slice, rs.tbl, rs.starttime, rs.session, rs.query, rs.filename, "  || '0a'x ||
			    "	rs.line_number, rs.colname, rs.type, "  || '0a'x ||
			    "	rs.col_length, rs.position, rs.raw_line, rs.raw_field_value, rs.err_code, rs.err_reason"  || '0a'x ||
			    "from stl_load_errors rs " || '0a'x ||
			    "join " || '0a'x ||
			    "	(select session, query, filename " || '0a'x ||
			    " 	from ( select *, row_number() over (partition by filename order by starttime desc) as rank " || '0a'x ||
			    "	       from stl_load_errors " || '0a'x ||
			    "	       where trunc(starttime) >= '&run_date_aws' ) a " || '0a'x ||
			    " 	where a.rank = 1) ls" || '0a'x ||
			    "	on rs.session = ls.session and rs.query=ls.query and rs.filename = ls.filename" || '0a'x ||
			    "   where rs.filename like '%.csv%'" || '0a'x ||
			    "	order by rs.filename, rs.session, rs.query;"
			  ;;
	
	call symput("query_invalid_char_str", strip(query_invalid_char_str));
	call symput("query_err_records", strip(query_err_records));
run;

%put query_invalid_char_str=&query_invalid_char_str;
%put query_err_records=&query_err_records;

proc sql;
	connect to sasiorst as x1(server=&rs_server port=&rs_port
	user=&rs_user password=&rs_pw database=&rs_db);
	
	   create table rs_replaced_latest as
		select *
		from connection to x1
		(&query_invalid_char_str);

	   create table rs_err_latest as
		select *
		from connection to x1
		(&query_err_records);		
quit;

proc sql;
	create table wsjplus.record_replaced_invalid_&date_mmddyy as
		select rs.*, ld.table_name_in_aws_staging
		from rs_replaced_latest rs
			join wsjplus.stg_table_load_status_&date_mmddyy ld
		on scan(rs.filename, -1, '/') =ld.data_file_name;

	create table wsjplus.record_err_records_&date_mmddyy as
		select rs.*, ld.table_name_in_aws_staging
		from rs_err_latest rs
		join wsjplus.stg_table_load_status_&date_mmddyy ld
		on scan(rs.filename, -1, '/') = ld.data_file_name;			
quit;

proc sql;
	select count(*) into: before_today_omniture
	from stg.tmp_wsjplus_omniture
	where access_dt ne
			(
				select max(access_dt)
				from stg.OMNITURE_DATA
			);
	
	select count(*) into: today_omniture_appended
	from stg.tmp_wsjplus_omniture
	where access_dt eq
			(
				select max(access_dt)
				from stg.OMNITURE_DATA
			);
	
	select count(*) into: today_omniture_delta
		from stg.OMNITURE_DATA;
quit;

*QC: if there is any missing dates from &omniture_qc_cutoff_dt to &today_date
Omniture daily data is one day behind;
proc sql;
	select min(access_dt),
		max(access_dt),
		count(distinct access_dt) into
			: min_access_dt,
			: max_access_dt,
			: ttl_access_dts
 	from stg.tmp_wsjplus_omniture
 	where access_dt>="&omniture_qc_cutoff_dt"d
 		and access_dt<&today_date;
quit;

proc sort data=stg.tmp_wsjplus_omniture (keep=access_dt)
	out=acce_dt nodupkey;
	by descending access_dt;
run;

data miss_days_flag;
	set acce_dt (where = (access_dt> "&omniture_qc_cutoff_dt"d));
	
	format lag_1 date9.;
	lag_1=lag1(access_dt) ;
	
	diff=lag_1 - access_dt;
	if diff>1;
run;
%let missing_days=0;

data miss_days;
	set miss_days_flag end=eof;
	
	format missing_days $500.;
	retain missing_days;
	
	do i= (lag_1 -1) to (access_dt +1) by -1;
		if missing_days = '' then missing_days = put (i, mmddyy10.);
		else missing_days = strip(missing_days) || ', ' || put (i, mmddyy10.);
	end;
	
	if eof then call symput("missing_days", strip(missing_days));
run;
%put &missing_days;

data miss_day_rpt;
	omniture_miss_days ="&missing_days";
	
	format missed_days $15.;
	
	days=countw(omniture_miss_days, ',');
	
	do i=1 to days;
		missed_days=strip(scan(omniture_miss_days, i, ','));
		output;
	end;
	
	drop i omniture_miss_days days;
	
	lable missed_days = 'Missed Date';
run;

%macro check_loading_status;
	*check to see if this is re-run, make sure code is re-runnable;
	data a;
		ran_already=%varexist (ds = wsjplus.stg_table_load_status_&date_mmddyy, var =overall_load_status);
		call symput("ran_already", strip(ran_already));
	run;
	
	* the overall status, we do allow <100 bad records loaded, if bad records exceed 100 then SAS session will end;
	data wsjplus.stg_table_load_status_&date_mmddyy;  * decision: if daily process will stop;
		set wsjplus.stg_table_load_status_&date_mmddyy
			(%if &ran_already =1 %then drop=overall_load_status;) end=eof;

		format overall_load_status $5.;

		if _n_ eq 1 then do;	
			overall_load_status='good';
      bad_count_indicator='n';
		end;
		retain overall_load_status bad_count_indicator;

		if ((Row_count_loaded - Row_count_in_control) > 100
			or Row_count_loaded = 0 or Row_count_loaded =.
			or Row_count_in_control =0 or Row_count_in_control =.
		   )
			and overall_load_status ='good'
			and Redshift_Schema_Name='stg'
			and table_name_in_aws_staging ne "TMP_WSJPLUS_OMNITURE" then do;
          overall_load_status='bad';
          bad_count_indicator='y';
      end;
		* the appended daily omniture delta has to be the same as daily omniture delta;		
		if table_name_in_aws_staging eq "TMP_WSJPLUS_OMNITURE"
			and (&today_omniture_appended ne &today_omniture_delta or &before_today_omniture=0)
			and overall_load_status ='good' then overall_load_status='bad';

		if eof then do;			
			call symput ("overall_load_status", strip(overall_load_status));
			call symput ("bad_count_indicator", strip(bad_count_indicator));
	end;
	run;

	*make sure no missing dates;	
	data validate_omniture_history;
		format start_access_dt end_access_dt date9. overall_load_status $5.;
		
		start_access_dt = "&min_access_dt"d;
		end_access_dt = "&max_access_dt"d;
		
		should_be_days = end_access_dt - start_access_dt + 1;
		
		ttl_access_dts=&ttl_access_dts;
		
		omniture_missing_dates = 'n';
		if should_be_days ne &ttl_access_dts then omniture_missing_dates='y';
		
		call symput ("omniture_missing_dates", strip(omniture_missing_dates));
		
		if "&overall_load_status" ='good' and omniture_missing_dates='y' then do;
			overall_load_status='bad';
			call symput ("overall_load_status", strip(overall_load_status));
		end;
	run;
	
	data wsjplus.stg_table_load_status_&date_mmddyy;
		set wsjplus.stg_table_load_status_&date_mmddyy;
		
		if table_name_in_aws_staging eq "TMP_WSJPLUS_OMNITURE" then omniture_missing_dates="&omniture_missing_dates";
	run;
	
	%if &omniture_missing_dates = y %then %do;
		ods pdf body ="&root./&user./&env./&project./report/omniture_data_missing_date_&date_mmddyy..pdf";	
			proc print data=validate_omniture_history noobs;
				format start_access_dt end_access_dt date9.;
				title "staging table overall loading status";
			run;
			
			proc print data=miss_day_rpt noobs;
				title "Omniture missed days";
			run;
		ods pdf close;	
	%end;
		
%mend check_loading_status;
%check_loading_status;
%put &overall_load_status;

ods pdf body ="&root./&user./&env./&project./report/staging_files_loading_status_&date_mmddyy..pdf";	
	proc print data=wsjplus.stg_table_load_status_&date_mmddyy noobs;
		var Redshift_Schema_Name Table_Name_in_AWS_Staging Update_Frequency Refresh_incremental	
			Load_status Row_count_loaded Row_count_in_control Must_update
			load_status Row_count_loaded Row_count_in_control Process_date
			Process_end_timestamp Process_start_timestamp time_taken_in_mins
			data_file_name control_file_name;
			;
		format Process_end_timestamp Process_start_timestamp datetime19. Process_date date9.;
		title "staging table overall loading status";
	run;

ods pdf close;

ods pdf body ="&root./&user./&env./&project./report/staging_table_loading_invalid_error_&date_mmddyy..pdf";	
	proc print data=wsjplus.record_replaced_invalid_&date_mmddyy noobs;
		title 'staging table loading status -- records with invalid chars replaced';
	run;

	proc print data=wsjplus.record_err_records_&date_mmddyy noobs;
		title 'staging table loading status -- records not loaded due to errors';
	run;

	title '';
ods pdf close;

*be careful that SAS session may end!!! you may comment out the section when debugging;
%macro issue_handling;
	%if &overall_load_status=bad %then %do;
		* send email notification first here, it is a logical error;
   
   proc sql;
      create table missing_dates as
        select distinct access_dt 
        from stg.TMP_WSJPLUS_OMNITURE
 
        order by access_dt desc;
    quit;

    data last_30_obs;
      set missing_dates(obs=30);
    run;
	data all_last30_obs;
	do i=0 to 30;
		access_dt=&date_run - i;
		output;
	end;
	keep access_dt;
	run;

	proc sql;
	create table match_set as
	

	select a.access_dt,b.access_Dt as match_access_dt
	from all_last30_obs a  left join
		 last_30_obs b on a.access_dt=b.access_dt;
   quit;
 
   data match_set;
   	set match_set;
	where match_access_dt =.;

	keep access_dt;
	format access_dt mmddyy10.;
	run;


    data _null_;

        set match_set end=last;

        call symput(compress('last_20_'||_n_), put(access_dt,mmddyy10.));

        if last then call symput('numb_ct',_n_);
      run;;


    %put &last_20_1  &numb_ct.;


 		filename mymail email 
      to = &to_email. 
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alerts: ERROR - &env_email. SAS/Redshift Update has ecountered an error for the as of date:  &today_MMDDYY10"
			replyto=&reply_to_email;

			
		data _null_;
			file mymail;
			put ;
			put "The SASCDM Update has encountered an error when loading the data from S3 to Redshift.";
			put;
			put "Error Codes:";
      %if &bad_count_indicator=y %then %do;
			put "The difference between count loaded to staging and count in control file exceeds a threshold of 100.";
      %end;

      %if &omniture_missing_dates=y %then %do;
      	put "Omniture table (tmp_wsjplus_omniture) has missing dates.";
      	put "See last loaded Access Dates below:";
		put " ";
        %do i=1 %to &numb_ct.;
          put "&&last_20_&i.";
        %end;
		put " ";
      	put "In order to correct the problem the days which are missing will need to be manually loaded via a utility program";
      %end;
			put " ";
			put "The Update process is suspended until resolution of the above listed issues.";
 			put " ";
      put "Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";

		run; 		
   
   		data _null_;
			abort abend ;;   *this is powerful, SAS session will end;
		run;
		
	%end; 		

  %else %do;
    filename mymail email 
      to = &to_email. 
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alerts: SUCCESS - &env_email. SAS/Redshift S3 File Load is complete for the as of date:  &today_MMDDYY10"
			replyto=&reply_to_email;
			
		data _null_;
			file mymail;

			dt=  put(&run_date, MMDDYY10.) || ' ' || put ((&start_time), timeampm11.);

	
        put "Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";
		run; 	
%end;
 
  
%mend issue_handling;
%issue_handling;

%macro if_abort;
	%if &overall_load_status=bad %then %do;
		data _null_;
			abort abend ;;   *this is powerful, SAS session will end;
		run;
	%end;
	
	%if &omniture_missing_dates = y %then %do;
		data _null_;
			abort abend ;;   *this is powerful, SAS session will end;
		run;
	%end;
%mend;

