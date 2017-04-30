*
This program will:
1) run after loading SAS datasets to production after transformation
2) query the log, expect to load the whole datasets. if failed, stop process
;

** the program is re-runnable;

*%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";

%macro check_load_2_temp_table;
	*check to see if this is re-run, make sure code is re-runnable;
	data a;
		ran_already=%varexist (ds = wsjplus.stg_table_load_status_&date_mmddyy, var =overall_load_status_tmp_tbl);
		call symput("ran_already", strip(ran_already));
	run;

	data wsjplus.stg_table_load_status_&date_mmddyy;
		set wsjplus.stg_table_load_status_&date_mmddyy
		    (%if &ran_already=1 %then drop=overall_load_status_tmp_tbl;) end=eof;

		format overall_load_status_tmp_tbl $25.;

		if _n_=1 then overall_load_status_tmp_tbl= 'good';
		retain overall_load_status_tmp_tbl;

		if Redshift_Schema_Name ="&cdb." 
			and (
				(Row_count_loaded - Row_count_in_control) gt 10 
        or (Row_count_loaded - Row_count_in_control) lt -10
				or Row_count_loaded = 0 or Row_count_loaded =.
				or Row_count_in_control =0 or Row_count_in_control =.
		   	     )
			and overall_load_status ne 'bad' then overall_load_status_tmp_tbl='bad';

		if eof then call symput ("overall_load_status_tmp_tbl", strip(overall_load_status_tmp_tbl));
	run;
	%put &overall_load_status_tmp_tbl;
%mend check_load_2_temp_table;
%check_load_2_temp_table;
%put overall_load_status_tmp_tbl=&overall_load_status_tmp_tbl;


*be careful that SAS session may end!!! you may comment out the section when debugging;
%macro err_handling;
	%if &overall_load_status_tmp_tbl=bad %then %do;
		ods pdf body ="&root./&user./&env./&project./report/staging_files_loading_status_&date_mmddyy..pdf";	
			proc print data=wsjplus.stg_table_load_status_&date_mmddyy noobs;
				var Redshift_Schema_Name Table_Name_in_AWS_Staging Update_Frequency Refresh_incremental	
					overall_load_status_tmp_tbl;
					;
				format Process_end_timestamp Process_start_timestamp datetime19. Process_date date9.;
				title "Loading SAS dataset to production tmp table status";
			run;
		
		ods pdf close;
		
		filename mymail email 
      to = &to_email. 
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alerts: ERROR - &env_email. Loading data to CDB for the as of date:  &today_MMDDYY10"
			replyto=&reply_to_email
			attach="&root./&user./&env./&project./report/staging_files_loading_status_&date_mmddyy..pdf";
			
		data _null_;
			file mymail;

			put "Loading SAS datasets to production tmp tables has issues, please review the attached report.";
			put "SAS session has stopped.";
			put ;
			put "Once the issue is resolved, please re-run the update.";
			put ;
        put "Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";
		run; 	
		
		%macro ou;
		data _null_;
			abort abend ;;   *this is powerful, SAS session will end;
		run;
		%mend ou;
	%end;
%mend err_handling;
%err_handling;
