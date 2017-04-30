* This program will do the following when moving to production
1) rename the permanent table name to old_ in Redshift
2) rename the tmp_ table to the permanent table name;
;

*%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";

%macro get_rows_in_table (connection, schema, table_name);
	%if_redshift_cdb_table_exist(rs_schema=&schema, rs_table=&table_name, flag_exist=table_exist);
	
	%global row_count_prod;
	
	%let row_count_prod=0;
	
	%if &table_exist eq 1 %then %do;
		proc sql;
			select count(*) into: row_count_prod
			from cdb.&table_name;
		quit;
	%end;
	
	data wsjplus.stg_table_load_status_&date_mmddyy;
		set wsjplus.stg_table_load_status_&date_mmddyy;
		
		if Redshift_Schema_Name ="&cdb." 
			and upcase(table_name_in_aws_staging)= upcase("&table_name") then do;
			row_count_prod =&row_count_prod;
		end;
	run;
%mend;		

%get_rows_in_table(connection=my_cdb, schema=&schema_cdb, table_name=address);
%get_rows_in_table(connection=my_cdb, schema=&schema_cdb, table_name=wsjplus_data);
%get_rows_in_table(connection=my_cdb, schema=&schema_cdb, table_name=omniture);
%get_rows_in_table(connection=my_cdb, schema=&schema_cdb, table_name=registration);
%get_rows_in_table(connection=my_cdb, schema=&schema_cdb, table_name=subscription);
%get_rows_in_table(connection=my_cdb, schema=&schema_cdb, table_name=entitlement);
%get_rows_in_table(connection=my_cdb, schema=&schema_cdb, table_name=single_view);
%get_rows_in_table(connection=my_cdb, schema=&schema_cdb, table_name=single_view_former);

*overall status in production tables;
%macro check_overall_prod_status;
	*check to see if this is re-run, make sure code is re-runnable;
	data a;
		ran_already=%varexist (ds = wsjplus.stg_table_load_status_&date_mmddyy, var =overall_prod_table_status);
		call symput("ran_already", strip(ran_already));
	run;
	
	data wsjplus.stg_table_load_status_&date_mmddyy;
		set wsjplus.stg_table_load_status_&date_mmddyy
			(%if &ran_already eq 1 %then drop=overall_prod_table_status;)
			end=eof;

		format overall_prod_table_status $25.;

		if _n_ eq 1 then overall_prod_table_status ='good';

		retain overall_prod_table_status;
		
		if Redshift_Schema_Name ="&cdb." 
			and (row_count_prod ne row_count_loaded
				or row_count_prod = 0
				or row_count_prod = .) 
			and overall_prod_table_status ='good' then overall_prod_table_status ='error';

		if eof then do;
			call symput ("overall_prod_table_status", strip(overall_prod_table_status));
		end;
	run;
%mend check_overall_prod_status;
%check_overall_prod_status;

%put overall_prod_table_status=&overall_prod_table_status;

ods pdf body ="&root./&user./&env./&project./report/program_running_status_&today_yyyymmdd..pdf";	
	proc print data=wsjplus.program_running_status_&date_mmddyy noobs;
		var program_name log_status;
		title "Production tables loading status as of &date_mmddyy";
	run;

	title '';
ods pdf close;

ods pdf body ="&root./&user./&env./&project./report/production_loading_status_&today_yyyymmdd..pdf";	
	proc print data=wsjplus.stg_table_load_status_&date_mmddyy noobs;
		title "Production tables loading status on &date_mmddyy";
		var table_name_in_aws_staging
			update_frequency refresh_incremental process_date
			row_count_loaded row_count_in_control row_count_before_copy row_count_prod
			data_file_name control_file_name
			;
		format process_start_timestamp datetime22.5;
	run;

	title '';
ods pdf close;

* only when there is missing files, the following macro will be run;
%macro report_product_tables;
	%if &overall_prod_table_status ne good %then %do;
				
		filename mymail email 
      to = &to_email. 
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alerts: ERROR - &env_email. Loading data to CDB for the as of date:  &today_MMDDYY10"
			replyto=&reply_to_email
			attach="&root./&user./&env./&project./report/production_loading_status_&today_yyyymmdd..pdf";

			
		data _null_;
			file mymail;

			put "Loading to production tables is not successful. SAS session has stopped.";
			put "Please review attached report.  The counts in the production table do not match the counts in the staging table";
			put ;
			put "Once issues are resolved, you may re-run partial of the updates.";
      put "Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";
			put ;
		run; 	
		
		*ENDSAS;   *this is powerful, SAS session will end;
	%end;
	%else %do;

		filename mymail email 
      to = &to_email. 
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email.
			subject="SASCDM &env_email. Alerts: SUCCESS - &env_email. SASCDM Process is complete for the as of date:  &today_MMDDYY10"
			replyto=&reply_to_email
			attach=("&root./&user./&env./&project./report/production_loading_status_&today_yyyymmdd..pdf"
				"&root./&user./&env./&project./report/staging_table_loading_invalid_error_&date_mmddyy..pdf"
				"&root./&user./&env./&project./report/program_running_status_&today_yyyymmdd..pdf"
				);

		data _null_;
			file mymail;
			put ;
			put "Loading to production tables was successful.";
			put "Please review attached reports.";
			put ;
      put "Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";
		run; 	
		
	%end;
%mend report_product_tables;
%report_product_tables;

