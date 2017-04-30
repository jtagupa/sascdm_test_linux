
data daily_check;
	set slow_chg.table_list;
run;

data stg;
	set daily_check;
	if Redshift_Schema_Name ='stg';
run;

proc sql;	
	select count(*) into: ttl_files_2_load
	from stg
	where Redshift_Schema_Name ='stg';
quit;
%put ttl_files_2_load=&ttl_files_2_load;

* %let ttl_files_2_load=1;  * for testing purpose;

%macro compare_stg;

	%do ith=1 %to &ttl_files_2_load;
		data control_file_1;;
			set stg;
			 if _n_ = &ith;
			
			call symput("table_name", strip(table_name_in_aws_staging));
		run;	
		
		%let row_count_in_stg_alone=0;
		proc sql noprint;
			select count(*) into: row_count_in_stg_alone
			from stg.&table_name;
		quit;
			
		%let row_count_in_cdb_alone=0; 
		proc sql noprint;
			select count(*) into: row_count_in_cdb_alone
			from cdb.&table_name;
		quit;
		
		data daily_check;
			set daily_check;
			
			format data_file_name control_file_name $60.;

			if table_name_in_aws_staging="&table_name" then do;
				row_count_in_stg_alone = &row_count_in_stg_alone;
				row_count_in_cdb_alone = &row_count_in_cdb_alone;
			end;
		run;		
	%end;
%mend compare_stg;
%compare_stg;


%macro compare_cdb(dataset_name, table_name);

	%let row_count_in_stg_alone=0;
	proc sql noprint;
		select count(*) into: row_count_in_stg_alone
		from &dataset_name. ;
	quit;

	%let row_count_in_cdb_alone =0;
	proc sql noprint;
		select count(*) into: row_count_in_cdb_alone
		from cdb.&table_name. ;
	quit;

	data daily_check;
		set daily_check;

		format data_file_name control_file_name $60.;

		if table_name_in_aws_staging="&table_name" then do;
			row_count_in_stg_alone = &row_count_in_stg_alone;
			row_count_in_cdb_alone = &row_count_in_cdb_alone;
		end;
	run;
%mend compare_cdb;

%compare_cdb(dataset_name=keep3.singleview_050516, table_name=single_view);         
%compare_cdb(dataset_name=WP_OP.wsjplus_data_&date_mmddyy, table_name=wsjplus_data);
%compare_cdb(dataset_name=wsjplus.omniture, table_name=omniture);             
%compare_cdb(dataset_name=wsjplus.address, table_name=address);                
%compare_cdb(dataset_name=wsjplus.registration, table_name=registration);         
%compare_cdb(dataset_name=wsjplus.subscription, table_name=subscription);           
%compare_cdb(dataset_name=wsjplus.entitlement, table_name=entitlement);             


* link to latest etl update;

proc sql noprint;
	select max( process_date) into: latest_process_date
	from stg.redshift_table_hist
	where Process_start_timestamp =
		(
			select max (Process_start_timestamp)
			from stg.redshift_table_hist
		);
quit;
%put &latest_process_date;

proc sql noprint;
	create table lastest_etl as
	select *
	from  stg.redshift_table_hist
	where process_date="&latest_process_date"d
	order by table_name_in_aws_staging;
quit;

proc sql noprint;
	create table big_table as
	select l.redshift_schema_name
		, l.table_name_in_aws_staging
		, l.note1
		, l.update_frequency
		, l.refresh_incremental
		, l.process_date
		, l.row_count_loaded
		, l.row_count_in_control
		, l.row_count_prod
		, l.row_count_in_control_today
		,d.row_count_in_stg_alone
		,d.row_count_in_cdb_alone
		, l.row_count_in_control_previous
		, l.row_count_in_control_median
		, l.data_file_name
		, l.control_file_name
		, l.omniture_missing_dates
		, l.load_status
		, l.overall_load_status
		, l.overall_load_status_tmp_tbl
		, l.overall_stg_2_cdb_status
		, l.overall_prod_table_status
		, l.time_taken_in_mins
		, l.file_name
		, l.process_start_timestamp
		, l.process_end_timestamp
	from daily_check d
		left join lastest_etl l
		on d.Table_Name_in_AWS_Staging = l.Table_Name_in_AWS_Staging;
quit;

data final_check;
	set big_table;
	if row_count_in_stg_alone ne row_count_in_cdb_alone 
		or row_count_in_cdb_alone =0 or row_count_in_cdb_alone=.
		or row_count_in_stg_alone =0 or row_count_in_stg_alone=.;
run;