* this program will save process history to Redshift;

%macro append_table_load_history;

	%if_redshift_table_exist(rs_schema=&stg., rs_table=redshift_table_hist, flag_exist=redshift_table_hist_flag);
	
	%if &redshift_table_hist_flag=1 %then %do;
		data new_redshift_table_hist;
			set stg.redshift_table_hist (where=(Process_date ne &today_date))
				wsjplus.stg_table_load_status_&date_mmddyy;
		run;

		proc sql;
			create table new_redshift_table_hist_ordered as
			select redshift_schema_name
				,table_name_in_aws_staging
				,note1
				,note2
				,update_frequency
				,refresh_incremental
				,process_date
				,row_count_loaded
				,row_count_in_control
				,row_count_prod
				,row_count_in_control_today
				,row_count_in_control_previous
				,row_count_in_control_median
				,data_file_name
				,control_file_name
				,omniture_missing_dates
				,load_status
				,overall_load_status
				,overall_load_status_tmp_tbl
				,overall_stg_2_cdb_status
				,overall_prod_table_status
				,time_taken_in_mins
				,file_name
				,process_start_timestamp
				,process_end_timestamp
			from new_redshift_table_hist;
		quit;

		proc sql noerrorstop noprint;
			connect using myred;

			execute("
				drop table if exists &stg..redshift_table_hist
				"
			) by myred;
		quit;
	%end;
	%else %do;
		data new_redshift_table_hist_ordered;
			set wsjplus.stg_table_load_status_&date_mmddyy;
		run;
	%end;
	
	*%split_n_export(numbe_of_slices=4, dataset_name=new_redshift_table_hist_1, export_file_name=jane_test_export, redshift_table_name=redshift_table_hist);
	data stg.redshift_table_hist;
		set new_redshift_table_hist_ordered;
	run;

%mend append_table_load_history;
%append_table_load_history;


