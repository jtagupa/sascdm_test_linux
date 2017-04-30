*loading staging tables to production;
data table_list;
	set slow_chg.table_list;
run;

%macro cdb_2_cdb;

	%put &ttl_files_2_load;
	
	%do ith=1 %to &ttl_files_2_load;
		data data_file_1;;
			set table_list;
			 if _n_ = &ith;
			
			call symput("table_name", strip(table_name_in_aws_staging));
		run;	
		
		%put data: &table_name;

		%macro oi;
		data construct_copy_string;
			format copy_csv_string $500.;
			copy_csv_string = "drop table if exists &cdb..&table_name._&today_yyyymmdd; ";
			
			call symput ("copy_csv_string", strip(copy_csv_string));
		run;
		%mend;

		data construct_copy_string;
			format copy_csv_string $500.;
			copy_csv_string = "drop table if exists &cdb..&table_name.&ty_mmdd; " || '0a'x ||
					"create table &cdb..&table_name.&ty_mmdd (like &cdb..&table_name.);" || '0a'x ||
				        "insert into &cdb..&table_name.&ty_mmdd select * from &cdb..&table_name.;";
							
			call symput ("copy_csv_string", strip(copy_csv_string));
		run;
				
		proc sql noerrorstop noprint;
			connect using my_cdb;

			select count(*) into: Row_count_before_copy
			from cdb.&table_name;

			execute(&copy_csv_string) by my_cdb;

			select count(*) into: Row_count_after_copy
			from cdb.&table_name.&ty_mmdd;			
		quit;
					
		data a;
			format table_name $50.;
			table_name="&table_name";
			Row_count_before_copy=&Row_count_before_copy;
			Row_count_after_copy=&Row_count_after_copy;
		run;
		
		data wsjplus.cp_cdb_2_cdb_&today_yyyymmdd;
			set wsjplus.cp_cdb_2_cdb_&today_yyyymmdd (where=(table_name ne "&table_name"))
				a;
		run;		
	%end;
%mend cdb_2_cdb;

%macro run_2_days;

	%if &today_yyyymmdd=20161026 or &today_yyyymmdd=20161028 %then %do;
		
		%global ty_mmdd;
		
		%let ty_mmdd=%substr(&today_yyyymmdd,5,4);
		
		data wsjplus.cp_cdb_2_cdb_&today_yyyymmdd;
			format table_name $50.;
			stop;
		run;
		
		%global ttl_files_2_load;
		
		%let ttl_files_2_load = 0;
		
		proc sql;
			select count(*) into: ttl_files_2_load
			from table_list
			where Redshift_Schema_Name ="&stg." or Redshift_Schema_Name ="&cdb.";
		quit;
		%put ttl_files_2_load=&ttl_files_2_load;
		
		%macro o;
			%let ttl_files_2_load=1;  * for testing;
		%mend;

		%cdb_2_cdb;
	%end;
%mend;

%run_2_days;