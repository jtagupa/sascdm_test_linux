* This program will do the following when moving to production
1) rename the permanent table name to old_ in Redshift
2) rename the tmp_ table to the permanent table name;
;

*%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";

%macro rename_table (connection, schema, from_table_name, to_table_name);
	%if_redshift_cdb_table_exist(rs_schema=&schema, rs_table=&from_table_name, flag_exist=from_table_exist);
	
	%if &from_table_exist eq 1 %then %do;
		proc sql;

			connect using &connection;
			   execute
				(
					drop table if exists &schema..&to_table_name;

					alter table &schema..&from_table_name
					rename to &to_table_name;

				) by &connection;
			disconnect from &connection;
		quit;
	%end;
	%else %do;
		data a;
			call symput("from_file_status",upcase('error'));
		run;
		%put &from_file_status -- from table &from_table_name does not exist, the &to_table_name is kept the same as before;
	%end;
%mend;		

%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=tmp_address, to_table_name=address);
%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=tmp_wsjplus_data, to_table_name=wsjplus_data);
%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=tmp_omniture, to_table_name=omniture);
%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=tmp_registration, to_table_name=registration);
%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=tmp_subscription, to_table_name=subscription);
%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=tmp_entitlement, to_table_name=entitlement);
%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=tmp_single_view, to_table_name=single_view);
%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=zTMP_WSJPLUS_OMNITURE, to_table_name=TMP_WSJPLUS_OMNITURE);


/****03/27/17  - Added single_view_former *****************/

%rename_table(connection=my_cdb, schema=&schema_cdb, from_table_name=tmp_single_view_former, to_table_name=single_view_former);


%let table_Ct=0;

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

data _null_;
set table_list end=last;

call symput(compress('table_name'||_n_), strip(table_name_in_aws_staging));

if last then call symput('table_ct',_n_);
run;

%macro rename_table_stg (connection, schema);


%do i=1 %to &table_ct;


	%if_redshift_cdb_table_exist(rs_schema=&schema, rs_table=z&&table_name&i., flag_exist=from_table_exist);
	
	%if &from_table_exist eq 1 %then %do;
		proc sql;

			connect using &connection;
			   execute
				(
					drop table if exists &schema..&&table_name&i.;

					alter table &schema..z&&table_name&i.
					rename to &&table_name&i.;

				) by &connection;
			disconnect from &connection;
		quit;
	%end;
	%else %do;
		data a;
			call symput("from_file_status",upcase('error'));
		run;
		%put &from_file_status -- from table z&&table_name&i. does not exist, the &&table_name&i. is kept the same as before;
	%end;

%end;

%mend rename_table_stg;		

%rename_table_stg(connection=my_cdb,schema=&schema_cdb);

