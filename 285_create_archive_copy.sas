

%macro archive_table;


data _null_;
set slow_chg.table_list end=last;

call symput(compress('table_name'||_n_),compress(table_name_in_aws_staging));
call symput(compress('table_name_o'||_n_),compress(table_name_in_aws_archive));

if last then call symput('num_tables',_n_);
run;

%put &table_name1.;

%do i=1 %to &num_tables.;

data _null_;

create_table_command="create table &schema_cdb_archive..&&table_name_o&i.._&today_yyyymmdd as
					 select *,'&today_aws.' as asofdate
                     from &schema_cdb..&&table_name&i.;";
call symput('create_table_command',strip(create_table_command));
run;



		proc sql noerrorstop noprint;
			connect using my_cdb;
			execute (drop table if exists &schema_cdb_archive..&&table_name_o&i.._&today_yyyymmdd;) by my_cdb;
			execute (&create_table_command.;) by my_cdb;
	   quit;

%end;
%mend archive_table;

%archive_table;
