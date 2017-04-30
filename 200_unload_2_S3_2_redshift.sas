* This program will:
1) go through variables in the dataset, convert date variables to character type in yyyymmdd
2) create the specified table in Redshift stg schema
3) split the dataset into &numbe_of_slices
4) export all of the dataset into double quoted | delimited file
5) gzip them, copy to S3
6) load all the files in S3 into Redshift table by manifest
7) currently it takes about 5 mins to load 1.8 million combined data
;

*%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";

%include "&root./&user./&env./&project./redshift-code/create_transformed_tables.sas";

%macro split_n_export(numbe_of_slices, dataset_name, export_file_name, redshift_schema,  redshift_table_name);
	
	%let Process_start_timestamp = "%sysfunc(datetime(),datetime19.)"dt;
	
	proc contents data=&dataset_name out=the_content short;
	run;

	%let row_count_to_be_loaded=0;
	proc sql noprint;
		select count(*) into: row_count_to_be_loaded
		from &dataset_name;
	quit;

	data b;
		set the_content end=eof;
		format var_list tmp_var_list $1000. rename_vars rename_back_vars new_format time_var_list new_format_order new_format_data $3000.;

		retain rename_vars new_format var_list tmp_var_list rename_back_vars time_var_list new_format_order new_format_data;

		if _n_ = 1 then do;
			rename_vars = 'rename=(';
			new_format=' ';
			new_format_order='';
			new_format_data='';
			var_list='';
			tmp_var_list='';
			rename_back_vars='rename=(';
			time_var_list='';
		end;

		if index(upcase(format), 'DATE') and index(upcase(format), 'TIME') =0 then do;
			rename_vars= strip(rename_vars) || ' ' || strip(name) || '=t_' || strip(name);

			new_format = strip(new_format) || ' if t_' || compress(name) || ' ne . then do; ' || compress( strip(name) || '= strip(put(t_' || strip(name)) || ', yymmdd10.)); end;';
			new_format_data = strip(new_format_data) || ' ' || strip(name) || '= t_' || strip(name) || ';';;

			new_format_order=strip(new_format_order) || ' ' || strip(name) || ' date9.';

			tmp_var_list = strip(tmp_var_list) || ' t_' || strip(name);
			var_list = strip(var_list) || ' ' || strip(name);
			rename_back_vars = strip(rename_back_vars) || ' t_' || strip(name) || '=' || strip(name);
		end;

		if index(upcase(format), 'DATE') and index(upcase(format), 'TIME') then do;
			rename_vars= strip(rename_vars) || ' ' || strip(name) || '=t_' || strip(name);
			new_format = strip(new_format) || ' if t_' || compress(name) || ' ne . then do; ' || compress( strip(name) || '= strip(put(t_' || strip(name)) || ', redshift_dttm.)); end;';
			new_format_data = strip(new_format_data) || ' ' || strip(name) || '= t_' || strip(name) || ';';;

			new_format_order=strip(new_format_order) || ' ' || strip(name) || ' datetime25.6';
			tmp_var_list = strip(tmp_var_list) || ' t_' || strip(name);
			var_list = strip(var_list) || ' ' || strip(name);
			rename_back_vars = strip(rename_back_vars) || ' t_' || strip(name) || '=' || strip(name);
		end;

		if eof then do;
			call symput("rename_vars", strip(rename_vars) || ')');
			call symput("new_format", strip(new_format));
			call symput("var_list", strip(var_list));
			call symput("tmp_var_list", strip(tmp_var_list));
			call symput("rename_back_vars", strip(rename_back_vars) || ')');
			call symput("time_var_list", strip(time_var_list));
			call symput("new_format_order", strip(new_format_order));
			call symput("new_format_data", strip(new_format_data));
		end;
	run;
	%put new_format =&new_format;
	%put rename_vars=&rename_vars;
	%put var_list=&var_list;
	%put rename_back_vars=&rename_back_vars;
	%put time_var_list=&time_var_list;

	%if &var_list ne or &time_var_list ne %then %do;
		data new_dataset;
			set &dataset_name(&rename_vars);
			format &var_list &time_var_list $30.;

			&new_format;;
			drop &tmp_var_list;
		run;


		data new_dataset_order;
			set &dataset_name(&rename_vars obs=5);
			format &new_format_order;

			&new_format_data;

			drop &tmp_var_list;
		run;
	%end;
	%else %do;
		data new_dataset new_dataset_order;
			set &dataset_name;
		run;
	%end;

	%let count_rows=0;
 
 title1 "&dataset_name.  -->  &redshift_table_name.";
 proc contents data=new_dataset;
 run;
 title1;
 
 proc contents data=new_dataset  
               out=wsjtmp.&redshift_table_name.  noprint;
 run;
     
 	
	proc sql;
		select count(*) into: count_rows
		from new_dataset;
	quit;
	
		
	data no_of_files;
		if &count_rows <&numbe_of_slices then number_of_files=&count_rows;
		else number_of_files=&numbe_of_slices;
		
		call symput("number_of_files", strip(number_of_files));
	run;

	data %do ith=1 %to &number_of_files; &export_file_name._&ith  %end;;
		set new_dataset;
		
		%do jth=1 %to &number_of_files;;
			if mod(_n_, &number_of_files) = %eval(&jth -1) then do;	
				output &export_file_name._&jth;
			end;
		%end;
	run;
		
	%do mth =1 %to &number_of_files;
		
		options missing ='';
		filename expfile "&root./&user./&env./&project./tmp/&export_file_name._&mth..dlm" lrecl=50056; *LRECL bigger if needed;
		data _null_;
			set &export_file_name._&mth;
			file expfile dlm='|';
			put (_all_)(:);
			format _character_ $quote500.;
		run;

		x "cd &root./&user./&env./&project./tmp; rm -f &export_file_name._&mth..dlm.gz; gzip &export_file_name._&mth..dlm";;
	
		x "aws s3 cp &root./&user./&env./&project./tmp/&export_file_name._&mth..dlm.gz s3://&s3_bucket_cdb./&user./&env./&export_file_name._&mth..dlm.gz";;
		
		x "rm -f &root./&user./&env./&project./tmp/&export_file_name._&mth..dlm.gz";
	%end;
	
	filename manifest "&root./&user./&env./&project./tmp/&export_file_name..manifest" lrecl=500;
	data file_list;
		file manifest;
		put '{';
		put '    "entries": [';
		
		format a $500.;
				
		%do jth =1 %to &number_of_files;
			
			%if &jth ne &number_of_files %then %do;
				a= '        {"url":"' || "s3://&s3_bucket_cdb./&user./&env./&export_file_name._&jth..dlm.gz" || '", "mandatory":true},';
			%end;
			%else %do;
				a= '        {"url":"' || "s3://&s3_bucket_cdb./&user./&env./&export_file_name._&jth..dlm.gz" || '", "mandatory":true}';
			%end;
			put '       ' a;
		%end;
		put '    ]';
		put '}';
	run;

	x "aws s3 cp &root./&user./&env./&project./tmp/&export_file_name..manifest s3://&s3_bucket_cdb/&user./&env./&export_file_name..manifest";;
		
	data construct_copy_string;
		format copy_string $500.;
		copy_string = "copy " || strip("&redshift_schema..tmp_&redshift_table_name " || '0a'x ||
				"from 's3://&s3_bucket_cdb./&user./&env./&export_file_name..manifest'") || '0a'x ||
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
			   "gzip" || '0a'x ||
			   "manifest" || '0a'x ||
			   "maxerror 10";

		call symput ("copy_string", strip(copy_string));
	run;
		
	proc sql noerrorstop;
	   connect using my_cdb;
		execute (&copy_string) by my_cdb;
		disconnect from my_cdb;
	quit;
	
	%let Process_end_timestamp = "%sysfunc(datetime(),datetime19.)"dt;
	
	%do nth =1 %to &number_of_files;
		x "aws s3 rm s3://&s3_bucket_cdb./&user./&env./&export_file_name._&nth..dlm.gz";;
	%end;
	
	x "aws s3 rm s3://&s3_bucket_cdb./&user./&env./&export_file_name..manifest";;
	
	proc sql noerrorstop;
		connect using my_cdb;
		execute ("analyze &redshift_schema..tmp_&redshift_table_name;
			") by my_cdb;
		disconnect from my_cdb;
	quit;

	%let row_count_loaded=0;
	proc sql noprint;
		select count(*) into: row_count_loaded
		from cdb.tmp_&redshift_table_name;
	quit;
	
	data count_compare;
		format load_status $5.;

		load_status='bad';
		if &row_count_to_be_loaded=&row_count_loaded then load_status='good';

		call symput("load_status", strip(load_status));
	run;
	
	data wsjplus.stg_table_load_status_&date_mmddyy;
		set wsjplus.stg_table_load_status_&date_mmddyy;

		format Process_date date9. Process_end_timestamp Process_start_timestamp datetime19.;

		if upcase(table_name_in_aws_staging)=upcase("&redshift_table_name")
			and upcase(redshift_schema_name) = upcase("&cdb.") then do;
			
			load_status="&load_status";
			row_count_loaded=&row_count_loaded;
			row_count_in_control=&row_count_to_be_loaded;
			process_date=&today_date;
			process_end_timestamp=&process_end_timestamp;
			process_start_timestamp=&process_start_timestamp;

			time_taken_in_mins = round((process_end_timestamp - process_start_timestamp)/60, 0.01);
		end;
	run;
 

  
%mend split_n_export;	

*%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=test, export_file_name=test, redshift_schema=&schema_cdb, redshift_table_name=redshift_table_hist_test);


*%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=test, export_file_name=single_view, redshift_schema=&schema_cdb, redshift_table_name=single_view);

%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy., export_file_name=single_view, redshift_schema=&schema_cdb, redshift_table_name=single_view);
%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=WP_OP.wsjplus_data_&date_mmddyy, export_file_name=wsjplus_data, redshift_schema=&schema_cdb, redshift_table_name=wsjplus_data);
%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=wsjplus.omniture_&date_mmddyy, export_file_name=omniture, redshift_schema=&schema_cdb, redshift_table_name=omniture);
%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=wsjplus.address_&date_mmddyy, export_file_name=address, redshift_schema=&schema_cdb, redshift_table_name=address);
%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=wsjplus.registration_&date_mmddyy, export_file_name=registration, redshift_schema=&schema_cdb, redshift_table_name=registration);
%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=wsjplus.subscription_&date_mmddyy, export_file_name=subscription, redshift_schema=&schema_cdb, redshift_table_name=subscription);
%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=wsjplus.entitlement_&date_mmddyy, export_file_name=entitlement, redshift_schema=&schema_cdb, redshift_table_name=entitlement);


%split_n_export(numbe_of_slices=&numbe_of_slices_cdb, dataset_name=fmr.WSJIP_FORMER_&date_mmddyy._Custview, export_file_name=single_view_former, redshift_schema=&schema_cdb, redshift_table_name=single_view_former);  


