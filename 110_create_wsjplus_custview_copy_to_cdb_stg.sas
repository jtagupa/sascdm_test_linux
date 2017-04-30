options sascmd='/opt/sas/sashome/SASFoundation/9.4/bin/sas_u8 -nosyntaxcheck'
autosignon;

%SYSLPUT user=&user /remote=custview;
%SYSLPUT root=%bquote(&root)/remote=custview;
%SYSLPUT env=&env/remote=custview;
%SYSLPUT project=&project/remote=custview;
%SYSLPUT schema=&schema/remote=custview;
%SYSLPUT schema_cdb=&schema_cdb/remote=custview;
%SYSLPUT schema_cdb_archive=&schema_cdb_archive/remote=custview;
%SYSLPUT date_run=&date_run/remote=custview;
%SYSLPUT config20=&config_20/remote=custview;
%SYSLPUT config50=&config_50/remote=custview;


%SYSLPUT user=&user/remote=wsjplus;
%SYSLPUT root=%bquote(&root)/remote=wsjplus;
%SYSLPUT env=&env/remote=wsjplus;
%SYSLPUT project=&project/remote=wsjplus;
%SYSLPUT schema=&schema/remote=wsjplus;
%SYSLPUT schema_cdb=&schema_cdb/remote=wsjplus;
%SYSLPUT schema_cdb_archive=&schema_cdb_archive/remote=wsjplus;
%SYSLPUT date_run=&date_run/remote=wsjplus;
%SYSLPUT config20=&config_20/remote=wsjplus;
%SYSLPUT config50=&config_50/remote=wsjplus;

%SYSLPUT user=&user/remote=cdbcopy;
%SYSLPUT root=%bquote(&root)/remote=cdbcopy;
%SYSLPUT env=&env/remote=cdbcopy;
%SYSLPUT project=&project/remote=cdbcopy;
%SYSLPUT schema=&schema/remote=cdbcopy;
%SYSLPUT schema_cdb=&schema_cdb/remote=cdbcopy;
%SYSLPUT schema_cdb_archive=&schema_cdb_archive/remote=cdbcopy;
%SYSLPUT date_run=&date_run/remote=cdbcopy;
%SYSLPUT config20=&config_20/remote=cdbcopy;
%SYSLPUT config50=&config_50/remote=cdbcopy;

%SYSLPUT user=&user /remote=formers;
%SYSLPUT root=%bquote(&root)/remote=formers;
%SYSLPUT env=&env/remote=formers;
%SYSLPUT project=&project/remote=formers;
%SYSLPUT schema=&schema/remote=formers;
%SYSLPUT schema_cdb=&schema_cdb/remote=formers;
%SYSLPUT schema_cdb_archive=&schema_cdb_archive/remote=formers;
%SYSLPUT date_run=&date_run/remote=formers;
%SYSLPUT config20=&config_20/remote=formers;
%SYSLPUT config50=&config_50/remote=formers;


rsubmit custview wait=no sysrputsync=yes;


&config20;
&config50;

%include "&root./&user./&env./&project./sas-code/175_create_cust_view_active_final.sas";


%sysrput pathcust=%sysfunc(pathname(work)); 
endrsubmit;




rsubmit wsjplus wait=no sysrputsync=yes;


&config20;
&config50;

%include "&root./&user./&env./&project./sas-code/120_get_initial_data6.sas";
%include "&root./&user./&env./&project./sas-code/130_get_mosaic_eligible.sas";
%include "&root./&user./&env./&project./sas-code/140_olf_eligible.sas";
%include "&root./&user./&env./&project./sas-code/150_get_apac_actives.sas";
%include "&root./&user./&env./&project./sas-code/160_get_emea_subs.sas";
%include "&root./&user./&env./&project./sas-code/165_get_inapp_eligible.sas";
%include "&root./&user./&env./&project./sas-code/170_get_eligible_group_access.sas";
%include "&root./&user./&env./&project./sas-code/180_combine_data.sas";


%sysrput xpathwsjplus=%sysfunc(pathname(work)); 
endrsubmit;



rsubmit cdbcopy wait=no sysrputsync=yes;


&config20;
&config50;

%include "&root./&user./&env./&project./sas-code/215_create_tmp_cdb_tables.sas";
%create_tmp_tables(my_cdb,&schema_cdb,z,Y); 
%include "&root./&user./&env./&project./sas-code/220_copy_stg_2_cdb.sas";

%sysrput xpathcdb=%sysfunc(pathname(work)); 
endrsubmit;



rsubmit formers wait=no sysrputsync=yes;


&config20;
&config50;

%include "&root./&user./&env./&project./sas-code/176_create_cust_view_formers_final.sas";
                                              


%sysrput zpathcust=%sysfunc(pathname(work)); 
endrsubmit;


LISTTASK _ALL_; 

waitfor _all_ cdbcopy signoff custview formers ; 



signoff cdbcopy; 
signoff wsjplus; 
signoff custview; 
signoff formers;

