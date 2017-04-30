
*%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";

*%let dateMinus2_mmddyy=%sysfunc(putn(%eval(%sysfunc(today())-2),mmddyyn6.));
*************ADD Comment to program.
*************;


options obs=max compress=yes reuse=yes fmtsearch=(work table ) ls=64 ps=79
	mprint symbolgen ORIENTATION=portrait papersize=letter ;
%let wsjplus=work;

proc sql;

CREATE TABLE prov_ent
AS
   SELECT t1.UU_ID&HASH, t1.identity_uuid&hash,
          t1.legacy_uuid, t1.add_ent_dt_tm, t1.remove_ent_dt_tm,
          case when t1.ent_name not in ( 'WSJ-PLUS', 'WSJ-PLUS-OPTIN', 'WSJ-PLUS-OPTIN-EMEA-APAC') then r.feature_name
                else t1.ent_name end as ent_name,
		  t1.ent_name as test_ent_name
   FROM
       stg.PROV_IDENTITY_PRIM_ENT_MQT t1 left outer join
       stg.prov_entitlement_feature_ref r
   ON
       t1.ent_name = r.ent_name
   and r.FEATURE_NAME IN ( 'WSJ-PLUS', 'WSJ-PLUS-OPTIN', 'WSJ-PLUS-OPTIN-EMEA-APAC')
    
   WHERE
      ( t1.ent_name IN ( 'WSJ-PLUS', 'WSJ-PLUS-OPTIN', 'WSJ-PLUS-OPTIN-EMEA-APAC')
    OR r.FEATURE_NAME IN ( 'WSJ-PLUS', 'WSJ-PLUS-OPTIN', 'WSJ-PLUS-OPTIN-EMEA-APAC'));
quit;

data prov_ent;
     set prov_ent;
   /* Code modified in Sep 16 to consider WSJ-PLUS-OPTIN-EMEA-APAC as not opt_in_ent */
   /* If ent_name in ('WSJ-PLUS-OPTIN', 'WSJ-PLUS-OPTIN-EMEA-APAC') then */

    If ent_name in ('WSJ-PLUS-OPTIN') 
	then
      opt_in_ent = 1;
    else
      opt_in_ent = 0;

   if ent_name in ('WSJ-PLUS-OPTIN') then us_opt_in_ent = 1 ; else us_opt_in_ent = 0;
   if ent_name in ('WSJ-PLUS-OPTIN-EMEA-APAC') then ea_opt_in_ent = 1 ; else ea_opt_in_ent = 0;

run;

data &wsjplus..uu_id_with_wsjplus;
   set prov_ent;
   where ent_name in ( 'WSJ-PLUS' );

   wsjplus_ent_exist = 1;

run;


proc sort data = prov_ent;
    by UU_ID&HASH descending opt_in_ent descending us_opt_in_ent descending ea_opt_in_ent descending add_ent_dt_tm;
run;

proc sort data=prov_ent nodupkey;
   by UU_ID&HASH;
run;

data prov_ent ( KEEP = UU_ID&HASH identity_uuid&hash LEGACY_UUID
                               ENT_NAME add_ent_dt 
							   opt_in_ent us_opt_in_ent ea_opt_in_ent );
   set prov_ent;
   
   newval = put(datepart(add_ent_dt_tm),yymmddd10.);
   
   add_ent_dt = input(newval,anydtdte10.);
   format add_ent_dt date9.;

run;

proc sort data=&wsjplus..uu_id_with_wsjplus nodupkey;
    by UU_ID&HASH;
 run;

/* this is to make sure that all subs reported has WSJPLUS entitlement */

 proc sql;

 CREATE TABLE &wsjplus..prov_ent
 AS
 SELECT m.*, d.wsjplus_ent_exist
 FROM
     prov_ent m,
     &wsjplus..uu_id_with_wsjplus d
 WHERE
     m.UU_ID&HASH = d.UU_ID&HASH;

 quit;


/****************************************************************************/

data &wsjplus..prov_customer_login;
    set stg.PROV_CUSTOMER_LOGIN_VW ( keep = vxid identity_uuid&hash client_org_id );
	where vxid <> ' ';
run;

proc sort data =&wsjplus..prov_customer_login nodupkey;
   by vxid ;
run;

/* wsj plus clock data */

data &wsjplus..clock_data;
  set stg.wsjplus_clock;
  where vxid ^= ' '
  and upcase(opt_in) = 'TRUE';
run;

/* Added on 09/21/2016 to get the latest clock opt in date also */
/*****************************************************************/

proc sort data = &wsjplus..clock_data out=&wsjplus..clock_data_tmp;
  by vxid descending opt_in_tm;
run;

proc sort data=&wsjplus..clock_data_tmp nodupkey;
  by vxid;
run;

proc sort data = &wsjplus..clock_data;
  by vxid opt_in_tm;
run;

proc sort data=&wsjplus..clock_data nodupkey;
  by vxid;
run;

/* get the latest optin and minoptin */

proc sql;

create table &wsjplus..clock_data_op
as
   select t1.*, t2.opt_in_tm as latest_clock_opt_in_dt
   from
       &wsjplus..clock_data t1,
       &wsjplus..clock_data_tmp t2
   where
       t1.vxid = t2.vxid;

quit;

proc sql;

create table &wsjplus..clock_optin
as
  select t1.*, t2.identity_uuid&hash
  from
       &wsjplus..clock_data_op t1 left outer join
	   &wsjplus..prov_customer_login t2
  on t1.vxid = t2.vxid;
  
  /* and t2.vxid = '0000035899cb04c49bf878db8be5c093f065470f688107c80d7d5a3e26f27168' */

quit;

data &wsjplus..clock_optin ( KEEP = VXID identity_uuid&hash clock_opt_in_date CLOCK_OPT_IN LATEST_CLOCK_OPT_IN_DT);
  set &wsjplus..clock_optin;
  where identity_uuid&hash not in (' ',"&hash_null");
   
  newval = put(datepart(opt_in_tm),yymmddd10.);
   
  clock_opt_in_date = input(newval,anydtdte10.);
  format clock_opt_in_date date9.;

  CLOCK_OPT_IN = 'Y';

run;


/* sort and merge with uu_id_map table for wsjplus omniture data */

/* the table mktuser.WSJPLUS_OMNITURE_UUID_MAP is created from Linux due to
performance issues 
*/

/* Below is the code for performance check  */


	data wsjplus_omn_login;
	  set stg.TMP_WSJPLUS_OMNITURE;
	run;
	
proc sort data=stg.TMP_WSJPLUS_OMNITURE out=wsjplus_omn_login nodupkey;
   by vxid_encrypt;
run;


proc sql;

	connect to sasiorst as x1(server=&rs_server port=&rs_port
	user=&rs_user password=&rs_pw database=&rs_db);

	   create table prov_id_map as
		select *
		from connection to x1
		(
			SELECT
			   om.vxid_encrypt, pv.IDENTITY_UUID&hash, pm.uu_id&hash
			
			FROM
			    stg.PROV_CUSTOMER_LOGIN_VW pv,
			    stg.PROV_IDENTITY_MQT pm,
			    (select vxid_encrypt
			     from (select vxid_encrypt, row_number() over (partition by vxid_encrypt) as rank
				  from stg.TMP_WSJPLUS_OMNITURE
				  ) rk
				  where rk.rank=1
			     ) om
			WHERE
			     om.vxid_encrypt = pv.VXID AND
			     pv.IDENTITY_UUID&hash = pm.identity_uuid&hash
		) ;

	CREATE TABLE loginvw_map
	AS
		SELECT
		  om.vxid_encrypt, cv.UU_ID&hash
		FROM
		   stg.CUSTOMER_LOGIN_VW (keep=UU_ID_CNVRT
		   				               UU_ID&hash) cv,
		   wsjplus_omn_login (keep=vxid_encrypt) om
		WHERE
		   om.vxid_encrypt = cv.UU_ID_CNVRT;
quit;

data &wsjplus..WSJPLUS_OMNITURE_UUID_MAP;
   set prov_id_map loginvw_map;
run;

/*
data &wsjplus..WSJPLUS_OMNITURE_UUID_MAP;
   set mktuser.WSJPLUS_OMNITURE_UUID_MAP;
run;
*/

/*****************************************************/

/******************** END of perf check **********************/

/* remove duplicate on vxid_encrypt from the mapping table */

/*************** mktuser.WSJPLUS_OMNITURE_UUID_MAP is loaded using unix script for now **************/

/**** Omniture ******/

data &wsjplus..omniture_data;
  set stg.tmp_wsjplus_omniture;
  /* rename vxid_encrypt = vxid; */
run;


proc sort data=&wsjplus..WSJPLUS_OMNITURE_UUID_MAP nodupkey;
   by vxid_encrypt;
run;

proc sort data=&wsjplus..omniture_data;
   by vxid_encrypt;
run;

data &wsjplus..omniture_op_uuid;

   MERGE &wsjplus..omniture_data ( in = in1 )
         &wsjplus..wsjplus_omniture_uuid_map (in = in2 );
   by vxid_encrypt;

   If in1;

run;

data &wsjplus..omniture_op_uuid;
   set &wsjplus..omniture_op_uuid;

   if UU_ID&HASH in ("","&hash_null") then UU_ID&HASH = vxid_encrypt;
run;


proc sql;

create table &wsjplus..omniture_op
as
select UU_ID&HASH,
       min(access_dt) as FIRST_ACCESS_DT,
       max(access_dt) as LATEST_ACCESS_DT,
	   sum(visits) as tot_num_visits,
	   sum(unique_visitors) as tot_unique_visitors,
	   sum(pageviews) as tot_pageviews
from
    &wsjplus..omniture_op_uuid
group by UU_ID&HASH;
     
quit;

/* 
data &wsjplus..omniture_op;
  set &wsjplus..omniture_op;
  where vxid_encrypt <> ' ';
run;

*/

/* merge data and get the UU_ID&HASH */

/*

/* testing */

/*
data &wsjplus..omniture_op_uuid;
  set &wsjplus..omniture_op_uuid;
  if UU_ID&HASH = ' ' then UU_ID&HASH = vxid_encrypt;
run;
*/
/*
data test;
   set &wsjplus..omniture_op_uuid;
   where UU_ID&HASH = ' ';
run;
*/

 data &wsjplus..ref_country;
   set stg.ref_country;
   where edw_row_stat_cd = 'A';
 run;

proc sql;

create table &wsjplus..ref_country
AS
select iso_cntry_name, olf_cntry_name, alpha_cd2, rf.ALPHA_CD3,
       rf.OLF_CNTRY_CD, cd.RPT_REGION, edw_srce_sys_cd
from  
stg.ref_country rf left outer join
stg.country_dim cd
on
upper(rf.ISO_CNTRY_NAME) = upper(cd.COUNTRY_NAME)
and edw_row_stat_cd = 'A';

quit;

proc sql;

create table
    &wsjplus..ref_zip_country
as
SELECT
    z1.zip, z1.city, z1.state, z1.dma, c1.country_name
from
    stg.COUNTRY_DIM c1 left outer join
    stg.zip_dim z1
on  c1.COUNTRY_ID = z1.COUNTRY_ID   
and z1.ZIP_ID is not null;

quit;

data &wsjplus..dma_data;
   set slow_chg.dma_052716;
   where zip_code is not missing;
run;

data &wsjplus..dma_data;
   set &wsjplus..dma_data;
   
   length zip_code_var $5.;
   zip_code_var = put(zip_code, z5.);

run; 
proc sort data=&wsjplus..dma_data dupout = &wsjplus..dup_dma nodupkey;
  by zip_code_var;
run;

	