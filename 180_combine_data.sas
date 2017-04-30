


/* WSJ PLUS eligible datasets

inapp - still in qn

olf - Olf_subs_&date_mmddyy
mosaic - Mosaic_subs_&date_mmddyy
emea - Emea_eligible_wsjplus_&date_mmddyy
apac - Apac_eligible_wsjplus_&date_mmddyy
gp - Gp_eligible_wsjplus_&date_mmddyy

*/

/* get delivery address for mosaic billed subs from olf account if it exists 
*/

proc sql;

CREATE TABLE &wsjplus..mcs_us_eligible_final_add
AS
SELECT
   mcs.*, olf.street_num, olf.addr, olf.addr2, olf.city, olf.state, olf.zip_5, olf.zip_plus_4,
          olf.postal_cd, olf.COUNTRY, olf.RPT_REGION
FROM
    &wsjplus..Mosaic_subs mcs left outer join
    &wsjplus..olf_data_all olf
ON
   mcs.acct_num&hash = olf.acct_num&hash;

quit;

data &wsjplus..combine_all;
  
  length WSJPLUS_subscription_type $25.;

  set &wsjplus..Olf_subs 
      &wsjplus..Mosaic_subs
      &wsjplus..Emea_eligible_subs 
      &wsjplus..Apac_eligible_subs
      &wsjplus..Gp_eligible_subs
	  &wsjplus..inapp_subs;


run;



data &wsjplus..combine_all;
   set &wsjplus..combine_all;

   If WSJPLUS_subscription_type = "" Then WSJPLUS_subscription_type = subscription_type;

   select (source);
     when ('OLF')  source_priority = 1;
     when ('MOSAIC')  source_priority = 2;
     when ('EMEA FEED')  source_priority = 3;
     when ('APAC FEED')  source_priority = 4;
     when ('GROUP ACCESS')  source_priority = 5;
	 when ('INAPP') source_priority = 6;
     otherwise source_priority = 7;

   end;

run;

proc freq data=&wsjplus..combine_all;
tables source_priority;
run;



data &wsjplus..mcs_ref_country;

    set &wsjplus..ref_country;
	where edw_srce_sys_cd = 'ISO'
	and alpha_cd2 ^= ' ';

run;

proc sort data=&wsjplus..mcs_ref_country nodupkey;
   by alpha_cd2;
run;

proc sql;

CREATE TABLE &wsjplus..combine_all_2
AS
SELECT
   mcs.*, mbill.pymt_addr as pymt_street_num, 
          mbill.pymt_addr_line1 as pymt_addr, 
          pymt_addr_line2 as pymt_addr2, 
          mbill.city as pymt_city, 
          mbill.state as pymt_state,
		  mbill.usps_zip5 as pymt_zip_5,
		  mbill.usps_zip4 as pymt_zip_plus_4,
		  rc.iso_cntry_name as pymt_country,
		  rc.rpt_region as pymt_rpt_region
FROM
    &wsjplus..combine_all mcs left outer join
    stg.mosaic_cust_billing_address mbill
ON  mcs.pymt_instr_id = mbill.pymt_instr_id left outer join
    &wsjplus..mcs_ref_country rc
ON mbill.country = rc.alpha_cd2;

quit;

data mcs_combine_all_2_bkp;
set &wsjplus..combine_all_2;
run;

data &wsjplus..combine_all_2;
  set &wsjplus..combine_all_2;

  /*
  olf.street_num, olf.addr, olf.addr2, olf.city, olf.state, olf.zip_5, olf.zip_plus_4,
          olf.postal_cd, olf.COUNTRY, olf.DMA,  olf.RPT_REGION
  */

   if country = '' then
	    olf_addr = 0; else olf_addr = 1;

     if pymt_country = '' then 
        mosaic_addr = 0; else mosaic_addr = 1;

     if olf_addr = 0 and mosaic_addr = 1 then
	 do;

       street_num = pymt_street_num;
       addr = pymt_addr;
       addr2 = pymt_addr2;
       city = pymt_city;
       state = pymt_state;
       zip_5 = pymt_zip_5;
       zip_plus_4 = pymt_zip_plus_4;
       country = pymt_country;
       rpt_region = pymt_rpt_region;

   end; 

run;
/*

can be run in initial_set

data wsjplus.dma_data;
   set Keep3.dma_052716;
   where zip_code is not missing;
run;

data wsjplus.dma_data;
   set wsjplus.dma_data;
   
   length zip_code_var $5.;
   zip_code_var = put(zip_code, z5.);

run; 
proc sort data=wsjplus.dma_data dupout = wsjplus.dup_dma nodupkey;
  by zip_code_var;
run;


data wsjplus.combine_all_2 ( drop = dma );
   set wsjplus.combine_all_2;
run;
*/

proc sql;

 CREATE TABLE &wsjplus..combine_all_3
 AS
    SELECT 
       t1.*, t2.dma_name as dma, t2.dma_code, t2.cbs_code, t2.cbs_Name,
	   t2.Fips_County_Code, t2.County_Name as Fips_County_Name
    FROM
        &wsjplus..combine_all_2 t1 left outer join
	    &wsjplus..dma_data t2
	ON t1.zip_5 = t2.zip_code_var;
	
 quit;


data &wsjplus..combine_all_no_email;
   set &wsjplus..combine_all_3;
   where email_addr&hash in (' ',"&hash_null");
run;


data &wsjplus..combine_all_email;
   set &wsjplus..combine_all_3;
   where email_addr&hash not in (' ',"&hash_null");
run;

proc sort data = &wsjplus..combine_all_email;
  by email_addr&hash source_priority descending wsjplus_eligible descending init_ord_cre_dt;
run;


proc sort data = &wsjplus..combine_all_email dupout=&wsjplus..duplicate_email_recs nodupkey;
  by email_addr&hash;
run;

/* 40 000 deleted due to duplicate email address from different source */

data &wsjplus..wsjplus_eligible_subs;
  set &wsjplus..combine_all_email &wsjplus..combine_all_no_email;
run;


/********************** Merge with Clock and Omniture and Prov **************************************/
/* check get_initial_data.sas program - the dataset prov_ent is all wsj_plus and wsj_plus_optin entitlements */

proc sort data = &wsjplus..prov_ent nodupkey;
   by uu_id&hash;
run;

/* reg data */

data &wsjplus..prov_reg;
   set stg.prov_reg_data ( keep = uu_id&hash register_dt rename=(register_dt=register_dt_old));
   where uu_id&hash not in ( ' ',"&hash_null");
register_dt=mdy(substr(register_dt_old,4,2),substr(register_dt_old,1,2),substr(register_dt_old,7,4));
format register_dt mmddyy10.;
DROP REGISTER_DT_OLD;
run;

proc sort data = &wsjplus..prov_reg nodupkey;
   by uu_id&hash;
run;

data &wsjplus..prov_ent;
  set &wsjplus..prov_ent;
  where uu_id&hash NOT IN (' ',"&HASH_NULL");
run;

proc sql;

create table &wsjplus..wsjplus_eligible_subs_1
as
   select 
       m1.*, pe.ENT_NAME, opt_in_ent, us_opt_in_ent,
	   ea_opt_in_ent, pe.add_ent_dt as wsjplus_add_ent_dt,
       pr.register_dt as registration_date,
	   coalesce(pe.wsjplus_ent_exist, 0) as wsjplus_ent_exist
   from
        &wsjplus..wsjplus_eligible_subs m1 left outer join
        &wsjplus..prov_ent pe
    on  m1.uu_id&hash = pe.uu_id&hash left outer join
		&wsjplus..prov_reg pr
	on  m1.uu_id&hash = pr.uu_id&hash;

 quit;
 

proc sort data = &wsjplus..clock_optin nodupkey;
   by identity_uuid&hash;
run;


proc sort data = &wsjplus..omniture_op_uuid dupout=om_test2 nodupkey;
  by uu_id&hash;
run;

options MISSING=" ";

/* get identity_uuid&hash for all subs in wsjplus dataset */

proc sql;

create table &wsjplus..wsjplus_identity_data
as
select m1.*, im.identity_uuid_hash,im.identity_uuid
from
    &wsjplus..wsjplus_eligible_subs_1 m1 LEFT OUTER JOIN
    stg.prov_identity_mqt im
ON  m1.uu_id&hash = im.uu_id&hash; 
    
quit;

proc sql;

create table &wsjplus..wsjplus_eligible_subs_2
as
   select 
       m1.*, co.clock_opt_in_date, co.CLOCK_OPT_IN, 
	   om.FIRST_ACCESS_DT, om.LATEST_ACCESS_DT, om.tot_num_visits,
	   om.tot_unique_visitors, tot_pageviews
   from
        &wsjplus..wsjplus_identity_data m1 left outer join
		&wsjplus..clock_optin co
      on m1.identity_uuid&hash = co.identity_uuid&hash left outer join
        &wsjplus..omniture_op om
	  on m1.uu_id&hash = om.uu_id&hash;

 quit;


/* changes on 09/09/2016 to add uu_id_cnvrt ( login_encrypt ) 
   This needs to be pulled again from customer_login_vw because the
   earlier query from omniture gets the login_encrypt only
 if there is a login 
 */

 /* get uu_id_cnvrt 

data wsjplus.wsjplus_subs_2_test;
   set wsjplus.wsjplus_eligible_subs_2;
   where uu_id&hash in (
   '0003124a-3dee-4044-afef-b39bf61764c7', '00076b53-8e67-47cb-aeb2-cdd71e9d03c7', '00287c01-1dd2-11b2-806d-a9797d62416d', 
   '00bdc109-5aef-4088-a433-df8007dfb03c', '00089926-2ed7-42a9-a8f6-1ec938bcf711','00337868-efdb-47dd-bbe6-659364293374',
   '001d1431-58f1-4b46-86cf-b6052123af87', '009d453a-b06e-41bf-beed-1160144a128c','0078ad1d-fc03-4a35-acae-d4338df679da',
   '00419ec1-e0d1-411b-9a24-93852b159987','0031b10c-b51d-4716-8d8c-2ba686d85338','001d749e-36c2-47f5-a434-5809f3a0d2a7',
   '000363db-bc17-477d-a6be-99218a36af8c','00057af3-154f-403c-a3db-726a6a34151d','00057d5c-51ad-4a5c-b5a7-e4821506e27b',
   '000150e5-a52a-40b7-a340-176351afb692','0003ae3c-eade-4a36-88e2-9e8e59d5bff3','001dd15a-04c4-4b81-b247-d5d90902f3e0'
   );
run;

 *********/

/********************************************************************************************************************************/
/* COMMENTED TEMPORARY
proc sql;

create table wsjplus_eligible_subs_3
as
  select m.*, d.uu_id_cnvrt
  from
       wsjplus.wsjplus_eligible_subs_2 m left outer join
	   circ.customer_login_vw d
  on 
       m.uu_id&hash = d.uu_id&hash;

quit;

proc sql;

create table wsjplus.wsjplus_eligible_subs_4
as
  select m.*, d.client_org_id, d.vxid
FROM 
    WSJPLUS_ELIGIBLE_SUBS_3 m left outer join
    circ.PROV_CUSTOMER_LOGIN_VW d
ON  m.identity_uuid&hash = d.identity_uuid&hash;

quit;

********************************** END OF TEMPORARY ************/

data &wsjplus..wsjplus_eligible_subs_4;
  set &wsjplus..wsjplus_eligible_subs_2;
run;

data &wsjplus..wsjplus_eligible_subs_4;

   set &wsjplus..wsjplus_eligible_subs_4;
   
   length ACTIVATED_30_DY_IND $1.;
   length ENGAGED_30_DY_IND $1.;
   length OPT_IN_TYPE $20.;

   format WSJPLUS_ACTIVATE_DT date9.;
   format clock_opt_in_date date9.;
   format first_access_dt date9.;

   _latest_access_dt  = input(scan(latest_access_dt,1,' '),ddmmyy10.);
   format _latest_access_dt date9.;

   length TENURE_YR 8;
   length TENURE 8;


   /* the first login date or the first clock opt in date -- WSJPLUS_FIRST_OPTIN_LOGIN_DT  */
   WSJPLUS_ACTIVATE_DT = MIN ( clock_opt_in_date, first_access_dt );

   /* whether the sub was activated with in the last 30 days  - logged in for first time or opted in first time */

   If WSJPLUS_ACTIVATE_DT = "" Then ACTIVATED_30_DY_IND = "";
   Else
       If (&date_run.) - WSJPLUS_ACTIVATE_DT <= 300   Then 
           ACTIVATED_30_DY_IND = 'Y' ;
       Else 
           ACTIVATED_30_DY_IND = 'N';

   /* whether the sub has logged in with in the last 30 days OR activated with in the last 30 days ENGAGED_30_DY_IND = */

   If clock_opt_in_date = "" AND _latest_access_dt = "" Then 
      ENGAGED_30_DY_IND = "";
   Else
      If ( (&date_run.) - clock_opt_in_date <= 300 )  OR ( (&date_run.) - _latest_access_dt <= 300 )
      Then 
         ENGAGED_30_DY_IND = 'Y';
      Else
         ENGAGED_30_DY_IND = 'N';

	If ( wsjplus_ent_exist = 1 and opt_in_ent = 1 )
    Then 
        OPT_IN_TYPE = "AUTO";
	Else
	   If ( wsjplus_ent_exist = 1 and opt_in_ent = 0 )
	   Then
	      If ( CLOCK_OPT_IN = "Y" OR WSJPLUS_ACTIVATE_DT ^= "" ) 
		  Then 
		     OPT_IN_TYPE = "MANUAL";
		  Else
		     OPT_IN_TYPE = "PENDING";
		Else
		   OPT_IN_TYPE = "";

	LOAD_DT = put((&date_run.),mmddyy10.);

	
    todaydt = (&date_run.);	
    TENURE_YR=intck('year',init_ord_cre_dt,todaydt, 'C');
	TENURE=intck('month',init_ord_cre_dt,todaydt, 'C');
    
	
run;

/****************************************************************************************************************************************/

/* COMPANY_NAME, pymnt_dma*/

data &wsjplus..wsjplus_eligible_subs_4 
( drop = init_ord_cre_dt SBSCR_TERM_STRT_DT SBSCR_TERM_END_DT SBSCR_TRMNT_DT wsjplus_add_ent_dt registration_date
         clock_opt_in_date first_access_dt _latest_access_dt
         wsjplus_activate_dt);

   set &wsjplus..wsjplus_eligible_subs_4;
   
   init_ord_cre_dt_var = put (init_ord_cre_dt, mmddyy10.);
   SBSCR_TERM_STRT_DT_var = put (SBSCR_TERM_STRT_DT, mmddyy10.);
   SBSCR_TERM_END_DT_var = put (SBSCR_TERM_END_DT, mmddyy10.);
   SBSCR_TRMNT_DT_var = put (SBSCR_TRMNT_DT, mmddyy10.);
   wsjplus_add_ent_dt_var = put (wsjplus_add_ent_dt, mmddyy10.);
   registration_date_var = put (registration_date, mmddyy10.);
   clock_opt_in_date_var = put (clock_opt_in_date, mmddyy10.);
   first_access_dt_var = put (first_access_dt, mmddyy10.);
   latest_access_dt_var = put (_latest_access_dt, mmddyy10.);
   wsjplus_activate_dt_var = put (wsjplus_activate_dt, mmddyy10.);

run;

/* Added WSJPLUS_subscription_type on 10/27/2016 */

proc sql;

create table WP_OP.wsjplus_data_&date_mmddyy
as 
SELECT

    uu_id_hash, identity_uuid_hash, 
    uu_id, identity_uuid,
    /*vxid, client_org_id, uu_id_cnvrt, */
    PROD_CD, acct_num&hash, acct_num, SBSCR_ID, FIRST_NAME, LAST_NAME, email_addr_hash,email_addr,
    STREET_NUM, ADDR, ADDR2, CITY, STATE, ZIP_5, ZIP_PLUS_4, POSTAL_CD,
	COUNTRY, RPT_REGION, DMA, CBS_CODE, CBS_NAME,
	FIPS_COUNTY_CODE, FIPS_COUNTY_NAME,
    AR_FLAG, CHANNEL, CHANNEL_2, PAYMENT_TYPE, FREQUENCY_DESC, OFFER_PRICE, 
    SUBSCRIPTION_TYPE, WSJPLUS_SUBSCRIPTION_TYPE, PRINT_STAT, MOSAIC_PROD_CD, OFFER_TYPE,
    SBSCR_STATUS, BILL_SYS_ID, IS_MIGRATED, BUSINESS_OWNER, COUPON_CD,
    PYMT_INSTR_ID, PYMT_STREET_NUM, PYMT_ADDR, PYMT_ADDR2, PYMT_CITY, PYMT_STATE,
    PYMT_ZIP_5, PYMT_ZIP_PLUS_4, PYMT_COUNTRY, PYMT_RPT_REGION, 
	INIT_ORD_CRE_DT_VAR, SBSCR_TERM_STRT_DT_VAR, SBSCR_TERM_END_DT_VAR, SBSCR_TRMNT_DT_VAR,
    WSJPLUS_ADD_ENT_DT_VAR, REGISTRATION_DATE_VAR, CLOCK_OPT_IN_DATE_VAR, FIRST_ACCESS_DT_VAR, 
    LATEST_ACCESS_DT_VAR,
	TOT_NUM_VISITS, TOT_UNIQUE_VISITORS, TOT_PAGEVIEWS,
    SOURCE, COMPANY_NAME, TENURE, TENURE_YR, 
    WSJPLUS_ELIGIBLE, FREE_PAID_TYPE,
	WSJPLUS_ENT_EXIST, ENT_NAME, CLOCK_OPT_IN, OPT_IN_TYPE,
    wsjplus_activate_dt_var, ACTIVATED_30_DY_IND, 
	ENGAGED_30_DY_IND, LOAD_DT
from &wsjplus..wsjplus_eligible_subs_4;

quit;


/* test insert directly to sas 
GIVING PERFROMANCE ISSUES
Needs to be verified

proc sql;

create table mktuser.wsjplus_load_data
as 
SELECT

    uu_id_hash, uu_id,PROD_CD, acct_num_hash, acct_num,SBSCR_ID, FIRST_NAME, LAST_NAME, email_addr_hash,email_addr,
    STREET_NUM, ADDR, ADDR2, CITY, STATE, ZIP_5, ZIP_PLUS_4, POSTAL_CD,
	COUNTRY, RPT_REGION, DMA,
    AR_FLAG, CHANNEL, CHANNEL_2, PAYMENT_TYPE, FREQUENCY_DESC, OFFER_PRICE, 
    SUBSCRIPTION_TYPE, PRINT_STAT, MOSAIC_PROD_CD, OFFER_TYPE,
    SBSCR_STATUS, BILL_SYS_ID, IS_MIGRATED, BUSINESS_OWNER, COUPON_CD,
    BUSINESS_OWNER_CL,
    PYMT_INSTR_ID, PYMT_STREET_NUM, PYMT_ADDR, PYMT_ADDR2, PYMT_CITY, PYMT_STATE,
    PYMT_ZIP_5, PYMT_ZIP_PLUS_4, PYMT_COUNTRY, PYMT_RPT_REGION, 
	INIT_ORD_CRE_DT_VAR, SBSCR_TERM_STRT_DT_VAR, SBSCR_TERM_END_DT_VAR, SBSCR_TRMNT_DT_VAR,
    WSJPLUS_ADD_ENT_DT_VAR, REGISTRATION_DATE_VAR, CLOCK_OPT_IN_DATE_VAR, FIRST_ACCESS_DT_VAR, 
    LATEST_ACCESS_DT_VAR,
	ENT_NAME, OPT_IN_ENT, US_OPT_IN_ENT, EA_OPT_IN_ENT, CLOCK_OPT_IN, 
    TOT_NUM_VISITS, TOT_UNIQUE_VISITORS, TOT_PAGEVIEWS,
	EBM_SBSCR_ID, EBM_CITY, EBM_PROVINCE, EBM_POSTAL_CD, EBM_COUNTRY,
    SLX_CONTACT_ID, SLX_POSTAL_CD, SLX_CITY, SLX_COUNTRY, SOURCE


from wsjplus.wsjplus_eligible_subs_2;

quit;

*/

