
/* this job has a dependency on getting the MOSAIC data 
   from the job get_mosaic_eligible.sas to 
   exclude the APAC subs available in feed and mosaic system
*/

/* load the APAC tables in MKTUSER 
   /home/unica/app/Affinium/Campaign/partitions/partition1/scripts/developer/sivaramanr/WSJPLUS/scripts
*/

*%let date_mmddyy=%sysfunc(putn(%eval(%sysfunc(today())),mmddyyn6.));
%let wsjplus=work;

data &wsjplus..wsjonline_mcs;
  set stg.MOSAIC_CUST_SUBSCRIPTION ( keep = uu_id_hash uu_id email_addr_hash email_addr first_name last_name coupon_cd 
                                             offer_type business_owner free_paid_type sbscr_status sbscr_term_strt_dt
                                             prod_cd bill_sys_id
                                   );
  where prod_cd = 'prod10004'
  and bill_sys_id = 1;
  
  /* where uu_id&hash in ('masubuti','2fc06f13-d797-4e0c-926f-6a39a9603911','2fde2151-dbc7-43f2-9a03-3896bd9ec37f'); */
run;

proc sort data = &wsjplus..wsjonline_mcs;
   by email_addr&hash sbscr_status descending sbscr_term_strt_dt;
run;

proc sort data = &wsjplus..wsjonline_mcs nodupkey;
   by email_addr&hash;
run;

proc sql;

CREATE TABLE &wsjplus..apac_data
AS
select ap.*, mcs.uu_id&hash as mosaic_uu_id, 
       coalesce(ap.cpn_cd, mcs.COUPON_CD) as COUPON_CD,
       mcs.BUSINESS_OWNER, 
       coalesce(ap.OFFER_TYPE, mcs.OFFER_TYPE) as MOSAIC_OFFER_TYPE, 
       coalesce(ap.uu_id, mcs.uu_id) as der_uu_id,
       coalesce(ap.uu_id_hash, mcs.uu_id_hash) as der_uu_id_hash,
       coalesce(ebm_email_addr_hash, mos_email_addr_hash) as der_email_addr_hash,
       coalesce(ebm_email_addr , mos_email_addr ) as der_email_addr,
	   coalesce(coalesce(ebm_first_name,mos_first_name),mcs.first_name) as first_name, 
	   coalesce(coalesce(ebm_last_name,mos_last_name),mcs.last_name) as last_name
from
   stg.WSJPLUS_APAC ap left outer join
   &wsjplus..wsjonline_mcs mcs
on coalesce(ap.ebm_EMAIL_ADDR&hash,'NA') = mcs.email_addr&hash and ap.ebm_email_addr&hash not in ("&hash_null",'')
and mcs.email_addr&hash not in ('',"&hash_null")
and mcs.prod_cd = 'prod10004';

/* Should we add business owner here - Some
of these have business owner as 'CONSUMER US' */

quit;

/* pull only active subs from APAC feed */

data &wsjplus..apac_data;
   set &wsjplus..apac_data;
   where sbscr_trmnt_dt is missing;
   key_var = strip(der_uu_id&hash) || strip (lowcase(der_email_addr&hash)) || strip(ebm_sbscr_id);
   SOURCE = 'APAC FEED';
run;

data &wsjplus..apac_data;
   set &wsjplus..apac_data;
   where key_var is not missing;
run;

proc sort data = &wsjplus..apac_data ;
  by key_var descending SBSCR_FIRST_START_DT;
run;

proc sort data=&wsjplus..apac_data nodupkey;
   by key_var;
run;

/* get country and region */

data apac_ref_country;

    set &wsjplus..ref_country;
	where edw_srce_sys_cd = 'ISO'
	and alpha_cd2 ^= ' ';

run;

proc sort data=apac_ref_country nodupkey;
   by alpha_cd2;
run;

proc sql;

CREATE TABLE &wsjplus..APAC_subs
AS
    SELECT wa.*, COALESCE(rc.ISO_CNTRY_NAME, wa.COUNTRY) AS RPT_COUNTRY, rc.RPT_REGION
    FROM
        &wsjplus..apac_data wa LEFT OUTER JOIN
        APAC_REF_COUNTRY rc
    ON  COALESCE(wa.COUNTRY,'NA') = COALESCE(rc.alpha_cd2,'NA2');
	/*
	UNION
    SELECT wa.*, COALESCE(rc.ISO_CNTRY_NAME, wa.COUNTRY) AS RPT_COUNTRY, rc.RPT_REGION
    FROM
        WSJPLUS.apac_data wa LEFT OUTER JOIN
        APAC_REF_COUNTRY rc
    ON  COALESCE(wa.COUNTRY,'NA') = COALESCE(rc.ISO_CNTRY_NAME,'NA2');
    */

quit;

/* check by iso_country instead of alpha_cd2 */

proc sql;

CREATE TABLE &wsjplus..APAC_subs_2
AS
    SELECT wa.*, coalesce(wa.rpt_region, rc.RPT_REGION) as RPT_REGION_2
    FROM
        &wsjplus..apac_subs wa LEFT OUTER JOIN
        APAC_REF_COUNTRY rc
    ON  COALESCE(wa.COUNTRY,'NA') = COALESCE(rc.ISO_CNTRY_NAME,'NA2');

quit;


/* Map APAC Feed to WSJPLUS data format */

data &wsjplus..apac_data_op
( KEEP = SOURCE
         uu_id_hash uu_id email_addr_hash email_addr SBSCR_ID acct_num_hash acct_num PROD_CD MOSAIC_PROD_CD 
         FIRST_NAME LAST_NAME INIT_ORD_CRE_DT SBSCR_TERM_STRT_DT SBSCR_TERM_END_DT SBSCR_TRMNT_DT
         SBSCR_STATUS BILL_SYS_ID IS_MIGRATED AR_FLAG 
         PAYMENT_TYPE OFFER_TYPE BUSINESS_OWNER SUBSCRIPTION_TYPE CHANNEL CHANNEL_2
         FREQUENCY_DESC OFFER_PRICE COMPANY_NAME COUPON_CD
         EBM_SBSCR_ID 
         CITY POSTAL_CD COUNTRY PURCH_CURRENCY RPT_REGION
		 PRINT_STAT FREE_PAID_TYPE WSJPLUS_ELIGIBLE TENURE
         );


		 /* EBM_CITY EBM_PROVINCE EBM_POSTAL_CD EBM_COUNTRY  */

    length uu_id_hash      		 $100.;
	length email_addr_hash 		 $100.;
	length acct_num_hash   		 $120.;
    length uu_id       		 $200.;
	length email_addr  		 $200.;
	length acct_num    		 $200.;
	length PROD_CD    		 $1.;
    length MOSAIC_PROD_CD 	 $25.;
	length FIRST_NAME 		 $200.;
	length LAST_NAME 		 $200.;
	length SBSCR_STATUS 	 $1.;
	length IS_MIGRATED 		 $1.;
	length AR_FLAG 			 $10.;
	length PAYMENT_TYPE		 $40.;
	length OFFER_TYPE		 $40.;
	length BUSINESS_OWNER	 $40.;
	length BUSINESS_OWNER_CL $40.;	
	length SUBSCRIPTION_TYPE $40.;	
	length CHANNEL			 $40.;
	length CHANNEL_2		 $40.;
	length FREQUENCY_DESC	 $40.;
	length COMPANY_NAME		 $100.;
	
	length SOURCE			 $25.;
	length PRINT_STAT		 $40.;
	length PYMT_INSTR_ID     $100.;
	length PURCH_CURRENCY    $10.;

	length CITY 			 $200.;
	length POSTAL_CD 		 $12.;
	length COUNTRY 		 	 $100.;
	length PROVINCE          $100.;
	length COUPON_CD         $40.;
	length FREE_PAID_TYPE    $25.;

	
    set &wsjplus..APAC_subs_2;
         
   
    SOURCE = 'APAC FEED';
   
    uu_id_hash = DER_UU_ID_hash;
   
    email_addr_hash = DER_EMAIL_ADDR_hash;

    uu_id  = DER_UU_ID;
   
    email_addr  = DER_EMAIL_ADDR;

    RPT_REGION = RPT_REGION_2;

   INIT_ORD_CRE_DT = SBSCR_FIRST_START_DT;
   format INIT_ORD_CRE_DT date9.;

   COUPON_CD = CPN_CD;

   SBSCR_TERM = strip ( dur_qty ) || ' ' || strip (dur_unit);
   OFFER_PRICE = PURCH_AMT;
   OFFER_TYPE = MOSAIC_OFFER_TYPE;

   SBSCR_STATUS = 'A';

   PROD_CD = 'J';

   If ( sub_type = 'INDIVIDUAL' ) Then 
   do;
       business_owner = 'CONSUMER APAC';
	   subscription_type = 'Regular';
   end;
   Else
      if (sub_type = 'CORPORATE' ) Then   
      do;

         business_owner = 'CORPORATE APAC';
	     subscription_type = 'Corporate';
      end;
	  Else
	  do;
	     business_owner = 'OTHER';
	     subscription_type = 'OTHER';
	  end;

   PRINT_STAT = 'APAC';
   COMPANY_NAME = CORP_NAME;

   FREE_PAID_TYPE = 'PAID';

   WSJPLUS_ELIGIBLE = 1;

   todaydt = &date_run.;
   format todaydt date9.;
    
   TENURE=ROUND( (intck('month',INIT_ORD_CRE_DT,todaydt)/12), 0.01) ;
   
 run;  
   

/*********************************************************************

/** Preference changed to get the mosaic data if the uu_id&hash exists in Mosaic 
Change made - June  17 2016 

******/

proc sql;

CREATE TABLE WP_OP.apac_eligible_subs_bck_&date_mmddyy
AS
SELECT * from &WSJPLUS..apac_data_op
where
    uu_id&hash not in ( select uu_id&hash from &wsjplus..mosaic_subs
                   );

/*	where business_owner = 'CONSUMER APAC' */

quit;

data apac_eligible_subs;
  set WP_OP.apac_eligible_subs_bck_&date_mmddyy;
run;

/* 

( select uu_id&hash from WP_OP.mosaic_eligible_wsjplus_&date_mmddyy
	               where business_owner = 'CONSUMER APAC'
                   );

*/
