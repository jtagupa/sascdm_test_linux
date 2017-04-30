* %let date_mmddyy=%sysfunc(putn(%eval(%sysfunc(&date_run.)),mmddyyn6.)); 

data &wsjplus..EMEA_Indiv;
  set stg.wsjplus_emea_indiv;

  length BUSINESS_OWNER $25.;

  BUSINESS_OWNER = 'CONSUMER EMEA';
  SOURCE_REGION = 'EMEA';
run;

data &wsjplus..EMEA_Corp;
  set stg.wsjplus_emea_corp;

  length BUSINESS_OWNER $25.;
  BUSINESS_OWNER = 'CORPORATE EMEA';

  SOURCE_REGION = 'EMEA';
run;

data &wsjplus..EMEA_Subs 
( drop = sbscr_term_strt_dt sbscr_term_end_dt first_sub_start_date sbscr_trmnt_dt);

  set &wsjplus..EMEA_Indiv &wsjplus..EMEA_Corp;

  first_sub_start_date_var = input(substr(strip(first_sub_start_date), 1, 10),anydtdte10.);
  format first_sub_start_date_var date9.;
  
  sbscr_term_strt_dt_var = input(substr(strip(sbscr_term_strt_dt), 1, 10),anydtdte10.);
  format sbscr_term_strt_dt_var date9.;
  
  sbscr_term_end_dt_var = input(substr(strip(sbscr_term_end_dt), 1, 10),anydtdte10.);
  format sbscr_term_end_dt_var date9.;
  
  sbscr_trmnt_dt_var = input(substr(strip(sbscr_trmnt_dt), 1, 10),anydtdte10.);
  format sbscr_trmnt_dt_var date9.;

  /*
  first_sub_start_date_var = input(substr(strip(first_sub_start_date), 1, 10),yymmddd10.);
  first_sub_start_date_var = input ( substr(strip(first_sub_start_date), 1, 10),MMDDYY10.);

    put first_sub_start_date_var = date9.;
   substr(strip(first_sub_start_date),1,10); */

run;


data &wsjplus..EMEA_Subs;
  
  set &wsjplus..EMEA_Subs;

  rename first_sub_start_date_var = first_sub_start_date;
  rename sbscr_term_strt_dt_var = sbscr_term_strt_dt;
  rename sbscr_term_end_dt_var = sbscr_term_end_dt;
  rename sbscr_trmnt_dt_var = sbscr_trmnt_dt;

run;

/* get all records from mcs which has a coupon code to match with 
   emea data and get the uu_id
*/

data &wsjplus..mcs_cpn_cd_data
(KEEP = uu_id uu_id_hash
		 LOGIN_NAME FIRST_NAME LAST_NAME SBSCR_ID SBSCR_STATE_CODE PROD_CD PROD_NAME AUTORENEW_IND
          SBSCR_CRE_DT SBSCR_TERM_ID SBSCR_TERM_STRT_DT	SBSCR_TERM_END_DT SBSCR_TRMNT_DT DUR_UNIT DUR_QTY	
          PURCH_AMT PURCH_CURRENCY BRAND_NAME PURCH_DT RENEWAL_DT SBSCR_GRP_ID	GRP_TYPE_NAME GRP_TYPE_CD
		  TRACK_CD	CHANNEL	CAMPAIGN_NAME CAMPAIGN_TYPE FREE_PAID_TYPE PHONE_NUM BUNDLE_NAME
          IS_MIGRATED IS_REACTIVATED EXTL_SYS_ID ICS_CUST_ID PRINT_ACCT_NUM PRINT_ACCT_NUM_hash COUPON_CD INIT_ORD_SOURCE_CD
          ICS_OOT BUSINESS_OWNER BUSINESS_OWNER_CL OFFER_TYPE	BILL_SYS_ID	EDW_OFFER_PRICE
          EDW_FIRST_OUT_IFP_DT INIT_ORD_CRE_DT SBSCR_STRT_DT
          FRE_ORDER_ID PYMT_INSTR_ID SBSCR_STATUS SBSCR_STATE_ID PRINT_PUB_CODE CIB_CUSTOMER_ID
	      TENURE_BY_CUST TENURE_BY_PROD CAMPAIGN_RENEWAL_CNT
		  SUBSCR_RENEWAL_CNT SRCE_KEY CAMPAIGN_MARKETING_PROGRAM DELVR_CALENDAR_NAME EMAIL_ADDR email_addr_hash
  );
     set stg.mosaic_cust_subscription;
	 where bill_sys_id = 1
	 and prod_cd = 'prod10004'
	 and coupon_cd is not missing;
	 /* and coupon_cd in ( 'DJCOMPNO-aaqcxqst', 'DJCOMPNO-aaqcxqwi','DJCOMPNO-aapwst79','DJCOMPNO-aapwsppd', 'DJCOMPMO-aaabnk3c');*/
run;

proc sql;
   
   CREATE TABLE &wsjplus..EMEA_data
   AS
   SELECT
        es.*,
		coalesce(es.coupon_code,es.mosaic_access_code) as der_coupon_code,
        coalesce(es.offer_type, mcs.offer_type) as der_offer_type,
		mcs.free_paid_type as MOSAIC_FREE_PAID_TYPE,
		coalesce(es.uu_id, mcs.uu_id) as der_uu_id,
        coalesce(es.uu_id_hash, mcs.uu_id_hash) as der_uu_id_hash,
		coalesce(coalesce(es.email,es.secondary_email), mcs.email_addr) as der_email_addr,
		coalesce(coalesce(es.email_hash,es.secondary_email_hash), mcs.email_addr_hash) as der_email_addr_hash,
        coalesce(es.first_name, mcs.first_name) as der_first_name,
        coalesce(es.last_name, mcs.last_name) as der_last_name
   FROM
       &wsjplus..EMEA_Subs es left outer join
       &wsjplus..mcs_cpn_cd_data mcs
   ON
       coalesce(es.coupon_code,es.mosaic_access_code) = mcs.coupon_cd;

quit;

data &wsjplus..emea_data;

   set &wsjplus..emea_data;
   where sbscr_trmnt_dt is missing;
   key_var = strip(der_uu_id) || strip (lowcase(der_email_addr)) || slx_contact_id;
   key_var_hash = strip(der_uu_id_hash) || strip ((der_email_addr_hash)) || slx_contact_id;
   

run;

data &wsjplus..emea_data;
   set &wsjplus..emea_data;
   where key_var_hash is not missing;
run;

proc sort data = &wsjplus..emea_data ;
  by key_var_hash descending sbscr_term_strt_dt;
run;

proc sort data=&wsjplus..emea_data nodupkey;
   by key_var_hash;
run;

data emea_ref_country;

    set &wsjplus..ref_country;
	where edw_srce_sys_cd = 'ISO'
	and ISO_CNTRY_NAME ^= ' ';

run;

proc sort data=emea_ref_country nodupkey;
   by ISO_CNTRY_NAME;
run;


proc sql;

CREATE TABLE &wsjplus..EMEA_subs_reg
AS
    SELECT
       wi.*, rc.ISO_CNTRY_NAME, rc.rpt_region
    FROM
       &wsjplus..emea_data wi LEFT JOIN
       emea_ref_country rc
    ON
        CASE WHEN wi.COUNTRY = 'RUSSIA' THEN 'RUSSIAN FEDERATION' 
        ELSE COALESCE(wi.COUNTRY, 'NA') 
        END = COALESCE(rc.ISO_CNTRY_NAME, 'NA2');

quit;

/* Map EMEA Feed to WSJPLUS data format */

data &wsjplus..emea_data_op
( KEEP = SOURCE
         UU_ID uu_id_hash
	     EMAIL_ADDR email_addr_hash 
		 SBSCR_ID ACCT_NUM acct_num_hash PROD_CD MOSAIC_PROD_CD 
         FIRST_NAME LAST_NAME INIT_ORD_CRE_DT SBSCR_TERM_STRT_DT SBSCR_TERM_END_DT SBSCR_TRMNT_DT
         SBSCR_STATUS BILL_SYS_ID IS_MIGRATED AR_FLAG 
         PAYMENT_TYPE OFFER_TYPE BUSINESS_OWNER BUSINESS_OWNER_CL SUBSCRIPTION_TYPE CHANNEL CHANNEL_2
         FREQUENCY_DESC OFFER_PRICE COMPANY_NAME COUPON_CD
		 SLX_CONTACT_ID	
         CITY POSTAL_CD COUNTRY RPT_REGION DMA 
		 FREE_PAID_TYPE WSJPLUS_ELIGIBLE TENURE

);
   set &wsjplus..EMEA_subs_reg;


   
   SOURCE = 'EMEA FEED';

   
   UU_ID = DER_UU_ID;
   uu_id_hash=der_uu_id_hash;
   EMAIL_ADDR = DER_EMAIL_ADDR;
   email_addr_hash=der_email_addr_hash;
   FIRST_NAME = DER_FIRST_NAME;
   LAST_NAME = DER_LAST_NAME;
   COUPON_CD = DER_COUPON_CODE;
   SBSCR_STATUS = 'A';
   COMPANY_NAME = ORGANIZATION_NAME;
   
   /*
   SBSCR_CRE_DT = FIRST_SUB_START_DATE;
   SBSCR_STRT_DT = FIRST_SUB_START_DATE;
   SBSCR_TERM = SUBSCRIPTION_TYPE;
   PURCH_AMT = ORDER_TOTAL_GROSS;
   FREE_PAID_TYPE = 'PAID';
   */

   IF FIRST_SUB_START_DATE ^= "" Then 
      INIT_ORD_CRE_DT = FIRST_SUB_START_DATE;
   ELSE
      INIT_ORD_CRE_DT = SBSCR_TERM_STRT_DT;

   format init_ord_cre_dt date9.;

   OFFER_PRICE = ORDER_TOTAL_NET;

   If OFFER_TYPE = 'Combi' Then OFFER_TYPE = 'DIGITAL PLUS PRINT' ;
   Else If OFFER_TYPE = 'Online Only' Then OFFER_TYPE = 'STANDALONE';
   Else OFFER_TYPE = DER_OFFER_TYPE;

   IF index(sales_ord_prod,'PRINT') > 1 Then mosaic_prod_cd = 'prod830009';
   Else mosaic_prod_cd = 'prod10004';

   prod_cd = 'J';

   SBSCR_STATUS = 'A';

   PRINT_STAT = 'EMEA';

   If ( business_owner = 'CONSUMER EMEA' ) Then subscription_type = 'Regular';
   Else If ( business_owner = 'CORPORATE EMEA' ) Then subscription_type = 'Corporate';
   Else subscription_type = 'OTHER';

   FREE_PAID_TYPE = 'PAID';

   todaydt = (&date_run.);
   format todaydt date9.;
    
   TENURE=ROUND( (intck('month',INIT_ORD_CRE_DT,todaydt)/12), 0.01) ;

   WSJPLUS_ELIGIBLE = 1;
   
run;


proc sql;

CREATE TABLE WP_OP.emea_eligible_subs_bck_&date_mmddyy
AS
SELECT * from &wsjplus..emea_data_op
where
    uu_id_hash not in ( select uu_id_hash from &wsjplus..mosaic_subs    );

				 /*
	               where business_owner = 'CONSUMER EMEA'
				 );
				 */

quit;

data &wsjplus..emea_eligible_subs;
   set WP_OP.emea_eligible_subs_bck_&date_mmddyy;
run;
/*
( select uu_id from WP_OP.mosaic_eligible_wsjplus_&date_mmddyy
	               where business_owner = 'CONSUMER EMEA'
				 );
*/





