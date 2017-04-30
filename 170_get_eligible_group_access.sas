
*%let date_mmddyy=%sysfunc(putn(%eval(%sysfunc(today())),mmddyyn6.)); 


/* Mosaic table PROV_IDENTITY_PRIM_ENTITLEMENT used because mqt is more than 30 characters
   PROV_CLIENT_ORGANIZATION is used because the client organization mqt is missing some records 
Group access is only subs with 
*/

proc sql;

    create table &wsjplus..gp_access_eligible
	AS
	select   
        pi.uu_id_hash, pc.identity_uuid_hash,
        pi.uu_id , pc.identity_uuid,
        pc.USERNAME, pc.FIRST_NAME, pc.LAST_NAME,
        pc.EMAIL, pc.email_hash,
        pc.FIRST_ADD_ENTITLEMENT_TMSTMP AS ADD_ENT_DT_TM, pc.last_access_tmstmp,
        pc.CLIENT_ORGANIZATION_ID, CLIENT_ORGANIZATION_NAME, BUSINESS_OWNER,
        FEATURE_NAME
    from
        stg.PROV_CLIENT_ORGANIZATION_MQT pc,
        stg.PROV_IDENTITY_MQT pi
    where
        pc.identity_uuid&hash = pi.identity_uuid&hash
    and FEATURE_NAME = 'WSJ-PLUS' ;  

     /* and coalesce(business_owner, 'UNKNOWN') IN ( 'UNKNOWN', 'CONSUMER_US', 'CORPORATE_EMEA', 'CORPORATE_US'); */

	/* getting all records for WSJ feature first and then will be restricted to WSJ-PLUS  at the end */

quit;
/*
data wsjplus.gp_access_eligible;
   set wsjplus.gp_access_eligible;
   where email <> ' ';
run;
*/

proc sort data=&wsjplus..gp_access_eligible;
  by uu_id&hash descending ADD_ENT_DT_TM;
run;

proc sort data=&wsjplus..gp_access_eligible nodupkey;
  by uu_id&hash;
run;


data &wsjplus..gp_access_eligible;

    length uu_id      		 $200.;
	length email_addr  		 $200.;
	length acct_num   		 $200.;

  length uu_id_hash      		 $100.;
	length email_addr_hash 		 $100.;
	length acct_num_hash   		 $120.;

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
	length COUPON_CD		 $40.;
	length SOURCE			 $25.;
	length PRINT_STAT		 $40.;
	length PYMT_INSTR_ID     $100.;
	length PURCH_CURRENCY    $10.;

  set &wsjplus..gp_access_eligible;

  SOURCE = 'GROUP ACCESS';

  SBSCR_STATUS = 'A';

  email_addr_hash = email_hash;
  email_addr = email;
  newval = put(datepart(add_ent_dt_tm),yymmddd10.);
   
  init_ord_cre_dt = input(newval,anydtdte10.);
  format init_ord_cre_dt date9.;

  if BUSINESS_OWNER= "CONSUMER_US" then subscription_Type = "Regular";
  else if BUSINESS_OWNER IN ( "CORPORATE_EMEA", "CORPORATE_US", "CORPORATE_APAC" ) then subscription_Type = "Corporate";
  else if BUSINESS_OWNER= "EDUCATION" then subscription_Type = "Educational";
  else subscription_Type = "OTHER";

   COMPANY_NAME = CLIENT_ORGANIZATION_NAME;

   /* Excluded subs where business_owner is not populated on 10/31/2016 */

   IF ( business_owner in ( 'CONSUMER_US', 'CORPORATE_EMEA', 'CORPORATE_US', 'CORPORATE_APAC' , 'EDUCATION') 
       )
   Then
      WSJPLUS_ELIGIBLE = 1;
   ELSE
      WSJPLUS_ELIGIBLE = 0;


   todaydt = (&date_run.);
   format todaydt date9.;
    
   TENURE=ROUND( (intck('month',INIT_ORD_CRE_DT,todaydt)/12), 0.01) ;

  run;
  
 data WP_OP.gp_eligible_subs_bck_&date_mmddyy
( KEEP = uu_id_hash uu_id email_addr_hash email_addr SBSCR_ID acct_num acct_num_hash PROD_CD MOSAIC_PROD_CD 
         FIRST_NAME LAST_NAME INIT_ORD_CRE_DT SBSCR_TERM_STRT_DT SBSCR_TERM_END_DT SBSCR_TRMNT_DT
         SBSCR_STATUS BILL_SYS_ID IS_MIGRATED AR_FLAG 
         PAYMENT_TYPE OFFER_TYPE BUSINESS_OWNER BUSINESS_OWNER_CL SUBSCRIPTION_TYPE CHANNEL CHANNEL_2
         FREQUENCY_DESC OFFER_PRICE COMPANY_NAME COUPON_CD
         SOURCE PYMT_INSTR_ID PRINT_STAT WSJPLUS_ELIGIBLE TENURE); 


 set &wsjplus..gp_access_eligible;
 
 run;

data &wsjplus..gp_eligible_subs;
  set WP_OP.gp_eligible_subs_bck_&date_mmddyy;
run;
