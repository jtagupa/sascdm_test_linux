
*%let date_mmddyy=%sysfunc(putn(%eval(%sysfunc(today())),mmddyyn6.));


proc sql;

create table inapp_eligible
as
   select pe.*, 
          PI.FIRST_NAME, PI.LAST_NAME, PI.EMAIL_hash, PI.EMAIL
   from
       stg.PROV_IDENTITY_MQT  PI,
       stg.PROV_IDENTITY_PRIM_ENT_MQT  PE
   where
       pi.uu_id&hash = pe.uu_id&hash
   and ENT_NAME IN ( 'APPLE-WSJ-DIGITALPLUS', 'GOOGLENS-WSJ-DIGITALPLUS', 
                        'AMAZON-PHONE-WSJ-DIGITALPLUS', 'AMAZON-WSJ-DIGITALPLUS', 'AMAZON-XP-WSJ-DIGITALPLUS', 
                        'SWA-WSJ-DIGITALPLUS');
run;

/* USE THE DATASET created in Initial data to determine
whether WSJPLUS ELIGIBLE OR NOT
*/

proc sql;

create table &wsjplus..inapp_eligible
as
   select m.*, d.wsjplus_ent_exist
   from
       inapp_eligible m left outer join 
       &wsjplus..prov_ent d
   on
       m.uu_id&hash = d.uu_id&hash;

run;
   
	
proc sort data=&wsjplus..inapp_eligible;
  by uu_id&hash descending ADD_ENT_DT_TM;
run;

proc sort data=&wsjplus..inapp_eligible nodupkey;
  by uu_id&hash;
run;

data &wsjplus..inapp_eligible;

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
	length COUPON_CD		 $40.;
	length SOURCE			 $25.;
	length PRINT_STAT		 $40.;
	length PYMT_INSTR_ID     $100.;
	length PURCH_CURRENCY    $10.;

  set &wsjplus..inapp_eligible;

  SOURCE = 'INAPP';

  PROD_CD = 'J';

  SBSCR_STATUS = 'A';

  email_addr_hash = email_hash;
  email_addr = email ;

  newval = put(datepart(add_ent_dt_tm),yymmddd10.);
   
  init_ord_cre_dt = input(newval,anydtdte10.);
  format init_ord_cre_dt date9.;

  subscription_Type = "INAPP";
  COMPANY_NAME = ENT_NAME;

   IF wsjplus_ent_exist = 1
   Then
      WSJPLUS_ELIGIBLE = 1;
   ELSE
      WSJPLUS_ELIGIBLE = 0;


   todaydt = (&date_run.);
   format todaydt date9.;
    
   TENURE=ROUND( (intck('month',INIT_ORD_CRE_DT,todaydt)/12), 0.01) ;

  run;

 data WP_OP.inapp_subs_bck_&date_mmddyy
( KEEP = uu_id_hash uu_id email_addr email_addr_hash SBSCR_ID acct_num acct_num_hash PROD_CD MOSAIC_PROD_CD 
         FIRST_NAME LAST_NAME INIT_ORD_CRE_DT SBSCR_TERM_STRT_DT SBSCR_TERM_END_DT SBSCR_TRMNT_DT
         SBSCR_STATUS BILL_SYS_ID IS_MIGRATED AR_FLAG 
         PAYMENT_TYPE OFFER_TYPE BUSINESS_OWNER BUSINESS_OWNER_CL SUBSCRIPTION_TYPE CHANNEL CHANNEL_2
         FREQUENCY_DESC OFFER_PRICE COMPANY_NAME COUPON_CD
         SOURCE PYMT_INSTR_ID PRINT_STAT WSJPLUS_ELIGIBLE TENURE); 


 set &wsjplus..inapp_eligible;

 
run;

data &wsjplus..inapp_subs;
   set WP_OP.inapp_subs_bck_&date_mmddyy;
run;
