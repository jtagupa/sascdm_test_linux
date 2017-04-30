
*%let date_mmddyy=%sysfunc(putn(%eval(%sysfunc(today())),mmddyyn6.));
%let wsjplus=work;

	data &wsjplus..mcs_data ;
	     
	set stg.mosaic_cust_subscription 
	   ( KEEP = uu_id&hash LOGIN_NAME SBSCR_ID SBSCR_STATE_CODE PROD_CD PROD_NAME AUTORENEW_IND
	          SBSCR_CRE_DT SBSCR_TERM_ID SBSCR_TERM_STRT_DT	SBSCR_TERM_END_DT SBSCR_TRMNT_DT DUR_UNIT DUR_QTY	
	          PURCH_AMT PURCH_CURRENCY BRAND_NAME PURCH_DT RENEWAL_DT SBSCR_GRP_ID	GRP_TYPE_NAME GRP_TYPE_CD
			  TRACK_CD	CHANNEL	CAMPAIGN_NAME CAMPAIGN_TYPE FREE_PAID_TYPE PHONE_NUM BUNDLE_NAME
	          IS_MIGRATED IS_REACTIVATED EXTL_SYS_ID ICS_CUST_ID print_acct_num&hash COUPON_CD INIT_ORD_SOURCE_CD
	          ICS_OOT BUSINESS_OWNER BUSINESS_OWNER_CL OFFER_TYPE	BILL_SYS_ID	EDW_OFFER_PRICE
	          EDW_FIRST_OUT_IFP_DT INIT_ORD_CRE_DT SBSCR_STRT_DT
	          FRE_ORDER_ID PYMT_INSTR_ID SBSCR_STATUS SBSCR_STATE_ID PRINT_PUB_CODE CIB_CUSTOMER_ID
		      TENURE_BY_CUST TENURE_BY_PROD CAMPAIGN_RENEWAL_CNT
			  SUBSCR_RENEWAL_CNT SRCE_KEY CAMPAIGN_MARKETING_PROGRAM DELVR_CALENDAR_NAME 
	          email_addr&hash FIRST_NAME LAST_NAME TERM_WKS DELVR_STAT_NAME DELVR_CALENDAR_NAME
	          PAY_TYPE COUPON_CD COMPANY_ORGANIZATION CAMPAIGN_SUBTYPE);

	where sbscr_trmnt_dt is missing
	and bill_sys_id IN ( 1, 8)
	and ( brand_name like '%WSJ%'
	      OR prod_cd in ( 'prod830009', 'prod10004', 'prod80002', 'prod10002' , 'prod300004', 'prod480005',  'prod480006')
		 )
	and sbscr_id > 0;

	/*********************
	    Commented this on 07/15 to get all and limit the subs only at end
	
	and (( FREE_PAID_TYPE IN ('IFP', 'PAID')  AND business_owner = 'CONSUMER US') OR
	       (BUSINESS_OWNER_CL = 'CORPORATE US'));
    **********************/

  /* and DELVR_CALENDAR_NAME <> 'WSJ_SAT'; */

/* Include German Subs
	AND OFFER_TYPE NOT IN ( 'GERMAN DIGITAL PLUS', 'GERMAN STANDALONE')
	AND lowcase(prod_cd) not like '%de%';
 */
	  
	run;

   data &wsjplus..paid_mcs;

      set &wsjplus..mcs_data;
	  where FREE_PAID_TYPE IN ('IFP', 'PAID');

      select (prod_cd);
		when ('prod830009')  prod_cd_var = 1;
		when ('prod480005')  prod_cd_var = 2;
		when ('prod480006')  prod_cd_var = 3;
		when ('prod10004')  prod_cd_var = 4;
		when ('prod80002')  prod_cd_var = 5;
		when ('prod300004')  prod_cd_var = 6;
		when ('prod10002')  prod_cd_var = 7;
	    otherwise prod_cd_var = 8;
	   end;

	   select (prod_cd);
		when ('prod10002')  prod_cd_mob_var = 1;
	    otherwise prod_cd_mob_var = 2;
	   end;

	   SBSCR_TERM = strip(dur_qty) || strip(dur_unit);

	/* uu_id&hash in ( 'dea000be-72c7-456d-abc0-d652fd1bce85', '0f5453c7-6587-44e7-b8ed-fc209d9ed0cb', 'fe1b2c67-bfcc-4ef1-a931-db050bcc4516') */

 run;

proc sort data=&wsjplus..paid_mcs;
    by uu_id&hash prod_cd_var sbscr_status descending sbscr_term_strt_dt ;
run;

proc sort data=&wsjplus..paid_mcs nodupkey;
    by uu_id&hash;
run;


/* check for mobile only */

	/********* Below code is commented to Include MOBILE ONLY subs also

 data wsjplus.check_mobile_only ( keep = uu_id&hash sbscr_status sbscr_term_strt_dt init_ord_cre_dt prod_cd prod_cd_mob_var );
     set  WSJPLUS.mcs_US;
run;

 proc sort data=wsjplus.check_mobile_only;
    by uu_id&hash descending prod_cd_mob_var; 
 run;

proc sort data=wsjplus.check_mobile_only nodupkey;
    by uu_id&hash;
run;

data wsjplus.mobile_only_uuids;
  set wsjplus.check_mobile_only;
  where prod_cd_mob_var = 1;
run;
*************************** END OF MOBILE ONLY *******************/

/* delete duplicate records for the same uuid */


/* Commenting this to include Mobile Only subs
proc sql;

create table wsjplus.mcs_us_eligible
as
    select * from WSJPLUS.mcs_US
	where
	uu_id&hash not in ( select uu_id&hash from wsjplus.mobile_only_uuids);

quit;
************/


/* End of Mosaic Eligible */


data &wsjplus..not_paid_mcs;

      set &wsjplus..mcs_data;
	  where ( FREE_PAID_TYPE = "" OR FREE_PAID_TYPE NOT IN ('IFP', 'PAID'));

 run;


 proc freq data=&wsjplus..not_paid_mcs;
    tables free_paid_type;
 run;

data &wsjplus..not_paid_wsjplus_elig;

  set &wsjplus..not_paid_mcs;
  where
      FREE_PAID_TYPE = 'FREE' AND BUSINESS_OWNER_CL IN ( 'CORPORATE US' , 'CORPORATE APAC', 'CORPORATE EMEA', 'EDUCATION');

run;


data &wsjplus..merge_data_paid_unpaid;
   
   length subscription_type $40.;

   set &wsjplus..paid_mcs &WSJPLUS..not_paid_wsjplus_elig;

   if FREE_PAID_TYPE = 'FREE' AND BUSINESS_OWNER_CL = 'CORPORATE US' THEN BUSINESS_OWNER = 'CORPORATE US';
   if FREE_PAID_TYPE = 'FREE' AND BUSINESS_OWNER_CL = 'CORPORATE APAC' THEN BUSINESS_OWNER = 'CORPORATE APAC';
   if FREE_PAID_TYPE = 'FREE' AND BUSINESS_OWNER_CL = 'CORPORATE EMEA' THEN BUSINESS_OWNER = 'CORPORATE EMEA';
   if FREE_PAID_TYPE = 'FREE' AND BUSINESS_OWNER_CL = 'EDUCATION' THEN BUSINESS_OWNER = 'EDUCATION';

   if BUSINESS_OWNER IN (  "CONSUMER US","CONSUMER APAC", "CONSUMER EMEA")  then subscription_Type = "Regular";
   else if BUSINESS_OWNER= "EDUCATION" then subscription_Type = "Educational";
   else if BUSINESS_OWNER IN  ( "CORPORATE US", "CORPORATE APAC", "CORPORATE EMEA" ) then subscription_Type = "Corporate";
   else subscription_Type = "OTHER";

   if DELVR_CALENDAR_NAME = 'WSJ_SAT' Then subscription_type="WKND ONLY";

   if OFFER_TYPE IN ( 'GERMAN DIGITAL PLUS', 'GERMAN STANDALONE') Then subscription_type = "GERMAN DIGITAL PLUS";
   
 run;

 data &wsjplus..merge_data_paid_unpaid;

     set &wsjplus..merge_data_paid_unpaid;

  select;
     WHEN  ( FREE_PAID_TYPE IN ('IFP', 'PAID') ) priority_var = 1;
     WHEN  ( FREE_PAID_TYPE IN ('FREE') AND BUSINESS_OWNER = 'CORPORATE US' )  priority_var = 2;
	 WHEN  ( FREE_PAID_TYPE IN ('FREE') AND BUSINESS_OWNER = 'CORPORATE APAC' )  priority_var = 3;
	 WHEN  ( FREE_PAID_TYPE IN ('FREE') AND BUSINESS_OWNER = 'CORPORATE EMEA' )  priority_var = 4;
     otherwise priority_var = 5;
  end;

  select (prod_cd);
		when ('prod830009')  prod_cd_var = 1;
		when ('prod480005')  prod_cd_var = 2;
		when ('prod480006')  prod_cd_var = 3;
		when ('prod10004')  prod_cd_var = 4;
		when ('prod80002')  prod_cd_var = 5;
		when ('prod300004')  prod_cd_var = 6;
		when ('prod10002')  prod_cd_var = 7;
	    otherwise prod_cd_var = 8;
  end;

  run;


proc sort data=&wsjplus..merge_data_paid_unpaid;
   by uu_id&hash priority_var prod_cd_var descending sbscr_term_strt_dt ;
run;

proc sort data=&wsjplus..merge_data_paid_unpaid nodupkey dupout=&wsjplus..paid_and_free_subs;
   by uu_id&hash;
run;


 data &wsjplus..merge_data_paid_unpaid;

    length uu_id_hash      		 $100.;
	length email_addr_hash 		 $100.;
	length acct_num_hash   		 $120.;
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
	length FREE_PAID_TYPE    $25.
  length uu_id       		 $200.;
	length email_addr  		 $200.;
	length acct_num    		 $200.;
	

    set &wsjplus..merge_data_paid_unpaid;
	
	SOURCE = 'MOSAIC';

    acct_num&hash = print_acct_num&hash;

	mosaic_prod_cd = prod_cd;

    prod_cd = 'J';
 
    AR_FLAG = 'AR';

    if PAY_TYPE in ("Credit") then Payment_Type = "CC";
    else Payment_Type = PAY_TYPE;
 
 /* 072613. Deitrich instructed 8 wk to move to quarterly bucket instead of monthly*/

    if term_wks in (5) then Frequency_Desc = "Monthly";
    else if term_wks in (8,13,17) then Frequency_Desc = "Quarterly";
    else if term_wks in (26,30) then Frequency_Desc = "Semi_Annual";
    else if term_wks in (52) then Frequency_Desc = "Annual";
    else if term_wks in (104) then Frequency_Desc = "2 Year";
    else Frequency_Desc = "OTHER";

    If ( mosaic_prod_cd = 'prod830009' ) and ( offer_type = 'STANDALONE' ) Then PRINT_STAT = 'Print Only';
	Else If ( mosaic_prod_cd = 'prod830009' ) and ( offer_type ^= 'STANDALONE' ) Then PRINT_STAT = 'BUNDLE';
    Else PRINT_STAT = 'ONLINE';

    COMPANY_NAME = COMPANY_ORGANIZATION;

    offer_price = edw_offer_price;

     todaydt = (&date_run.);
	 format todaydt date9.;
    
	 TENURE=ROUND( (intck('month',init_ord_cre_dt,todaydt)/12), 0.01) ;

	 IF FREE_PAID_TYPE IN ('IFP', 'PAID') Then FREE_PAID_TYPE = 'PAID';
	 ELSE IF FREE_PAID_TYPE IN ('FREE') Then FREE_PAID_TYPE = 'FREE';

   /* SET THE WSJPLUS Eligible Flag */

	
	If  ( 
         ( FREE_PAID_TYPE IN ('IFP', 'PAID')
	        AND BUSINESS_OWNER IN (  "CONSUMER US","CONSUMER APAC", "CONSUMER EMEA") 
          ) 
          OR
		  ( FREE_PAID_TYPE IN ('FREE') AND 
		    BUSINESS_OWNER IN (  "CORPORATE US","CORPORATE APAC", "CORPORATE EMEA")
          )
		  OR
		 (
		   BUSINESS_OWNER = 'EDUCATION' AND
		  (CAMPIGN_SUBTYPE NOT IN ('PROFESSOR SAMPLING') 
           AND COMPANY_ORGANIZATION NOT IN ('PROFESSOR SAMPLING'))
		 )
       )
	  AND 
	  (
          subscription_type not in ( 'WKND ONLY') AND 
		  offer_type ^= 'ENTITLEMENTS TO PRINT' 
	   )
	 THEN
	     WSJPLUS_ELIGIBLE = 1;
	 ELSE 
	     WSJPLUS_ELIGIBLE = 0;

 run;
     
 
/* This includes all Paid ( after applying the remove duplicates
   and all FREE ( without removing duplicates */

/****************************************************************
data WP_OP.mosaic_subs_&date_mmddyy
( KEEP = uu_id&hash email_addr&hash SBSCR_ID acct_num&hash PROD_CD MOSAIC_PROD_CD 
         FIRST_NAME LAST_NAME INIT_ORD_CRE_DT SBSCR_TERM_STRT_DT SBSCR_TERM_END_DT SBSCR_TRMNT_DT
         SBSCR_STATUS BILL_SYS_ID IS_MIGRATED AR_FLAG 
         PAYMENT_TYPE OFFER_TYPE BUSINESS_OWNER BUSINESS_OWNER_CL SUBSCRIPTION_TYPE CHANNEL CHANNEL_2
         FREQUENCY_DESC OFFER_PRICE COMPANY_NAME COUPON_CD
         SOURCE PYMT_INSTR_ID PRINT_STAT FREE_PAID_TYPE WSJPLUS_ELIGIBLE
         uu_id email_addr acct_num ); 

   set WSJPLUS.mosaic_data_all;

run;
********************************************************************/

 data WP_OP.mosaic_subs_bck_&date_mmddyy;

 set &wsjplus..merge_data_paid_unpaid;

run;

data &wsjplus..mosaic_subs
( KEEP = uu_id&hash email_addr&hash SBSCR_ID acct_num&hash PROD_CD MOSAIC_PROD_CD 
         FIRST_NAME LAST_NAME INIT_ORD_CRE_DT SBSCR_TERM_STRT_DT SBSCR_TERM_END_DT SBSCR_TRMNT_DT
         SBSCR_STATUS BILL_SYS_ID IS_MIGRATED AR_FLAG 
         PAYMENT_TYPE OFFER_TYPE BUSINESS_OWNER BUSINESS_OWNER_CL SUBSCRIPTION_TYPE CHANNEL CHANNEL_2
         FREQUENCY_DESC OFFER_PRICE COMPANY_NAME COUPON_CD
         SOURCE PYMT_INSTR_ID PRINT_STAT FREE_PAID_TYPE TENURE WSJPLUS_ELIGIBLE 
         uu_id email_addr acct_num); 


 set &wsjplus..merge_data_paid_unpaid;

run;

