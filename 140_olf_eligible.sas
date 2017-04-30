
*%let date_mmddyy=%sysfunc(putn(%eval(%sysfunc(today())),mmddyyn6.));


options obs=max compress=yes reuse=yes fmtsearch=(work table ) ls=64 ps=79
	mprint symbolgen ORIENTATION=portrait papersize=letter ;
/*
proc format;
	value $program
		'W'='WSJ.com/Online Promos'
		'P'='Services'
	 	'S'='Search'
		'A'='Affiliate'
		'N'='Ad Network/Media'
		'F'='Formers Email'
		'H'='Other House Lists'
		'G'='Gift'
		'M'='Next Jump/Outside Lists/Misc'
		'L'='Co Reg Leads'
		'Z'='Misc'
		other='Unknown';
		
value $paytyp
		'0'="Credit" 	
		'1'="Cash" 	 
		'2'="Sub Agency"
		'3'="Credit Card";
VALUE $SD
	'1'='DM'
	'A'='DM'
	'B'='DM'
	'2'='MA'
	'3'='DRB'
	'4'='TM'
	'5'='TM'
	'6'='IM'
	'7'='JIE'
	'8'='ADV'
	'0'='WHITE MAIL'
	'93'='EE & CO SUBS'
	'96'='EE & CO SUBS'
	'97'='CALL CTR'
	'98'='RETENTION'
	'99'='WHITE MAIL'
	'9A'='RETENTION'
	'9B'='RETENTION'
	'9C'='MA'
	'9D'='RETENTION'
	'9G'='MA'
	'9M'='IM'
	'9V'='WHITE MAIL'
	'9W'='MA'
	'9X'='GIFT'
	'9Y'='RET DEFAULT';

	VALUE $OD
	'1'='DM'
	'A'='DM'
	'B'='DM'
	'2'='MA'
	'3'='DRB'
	'4'='TM'
	'5'='TM'
	'6'='IM'
	'7'='JIE'
	'8'='ADV'
	'0'='WHITE MAIL'
	'93'='EE & CO SUBS'
	'96'='EE & CO SUBS'
	'97'='CALL CTR'
	'98'='RETENTION'
	'99'='WHITE MAIL'
	'9A'='RETENTION'
	'9B'='RETENTION'
	'9C'='MISC'
	'9D'='RETENTION'
	'9G'='MISC'
	'9M'='IM'
	'9V'='WHITE MAIL'
	'9W'='MISC'
	'9X'='GIFT'
	'9Y'='RET DEFAULT';
	
	value carrier
		1='Carrier'
		0='Mail';
	value $pctr
		'00'="Orlando FL"
		'01'="Chicopee, MA"
		'02'="White Oak, MD"
		'03'="Naperville, IL"
		'04'="Sharon, PA"
		'05'="Dallas, TX"
		'06'="Riverside, CA"
		'07'="Palo Alto, CA"
		'08'="Highland, IL"
		'09'="So. Brunswick, NJ"
		'10'="Seattle, WA"
		'11'="Denver, CO"
		'12'="Bowling Green, OH"
		'13'="Des Moines, IA"
		'14'="LaGrange, GA"
		'15'="Beaumont, TX"
		'16'="Charlotte, NC"
		'17='="Oklahoma City, OK";
	value $mkt
		'00'='South'
		'01'='Northeast'
		'02'='South'
		'03'='Midwest'
		'04'='Midwest'
		'05'='Central'
		'06'='Southwest'
		'07'='Northwest'
		'08'='Central'
		'09'='Northeast'
		'10'='Northwest'
		'11'='Southwest'
		'12'='Midwest'
		'13'='Central'
		'14'='South'
		'15'='Central'
		'16'='South'
		'17'='Oklahoma City, OK';	
	value $subtyp
		'0R'='Employee'
		'0V'='Employee'
		'1 '='Regular'
		'1G'='Regular'
		'1P'='Commission/Cap+'
		'1R'='Regular'
		'1S'='Regular'
		'1T'='Regular'
		'1V'='Regular'
		'1X'='Regular'
		'4 '='Educational'
		'4A'='Educational'
		'4C'='Educational'
		'4D'='Educational'
		'4E'='Educational'
		'4F'='Educational'
		'4M'='Institutional Sales'
		'4P'='Educational'
		'4R'='Educational'
		'7 '='Complimentary'
		'7A'='Advertising Agcy'
		'7C'='Complimentary'
		'7E'='Complimentary'
		'7F'='Complimentary'
		'7I'='Complimentary'
		'7M'='Complimentary'
		'7P'='Program Professors'
		'7S'='Complimentary'
		'7T'='Complimentary'
		'7X'='Complimentary'
		'7Z'='Migrated'
		'9C'='Canadian'
		'9M'='Canadian'
		'9R'='Foreign'
		'XF'='Bulk'
		'XR'='Bulk'
		'WR'='Waiting Room'
		'??'='Corporate'
		'KP'='Kaplan'
		'CR'='Classroom Edition'
		'EW'='Employee_Wknd_Only'
		'WW'= 'Waiting_Rm_Wknd_Only'
		 other='Regular';
		
	value $addscr
		'C'='CSR'
		'E'='Email'
	 	'F'='Field'
		'M'='Mail'
		'S'='DM'
		'T'='TM'
		'U'='Unknown'
		'V'='Vendor File'
		'W'='Web'
		' '='N/A';
	Value optout
		1="Opted Out"
		0="Opted In";
	value $optout
		'01'="No time to read"
		'02'="Not interested in WE content"
		'03'="Does not want/need WSJ on weekend"
		'04'="Traveling on weekends"
		'05'="Not able to get carrier on Saturday"
		'06'="Carrier delivery problems"
		'07'="Mail delivery problems"
		'08'="Already receiving Barron’s"
		'09'="Already receiving NYT on Saturday"
		'10'="already receiving Financial Times on Saturday"
		'11'="Already receiving another publication on Saturday";
	value $Addtyp
		'B'='Confirmed'
		'A'='Alternate'
		'D'='Default'
		'C'='Opt Out';

run;
*/

data &wsjplus..olfmaster
( Keep = uu_id_hash uu_id PROD_CD acct_num_hash acct_num CO_NAME FIRST_NAME LAST_NAME email_addr_hash   email_addr
         STREET_NUM ADDR ADDR2 CITY STATE ZIP_5 ZIP_PLUS_4 POSTAL_CD CNTRY_CD
         DELVY_IND SBSCR_TYPE SBSCR_SUB_TYPE LAST_SBSCR_TYPE LAST_SBSCR_SUB_TYPE
		 CURR_SRCE_KEY ORIGL_SRCE_KEY CURR_TERM SBSCR_STRT_DT RENWL_CNT RENWL_DT 
		 STOP_DT STOP_RSN STOP_SUB_RSN BILL_PLAN_CD BILL_SUB_PLAN_CD ICU_IND CORP_ACCT_NUM
		 ONLINE_ACCT_NUM TRMNT_DT 
         RATE_COMMMIT TERM_COMMMIT COMMIT_EXPR_DT NUM_OF_COPY 
         CUMUL_AMT_PAID CURR_TERM EXPR_DT
         CURR_PAY_STATUS
         STREET_NUM ADDR ADDR2 CITY STATE ZIP_5 ZIP_PLUS_4 POSTAL_CD CNTRY_CD );
   set stg.olf_master;
   where prod_cd = 'J';
   /* and acct_num&hash in ( 
    '010160059583', '010160769106', '010160847042', '010208680818' ,'010112972575', '010112972303', '010112971587', 
    '010160278561', '010160254882', '010213022157', '010213045122', '010160398523', '010160004435', '010160015462', 
    '010160000162', '010160025174', '010160007486', '010160014252', '010160026341', '010112971846',
    '092101565935', '101311279800', '050808991694', '061693280958');
  */
run;

/* Email data 


filename myfile ftp "'MW.MWEMAIL.LOADFILE'"
          host='162.55.33.54' prompt recfm=f lrecl=80;


data myemail/ view=myemail;
   	infile myfile DSD missover lrecl=80 recfm=f;
	input	
   		@1 		Acct_No	  	    	$ebcdic12.
		@13		Prod_Code	  		$ebcdic1.
		@14		Email_Address 		$ebcdic63.
		@77		Renew_Via_Email 	$ebcdic1.
		@78		Req_Ques_Via_Email	$ebcdic1.
		@79		Req_Djps_Via_Email	$ebcdic1.
		@80		Wsjie_Sub 			$ebcdic1.;
	label	Acct_No="Customer Unique Account Number"
            Prod_Code="Product Code"
			Email_Address="Subscribers Email Address"
			Renew_Via_Email="Renewal Subscription via email"
			Req_Ques_Via_Email="Receive Questioniare via email"
			Req_Djps_Via_Email="Receive Solicitations via email"  
			Wsjie_Sub="Subscription to WSJIE Indicator";
run;


data &wsjplus..olf_email ;
	set myemail (where=(Prod_Code='J' &  
	(email_address ^ ? 'spam' & email_address ^ ? 'abuse' &
	email_address ^ ? 'dowjones' & email_address ^ ? 'DOWJONES' &
	email_address ^ ? 'wsj' & email_address ^ ? 'WSJ' &
	email_address ^ ? 'fmr' & email_address ^ ? 'FMR' &
	email_address ^ ? 'factiva' & email_address ^ ? 'FACTIVA' &
	email_address ^ ? 'SPAM' & email_address ^ ? 'ABUSE')) ) ;
	keep Acct_No 
	 Email_Address Req_Djps_Via_Email Req_Ques_Via_Email;
run;

data keep.wsjemail; set keep.wsjemail; email_address=left(email_address); run;
data keep.wsjemail; set keep.wsjemail; email_address=lowcase(email_address); run;
proc sort nodupkey data=keep.wsjemail; by email_address; run;


 End of Email */


data &wsjplus..olfmaster;
  set &wsjplus..olfmaster;

  length cust_status $10.;

  /* Live includes active and advance start from olf master */

  if stop_dt = '08AUG8888'd then cust_status = 'Live';
  else 
    if stop_dt <> '08AUG8888'd and substr(stop_rsn, 1, 1) = '6'
	then
	   cust_status = 'Suspended';
	else
	   if substr(stop_rsn, 1, 1) = 'A' 
       then 
          cust_status = 'Advance Start';
	   else 
          cust_status = 'Stopped';

 run;  
 
 
 data &wsjplus..olf_eligible;
   set &wsjplus..olfmaster;

   /*
   where 
     cust_status ^= 'Stopped' ;
   */

	length cur_Src_grp $20.;
	length cur_Src_grp2 $20.;
	length last_sub_type $2.;
	length CK2 $2.;
	length AR_FLAG $10.;
	length CHANNEL $40.;

	
	if BILL_PLAN_CD in('3A','3P') then AR_FLAG = 'AR';
	else AR_FLAG = 'Non AR' ;
	
	length Payment_Type $8.;
   
    if CURR_PAY_STATUS IN ("0","2") then Payment_Type = "OTHER";
    else if CURR_PAY_STATUS in ("1") then Payment_Type = "CASH";
    else if CURR_PAY_STATUS in ("3") then Payment_Type = "CC";
    else payment_type = "OTHER";

	last_sub_type = last_sbscr_type || last_sbscr_sub_type ;

    if substr(CURR_SRCE_KEY,1,1)='9' then CK2=substr(CURR_SRCE_KEY,1,2);
    else if Substr(CURR_SRCE_KEY,1,1)^='9' then CK2=substr(CURR_SRCE_KEY,1,1);

    cur_Src_grp=put(CK2,$sd.); 
	CHANNEL = put(CK2,$sd.); 

	if Last_Sub_Type='1P' then cur_Src_grp2='MA';
	else if Last_Sub_Type in ('1X','1G') then cur_Src_grp2='GIFT';
	else if Last_Sub_Type in ('1T','1S') then cur_Src_grp2='JIE TRANS';
	else cur_Src_grp2=cur_Src_grp;

	if  CURR_SRCE_KEY in("9BFSB4","9AFSB3","9PFSP4","98FSP3","9BFBC2","9PFPC2","9BFBC3","9PFPC3","9AFBC5","94FPC5","9BFCS2","9PFCS2",
                          "9LBNA2","9BPNA3","9LCSR1","93CSR1","9RBNA4","98PNA5","9LCSR2","92CSR2")
    then cur_Src_grp2="SOLUTIONS" ;
    
	if CURR_SRCE_KEY in("9LBCC1","9LBCC2","9LBCC5","9LBCC3","9LBCC4","9JBCC5","9JNARJ",
	                      "9JBCC7","9BFAR1","9BFAR2","9BFAR9","9BFAR3","9BFAR4","9AFAR5","9AFARJ","9AFAR7",
                          "9PFAR1","9PFAR2","9PFAR9","9PFAR3","9PFAR4","9PFAR8","94FAR6")
    then cur_Src_grp2="CALL CTR";

    if CURR_SRCE_KEY in("9C3PCJ","9C3PER","9C3PHJ","9C3PKJ","9C3PSJ","9C7PBJ","9C7PCJ","9CEREW")
    then cur_Src_grp2="REWARDS";
    

	if (substr(CURR_SRCE_KEY,1,2)in ("9A","9B","92","93","94","9P","9A","9B","9R","9T","9J","9L","9K",
                                       "9N","9H","9S","9E","9F","91","95"))
    then cur_Src_grp2="RETENTION";
    
    if substr(CURR_SRCE_KEY,2,1)="T" then cur_Src_grp2= "SAT Only";

	if (substr(CURR_SRCE_KEY,1,4) = "9CLP") then cur_Src_grp2="CAP";

	
 run;


 data &wsjplus..olf_eligible;
    set &wsjplus..olf_eligible;

 length Price_range4 $22.;
 length Frequency_Desc $25.;


  /* Get the Price Range */

/*
 If NUM_OF_COPY = 0 OR NUM_OF_COPY = '.' Then NUM_OF_COPY = 1;

 If term_commmit > 0 and commit_expr_dt ^= '09SEP9999'd then do;

         rate=round(RATE_COMMMIT/NUM_OF_COPY,1);
		 term=term_commmit;

		 if cust_status="Live" and (&date_run.) >= COMMIT_EXPR_DT then cust_status='Grace'; 
 end;

 else do;

 */

          rate=round(CUMUL_AMT_PAID/NUM_OF_COPY,1);
		  term=CURR_TERM;

		  if cust_status="Live" and (&date_run.) >= EXPR_DT then cust_Status='Grace'; 

		  /*
 end;
		  */

/*
    IF 0<=CURR_TERM<= 35 THEN term_wk=5; 
	IF 36<=CURR_TERM<=70 THEN term_wk=8; 
	IF 71<=CURR_TERM<=98 THEN term_wk=13; 
	IF 99<=CURR_TERM<=140 THEN term_wk=17; 
	IF 141<=CURR_TERM<=182 THEN term_wk=26;
	IF 183<=CURR_TERM<=315 THEN term_wk=30;
	IF 316<=CURR_TERM<=537 THEN term_wk=52;
	IF 538<=CURR_TERM THEN term_wk=104; 
 */

    IF 0<= TERM <= 35 THEN term_wk=5; 
	IF 36<= TERM <=70 THEN term_wk=8; 
	IF 71<= TERM <=98 THEN term_wk=13; 
	IF 99<= TERM <=140 THEN term_wk=17; 
	IF 141<= TERM <=182 THEN term_wk=26;
	IF 183<= TERM <=315 THEN term_wk=30;
	IF 316<= TERM <=537 THEN term_wk=52;
	IF 538<= TERM THEN term_wk=104; 

	if term_wk in (5) then Frequency_Desc = "Monthly";
    else if term_wk in (8, 13,17) then Frequency_Desc = "Quarterly";
    else if term_wk in (26,30) then Frequency_Desc = "Semi_Annual";
    else if term_wk in (52) then Frequency_Desc = "Annual";
    else if term_wk in (104) then Frequency_Desc = "2 Year";
    else Frequency_Desc = "OTHER";

	/* Annualizing the rate 

	rate3=rate;
    rate3 = (360/term)*rate;
    rate3=Round(rate3,1);

	*/


    /*NEW*/
    length Price_range4 $22.;

    
    if rate_commmit > 0 then 
	do;

       rate3 = rate_commmit;
	end;
	else
	do;

	   rate3=rate;

	   If term <> 0 then
          rate3 = (360/term)*rate;
	   else
          rate3 = rate;

       rate3=Round(rate3,1);

	    /* Check with Sharon whether the rate_commit can also be 104 weeks */

    end;

	if term_wk=104 then rate3=rate;

	/* offer_price = rate3;  Commented in June to have the offer price as cum amt paid */

    /*012215*/
    if rate3 < 99 then Price_range4= "$99 and below";
    else if rate3 >= 99 and rate3 < 100 then Price_range4= "$99";
    else if rate3 >= 100 and rate3 < 125 then Price_range4= "$119";
    else if rate3 >= 125 and rate3 < 145 then Price_range4= "$140";
    else if rate3 >= 145 and rate3 < 150 then Price_range4= "$149";
    else if rate3 >= 150 and rate3 < 180 then Price_range4= "$179";
    else if rate3 >= 180 and rate3 < 198 then Price_range4= "$187";
    else if rate3 >= 198 and rate3 < 199 then Price_range4= "6 for $99/$198";
  
    else if rate3 >= 199 And rate3 < 323 then Price_range4= "$259";
    else if rate3 >= 323 And rate3 < 346 then Price_range4= "$324";
    else if rate3 >= 346 And rate3 < 360 then Price_range4= "$348";
    else if rate3 >= 360 And rate3 < 392 then Price_range4= "$374";
    else if rate3 >= 392 And rate3 < 400 then Price_range4= "$396";

    else if rate3 >= 400 And rate3 < 425 then Price_range4= "$413";
    else if rate3 >= 425 And rate3 < 460 then Price_range4= "$455";
    else if rate3 >= 460 And rate3 < 503 then Price_range4= "$502";
    else Price_range4= ">$502"; 

    if CUMUL_AMT_PAID in (44.75,179) then Price_range4= "$179"; 

 run;


 data &wsjplus..olf_eligible;
     set &wsjplus..olf_eligible;

	 length subscription_Type $25.;
	 length print_status $40.;
	 length new_print_stat $40.;

	 length registration $25.;

	 Subscription_Type=put(Last_Sub_Type,$subtyp.);

	 If Last_Sub_Type in ('1X','1G') then subscription_type='GIFT';
	 else if Last_Sub_Type in ('1T','1S') then subscription_type='JIE TRANS';

	 if cur_Src_grp2= "AGENCY" then subscription_type="AGENCY";
	 
	 If (substr(BILL_PLAN_CD,1,1)='1') then subscription_type="Corporate";

	 if cur_Src_grp2= "SAT Only" then subscription_type="WKND ONLY";

	 /*
	 if cur_Src_grp2='GIFT' then subscription_type="GIFT";
     */

	 if Icu_Ind in ('D','W') & substr(Online_Acct_Num,1,1)  in ("A")
		then Print_status="Combo Registered";
     else if Icu_Ind in ('D','W') & substr(Online_Acct_Num,1,1) not in ("A") 
		then Print_status="Combo Unregistered";
	 else if Icu_Ind in ('Q') then Print_status="ComboJIE";
		else if Icu_Ind ^ in ('D','Q','W') & substr(Online_Acct_Num,1,1) = "A"
	 then Print_status="Dual";
		else Print_status="Print Only";


     if substr(Online_Acct_Num,1,1) in ("A") then registration="Registered";
     else if substr(Online_Acct_Num,1,1) in ("C")  then registration="Cancelregistered";
     else registration="Unregistered";

	 
     if icu_ind in ("D") then new_print_stat='COMBO';
		else if icu_ind in ("W") then new_print_stat='BUNDLE';
		else if icu_ind in ("Q") then new_print_stat='ED BUNDLE';
			else if icu_ind ^ in ("D","W","Q")& substr(Online_Acct_Num,1,1) = "A" then new_Print_stat="DUAL";
	 	else new_print_stat="Print Only";


		/* Copying current source key to the same names as in the Rules files */

 run;

/* AGENCY */

data &wsjplus..agency_key;

  set slow_chg.Cap_agency_10282014;
  where _flip = 'AGENCY';
  /*
  set Rules.Price_103013_agency;
  */

  length CURR_SRCE_KEY $6.;
  CURR_SRCE_KEY = curr_Source_key ;

run;

proc sort data=&wsjplus..agency_key nodupkey; 
   by CURR_SRCE_KEY; 
run;

proc sort data=&wsjplus..olf_eligible;
   by CURR_SRCE_KEY; 
run;

data &wsjplus..olf_eligible ( drop = _GRP _Curr_Source_key _Flip Curr_Source_Key ) ;
   MERGE &wsjplus..olf_eligible (in=in1) 
         &wsjplus..agency_key(in=in2);
   BY CURR_SRCE_KEY; 
   if in1;

   if _flip = "AGENCY" then cur_Src_grp2= "AGENCY"; 

   if cur_Src_grp2= "AGENCY" then subscription_type="AGENCY";

run;


/* End of Agency */

/* CAP */

data &wsjplus..cap_key;

   set slow_chg.Cap_agency_10282014;
   where _flip = 'CAP';
   /*
   set Rules.Cap011614_use;
   */

   length CURR_SRCE_KEY $6.;
   CURR_SRCE_KEY = curr_Source_key ;
   
run;

proc sort data=&wsjplus..cap_key nodupkey; 
   by CURR_SRCE_KEY;
run;

proc sort data=&wsjplus..olf_eligible;
   by CURR_SRCE_KEY;
run;

data &wsjplus..olf_eligible ;
    MERGE &wsjplus..olf_eligible (in=in1) 
          &wsjplus..cap_key (in=in2);
    BY CURR_SRCE_KEY;

	/* &wsjplus..cap_key (in=in2 keep=CURR_SRCE_KEY _flip); */

    if in1;

	if _flip="CAP" then subscription_type="CAP";

	If substr(CURR_SRCE_KEY,1,4) = "9CLP" then cur_Src_grp2="CAP";

    If  cur_Src_grp2= "CAP" then subscription_type="CAP";

run;

/*
proc sql;

CREATE TABLE &wsjplus..olf_eligible_op
AS
   SELECT
       tbl1.*,tbl2._flip
   FROM
       &wsjplus..olf_eligible tbl1 left outer join
       &wsjplus..cap_key tbl2
   ON
       tbl1.CURR_SRCE_KEY = tbl2.CURR_SRCE_KEY;

quit;       
*/


data &wsjplus..olf_eligible;

   set &wsjplus..olf_eligible;
   If subscription_type in ("CAP",'Commission/Cap+')
      and curr_srce_key ^=  "9CLKGJ"
      and substr(curr_srce_key,1,1) ^in ("A","B","1","2","3","4","5","6","7","8","0" )
      and CUMUL_AMT_PAID in (26.99,28.99,32.99,31.20,37.91,37.92,41.81,41.82,34.45,99)
   then
      subscription_type = 'CAP VERIFY';

   /* Accounts which are marked as CAP, but are not CAP based on the above criteria.
      For example If the price is paid as 32.99 , even if the subscription type says as CAP, they are not CAP */
run;

data &wsjplus..olf_eligible;

  set &wsjplus..olf_eligible;

  If subscription_type = 'Regular' and curr_srce_key ^= '9CLKGJ' and CUMUL_AMT_PAID in (44.75,179,1)
    then subscription_type = 'CAP CONV';
  
  /* Accounts which are marked as Regular which are CAP CONVERSIONS */

run;

data &wsjplus..olf_eligible;
  set &wsjplus..olf_eligible;

  length WSJPLUS_subscription_type $25.;
  WSJPLUS_subscription_type = subscription_type;

  Subscription_Type=put(Last_Sub_Type,$subtyp.);
  
  If (substr(BILL_PLAN_CD,1,1)='1') then subscription_type="Corporate";

  if cur_Src_grp2= "AGENCY" then subscription_type="AGENCY";
  if cur_Src_grp2= "CAP" then subscription_type="CAP";

  if cur_Src_grp2= "SAT Only" then subscription_type="WKND ONLY";
   

run;

data &wsjplus..olf_eligible_op1;
   
   set &wsjplus..olf_eligible;
   where cust_status <> 'Stopped';

   /*

   IMPORTANT ******
   this subscription type check is moved at the end to get the delivery address
   for mosaic billed subs
   The same data set can be used if all subs are included here
   and 
       subscription_type in ( "Advertising Agcy", "AGENCY","Bulk","Canadian","Regular","Corporate","Program Professors", "JIE TRANS", "CAP VERIFY", "WKND ONLY");

   */
run;   
 

/* get all entitlements to print before remove dups */

data &wsjplus..mcs_ent_to_print ( drop = print_acct_num&hash );

   set stg.mosaic_cust_subscription ( keep = print_acct_num&hash sbscr_id sbscr_status sbscr_term_strt_dt offer_type);
   where offer_type = 'ENTITLEMENTS TO PRINT'
   and sbscr_status = 'A';
   /*
   and print_acct_num&hash is not null;
   */

   length acct_num_hash $120.;
   acct_num_hash = print_acct_num_hash;
 
    length acct_num  $120.;
   acct_num  = print_acct_num ;
 
   
run;

proc sort data=&wsjplus..mcs_ent_to_print  (rename=(sbscr_id = mcs_sbscr_id));
   by acct_num&hash descending sbscr_term_strt_dt;
run;

proc sort data=&wsjplus..mcs_ent_to_print ( keep = acct_num_hash  acct_num mcs_sbscr_id) nodupkey;
   by acct_num&hash;
run;
/* join to mcs and get the new_print_stat */

proc sort data =&wsjplus..olf_eligible_op1;
  by acct_num&hash;
run;


data &wsjplus..olf_eligible_op11;

   MERGE &wsjplus..olf_eligible_op1 ( IN = in1 )
         &wsjplus..mcs_ent_to_print ( IN = in2 );
   by acct_num&hash;
   IF in1;

   if mcs_sbscr_id ^= '.' then new_print_stat = 'BUNDLE';
  
   if WSJPLUS_subscription_type = 'Corporate' and new_print_stat = 'Print Only' then WSJPLUS_subscription_type = 'Corp_Print_Only';

run;

data &wsjplus..olf_missing_uuid ( keep = acct_num_hash uu_id_hash email_addr_hash acct_num  uu_id  email_addr );
   set &wsjplus..olf_eligible_op11;

   /* This is modified on 09/22/2016 to get the data from mcs for matching print account numbers
      since the uu_id&hash in olf is not always updated with the recent one
   
   where email_addr&hash = '' OR uu_id&hash = ''; -- Commented
   */

run;

proc sort data=&wsjplus..olf_missing_uuid nodupkey;
   by acct_num&hash;
run;


/*
proc freq data =&wsjplus..olf_eligible_op11;
  tables new_print_stat;
run;
*/

proc sql;

CREATE TABLE &wsjplus..get_mcs_uu_id
AS
SELECT olf.*, mcs.uu_id_hash as mcs_uu_id_hash, mcs.email_addr_hash as mcs_email_addr_hash,
			  mcs.uu_id  as mcs_uu_id , mcs.email_addr  as mcs_email_addr, 
			  mcs.sbscr_status, mcs.sbscr_term_strt_dt, mcs.offer_type, 
       mcs.prod_cd as mcs_prod_cd
from
    &wsjplus..olf_missing_uuid olf, 
	stg.mosaic_cust_subscription mcs
where   
    olf.acct_num&hash = mcs.print_acct_num&hash
AND ( brand_name like '%WSJ%'
	      OR prod_cd in ( 'prod830009', 'prod10004', 'prod80002', 'prod10002' , 'prod300004', 'prod480005',  'prod480006')
     )
AND mcs.print_acct_num&hash  not in ('',"&hash_null");

/* mcs.prod_cd in ('prod830009', 'prod10004') */

quit;   


data &wsjplus..get_mcs_uu_id;
   set &wsjplus..get_mcs_uu_id;

   /* Order of products changed to check whether an active online subscription exists 10/31/2016 */

   select ( mcs_prod_cd );

      when ( 'prod10004') email_prod_pref = 1;
      when ('prod830009') email_prod_pref = 2;
	  when ('prod80002') email_prod_pref = 3;
	  when ('prod10002') email_prod_pref = 4;
	  when ('prod300004') email_prod_pref = 5;
      
      otherwise email_prod_pref = 6;
   end;

run;

proc sort data=&wsjplus..get_mcs_uu_id;
   by acct_num&hash sbscr_status email_prod_pref descending sbscr_term_strt_dt;
run;

proc sort data=&wsjplus..get_mcs_uu_id nodupkey;
   by acct_num&hash;
run;

proc sql;

CREATE table &wsjplus..olf_eligible_op2
AS
  select olf.*, tmp.mcs_uu_id, tmp.mcs_email_addr_hash, tmp.offer_type as mcs_offer_type, tmp.mcs_email_addr, 
         tmp.sbscr_status as mcs_sbscr_status,
         tmp.mcs_prod_cd
  from
      &wsjplus..olf_eligible_op11 olf left outer join
      &wsjplus..get_mcs_uu_id tmp
  on
      olf.acct_num&hash = tmp.acct_num&hash;

 quit;


 proc sort data=&wsjplus..ref_country out=&wsjplus..ref_olf_country nodupkey;
    by olf_cntry_cd;
 run;

 proc sql;

 CREATE TABLE &wsjplus..olf_eligible_op3
 AS
    SELECT 
	    t1.*, ref.olf_cntry_name as country, ref.rpt_region
    FROM
        &wsjplus..olf_eligible_op2 t1 left outer join
	    &wsjplus..ref_olf_country ref
	ON t1.cntry_cd = ref.olf_cntry_cd;

 quit;

/* 
proc sql;

 CREATE TABLE &wsjplus..olf_eligible_op4
 AS
    SELECT 
       t1.*, t2.dma_name as dma, t2.dma_code
    FROM
        &wsjplus..olf_eligible_op3 t1 left outer join
	    &wsjplus..dma_data t2
	ON t1.zip_5 = t2.zip_code_var;
	
 quit;

 */

data &wsjplus..olf_data_all;

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

	length STREET_NUM 		 $200.;
	length ADDR 			 $200.;
	length ADDR2 			 $200.;
	length CITY 			 $200.;
	length STATE 			 $200.;
	length ZIP_5 			 $5.;
	length ZIP_PLUS_4 	     $4.;
	length POSTAL_CD 		 $12.;
	length COUNTRY 		 	 $100.;
    length RPT_REGION 		 $100.;
	length FREE_PAID_TYPE    $25.;
	length SBSCR_ID          8.;
	informat sbscr_id        11.;
	length WSJPLUS_SUBSCRIPTION_TYPE $40.;	

    set &wsjplus..olf_eligible_op3;

    /* where subscription_type ^= 'Migrated';
	   INCLUDING MIGRATED to join and get the address for mosaic billed subs
    */ 

	SOURCE = 'OLF';

	SBSCR_ID = acct_num&hash * 1;
	

	/* Modified to give pref to uu_id&hash and email address if the circ mosaic table have a record

	if uu_id&hash = ' ' then uu_id&hash = mcs_uu_id;

	if email_addr&hash = ' ' then email_addr&hash = mcs_email_addr;

	*/

	if mcs_uu_id_hash not in ('',"&hash_null") Then uu_id_hash = mcs_uu_id_hash;
	if mcs_uu_id  not in ('',"&hash_null") Then uu_id  = mcs_uu_id;
	if mcs_email_addr_hash  not in ('',"&hash_null") Then email_addr_hash = mcs_email_addr_hash;
	if mcs_email_addr   not in ('',"&hash_null") Then email_addr  = mcs_email_addr ;

	
	If mcs_sbscr_status = 'A' then mosaic_prod_cd = mcs_prod_cd;


	prod_cd = 'J';

	OFFER_PRICE = CUMUL_AMT_PAID;

	init_ord_cre_dt = SBSCR_STRT_DT;
	SBSCR_TERM_STRT_DT = intnx('day', RENWL_DT,1); /* how to add a day to date */

	/**************************
    If ( commit_expr_dt ^= '09SEP9999'd AND commit_expr_dt ^= '01JAN0001'd ) Then 
	   SBSCR_TERM_END_DT = commit_expr_dt;
	Else
	***************************/

	SBSCR_TERM_END_DT = EXPR_DT;

	SBSCR_STATUS = 'A';
	BILL_SYS_ID = 3;
	IS_MIGRATED = 'N';

	IF mcs_offer_type = 'ENTITLEMENTS TO PRINT' Then
	   new_print_stat = "BUNDLE";

	print_stat = new_print_stat;
 
	OFFER_TYPE = 'ULTIMATE PACKAGE';

	If subscription_type = 'Corporate' then BUSINESS_OWNER = 'CORPORATE US';
    If subscription_type = 'Educational' then BUSINESS_OWNER = 'EDUCATION';
    Else BUSINESS_OWNER = 'CONSUMER US';

	CHANNEL_2 = cur_src_grp2;
	COMPANY_NAME = CO_NAME;

	format INIT_ORD_CRE_DT date9.;
    format SBSCR_TERM_STRT_DT date9.;
    format SBSCR_TERM_END_DT date9.;
    format SBSCR_TRMNT_DT date9.;

	/* Calculate tenure */

	 length _yr $2.;
     length _yr2 $4.;
     length date $8.;
     length _calcdate $4.;

	 _yr=substr(acct_num&hash,5,2);
     _calcdate=substr(acct_num&hash,1,4);

     if _yr>='60' then _yr2=('19' || _yr);
     else _yr2= ('20' || _yr);
     date=(_yr2 || _calcdate);
     Start_date=date+0;

	 init_ord_cre_dt_var=input(put(Start_date,8.),yymmdd8.);
     format init_ord_cre_dt_var date9.;

	 todaydt = (&date_run.);
	 format todaydt date9.;
    
	 TENURE=ROUND( (intck('month',init_ord_cre_dt_var,todaydt)/12), 0.01) ;

	 If subscription_type = 'Complimentary' Then FREE_PAID_TYPE = 'FREE';
	 ELSE FREE_PAID_TYPE = 'PAID';

	 /* WSJPLUS subtype ??? */
   
	 If WSJPLUS_subscription_type in ( "Advertising Agcy", "AGENCY","Bulk","Canadian","Regular",
         "Corporate","Program Professors", "JIE TRANS", "CAP VERIFY", "CAP CONV","GIFT","Educational", "Employee" )
	 Then
	   WSJPLUS_ELIGIBLE = 1;
	 Else
       WSJPLUS_ELIGIBLE = 0;

   
run; 


data WP_OP.olf_subs_bck_&date_mmddyy;

   set &wsjplus..olf_data_all;
   where subscription_type ^= "Migrated";

run;


data &wsjplus..olf_subs
( 
KEEP = uu_id_hash uu_id email_addr_hash email_addr SBSCR_ID acct_num_hash acct_num PROD_CD MOSAIC_PROD_CD 
       FIRST_NAME LAST_NAME INIT_ORD_CRE_DT SBSCR_TERM_STRT_DT SBSCR_TERM_END_DT SBSCR_TRMNT_DT
       SBSCR_STATUS BILL_SYS_ID IS_MIGRATED AR_FLAG PAYMENT_TYPE OFFER_TYPE BUSINESS_OWNER 
       BUSINESS_OWNER_CL SUBSCRIPTION_TYPE CHANNEL CHANNEL_2
       FREQUENCY_DESC OFFER_PRICE COMPANY_NAME COUPON_CD
	   SOURCE PRINT_STAT
	   STREET_NUM ADDR ADDR2 CITY STATE ZIP_5 ZIP_PLUS_4 POSTAL_CD 
	   COUNTRY RPT_REGION CURR_SRCE_KEY
	   FREE_PAID_TYPE TENURE WSJPLUS_ELIGIBLE TENURE FREE_PAID_TYPE
	   WSJPLUS_SUBSCRIPTION_TYPE
 );

   set &wsjplus..olf_data_all;
   where subscription_type ^= "Migrated";

run;

