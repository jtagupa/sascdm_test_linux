 
 options compress=yes reuse=yes obs=max;



options obs=max compress=yes reuse=yes fmtsearch=(work table ) ls=64 ps=79
	mprint symbolgen ORIENTATION=portrait papersize=letter ;

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
     
%macro read_in(brand);
	data &brand;
		set stg.&brand._dailymf;
		
		ZIPCODE=substr(zipcode_12,1,5);
		ZIPPLUS4=substr(zipcode_12, 6,4);
		ZIPPLUS3=substr(zipcode_12, 10,3);

		*drop zipcode_12;

		SYSTEM_DATE=substr(system_dt,1,8);
		SYSTEM_TIME=substr(system_dt,9,6);

		*drop system_dt;

		TRANS_DATE=substr(trans_date_n_seq,1,7) +0; *!!!! doesnt look right ???;
		TRANS_SEQ_NUM=substr(trans_date_n_seq,8) +0;

		*drop trans_date_n_seq;

		TRANS_SOURCE=substr(trans_source_sub_source,1,1);
		TRANS_SUBSOURCE=substr(trans_source_sub_source,2,1);

		*drop trans_source_sub_source;

		ORIG_SALES_DATE=ORIGINAL_SALES_MONTH || ORIGINAL_SALES_YEAR;
		*drop ORIGINAL_SALES_MONTH ORIGINAL_SALES_YEAR;

*		SUB_START_DATE =START_CENTURY ||START_YEAR ||START_MONTH ||START_DAY;
		sub_start_date=mdy(start_month,start_day,compress(start_century||start_year));

		*drop START_CENTURY  START_YEAR START_MONTH;

		CURR_PREM1=substr(PREMIUM_CHOSEN,1,2);
		CURR_PREM2=substr(PREMIUM_CHOSEN,3,2);
		CURR_PREM3=substr(PREMIUM_CHOSEN,5,2);

		*drop PREMIUM_CHOSEN;

		QUAL_PREMIUM1=substr(PREMIUM_QUALIFICATION,1,1);
		QUAL_PREMIUM2=substr(PREMIUM_QUALIFICATION,2,1);
		QUAL_PREMIUM3=substr(PREMIUM_QUALIFICATION,3,1);

		*drop PREMIUM_QUALIFICATION;

		if bill_to_fields ne '' then do;
			bill_comp_name=substr(bill_to_fields,1,40);	
			bill_prefix=substr(bill_to_fields,41,9);
			bill_first_name	=substr(bill_to_fields,50,16);
			bill_last_name	=substr(bill_to_fields,66,23);
			bill_suffix=substr(bill_to_fields,89,9);
			bill_street_num	=substr(bill_to_fields,98,15);
			bill_street_nam	=substr(bill_to_fields,113,40);
			bill_extra_add	=substr(bill_to_fields,153,23);
			bill_city	=substr(bill_to_fields,176,14);
			bill_state	=substr(bill_to_fields,190,20);
			bill_zipcode	=substr(bill_to_fields,210,5);
			bill_zipplus4	=substr(bill_to_fields,215,4);
			bill_zipplus3	=substr(bill_to_fields,219,3);
			bill_country	=substr(bill_to_fields,222,3);
			donor_prod_cd	=substr(bill_to_fields,225,1);
			donor_acct_num	=substr(bill_to_fields,226,12);
			billto_num_ren	=substr(bill_to_fields,238,20);
			cre_num_adopt	=substr(bill_to_fields,258,3); *???double check;
			sponsor_info	=substr(bill_to_fields,261,1);
			bill_chg_add	=substr(bill_to_fields,262,1);
			bill_verify_add	=substr(bill_to_fields,263,10);
			bill_valid_add	=substr(bill_to_fields,273,4);
		end;
		
		if PURCHASE_ORDER_FLDS ne '' then do;
			PURCHASE_NUM =substr(PURCHASE_ORDER_FLDS,1,20);

			PURCHASE_NAME=substr(PURCHASE_ORDER_FLDS,21,23);
			PURCHASE_DATE=substr(PURCHASE_ORDER_FLDS,44,1);
			* drop PURCHASE_ORDER_FLDS ;
		end;

		format Optout_Reason $2. Optout_Date $8.;
		
		If WE_FIELDS ne '' then do;
			WE_COMPANY_NAME =substr(WE_FIELDS,1,40);
			WE_PREFIX = substr(WE_FIELDS,41,9);
			WE_FIRSTNAME =substr(WE_FIELDS,50,16);
			WE_LASTNAME= substr(WE_FIELDS,66,23);
			WE_SUFFIX = substr(WE_FIELDS,89,9);
			WE_STREET_NO =substr(WE_FIELDS,98,15);
			WE_STREET_NAME =substr(WE_FIELDS,113,40);
			WE_EXTRA_ADDR =substr(WE_FIELDS,153,23);
			WE_CITY = substr(WE_FIELDS,176,14);
			WE_STATE =substr(WE_FIELDS,190,2);
			WE_ZIPCODE =substr(WE_FIELDS,192,5);
			WE_ZIPPLUS4 = substr(WE_FIELDS,197,4);
			WE_ZIPPLUS3 =substr(WE_FIELDS,201,3);
			WE_COUNTRY_CODE=substr(WE_FIELDS,214,3);
			WE_CARRIER_NUM = substr(WE_FIELDS,217,18);
			WE_CARRIER_INST =substr(WE_FIELDS,235,23);
			WE_MAIL_REASON = substr(WE_FIELDS,258,2);
			WE_PRINT_CENTER = substr(WE_FIELDS,260,2);
			WE_REQUEST_ED = substr(WE_FIELDS,262,2);
			WE_OLF_EDITION= substr(WE_FIELDS,264,2);
			WE_CHG_ADD_RSN =substr(WE_FIELDS,266,1);
			WE_VALID_ADD_CD = substr(WE_FIELDS,267,4);
			WE_POSTAL_ROUTE =substr(WE_FIELDS,271,5);
			WE_LINE_OF_TRVL=substr(WE_FIELDS,276,5);
			WE_WALK_SEQ_NO=substr(WE_FIELDS,281,2);
			Optout_Reason=substr(WE_FIELDS,283,2);
			Optout_Date=substr(WE_FIELDS,285,8);
			
			*drop WE_FIELDS;
		end;

	run;
%mend read_in; 

%read_in(wsj);


data mypwolf3;
	set  wsj;

			length Print_status $25.;
		length bill_subplan $1.;
		length cust_status $10.;
				
		bill_subplan=substr(billplan,1,2);

		if Stop_Date=0 then Cust_Status='Live';
		else if Stop_Date ^=0 & substr(Stop_Reason,1,1) ='6'
		then Cust_Status='Suspended';
		else if Stop_Date =99999999 then Cust_Status='Adv Start';
		else if Stop_Date ^=0 then Cust_Status='Stopped';
		
		if Icu_Ind in ('D','W') & substr(Online_Acct_No,1,1) not in ("CC"," ")
		then Print_status="Combo Registered";
		else if Icu_Ind in ('D','W') & substr(Online_Acct_No,1,1) in ("CC"," ") 
		then Print_status="Combo Unregistered";
		else if Icu_Ind in ('Q') then Print_status="ComboJIE";
		else if Icu_Ind ^ in ('D','Q','W') & substr(Online_Acct_No,1,1) not = " "
		then Print_status="Dual";
		else Print_status="Print Only";

run;

data price_&date_mmddyy.; 
set mypwolf3(keep=Prod_Code 
	Acct_No acct_no_hash
	Company Prefix	Firstname Lastname	Suffix Street_number 
	Street_name	Extra_address City State Zipcode Zipplus4 Zipplus3 Country_Code term_commmit	rate_commmit
	User_ID	No_Sell_Type Carrier_Num Sub_Type Last_Sub_Type Num_Copies install_code commit_date
	Curr_Source_Key	Expire_Key Orig_Source Current_Term	Sub_Start_Date Payment_Rec install_paid	
	Renewals Bill_Renewals Renewal_Date Expire_Date	Stop_Date Stop_Reason Bus_Phone total_install
	Home_Phone Home_Unlist Refund_Reason Bill_Cycle_Ind Bill_Effort	Rate_Delvy_Typ spc_postage
	Series_ID Series_ID_Stat Billplan date_paid cumm_sub_amt Effort_Paid_On	cumm_spc_post
	curr_pay_stat credit_card_typ Icu_Ind Corp_Acct_No Online_Acct_No Optout_Reason 
	address_type Addr_Source Addr_Source_Typ Multi_Buyer_Ind Cust_Status Bus_Phone
	Home_Phone Home_Unlist No_Sell_Type Group_Sale_CD ABC_Rpt_Type System_Date Trans_Date);		
length MF_Match_Key WE_Match_Key $10 multi_seg $20.;




	/*%** Further sub def of sub type;
	if substr(billplan,1,1)='1' then Last_Sub_Type='??';
	if Last_Sub_Type ='7S' & substr(Curr_Source_Key,1,3)='0EW' then Last_Sub_Type='WR';
	if  (substr(Curr_Source_Key,1,2)='7K' | Curr_Source_Key in ('75KAP','75RX','75MCAT','75YG')) then Last_Sub_Type='KP';
	if Curr_Source_Key ='0ECRE' then Last_Sub_Type='CR';

	if Last_Sub_Type ='7S' & Curr_Source_Key = '0ESEMP' then Last_Sub_Type='EW';
	else if Last_Sub_Type ='7S' & Curr_Source_Key ^='0ESEMP' & substr(Curr_Source_Key,1,3)='0ES' then Last_Sub_Type='WW';
	else Last_Sub_Type=Last_Sub_Type;*/

	optout=(Optout_Reason ^=" ");
	
	if Address_Type='C' & optout=0 then Address_Type2='B';
	else if optout=1 then Address_Type2='C';
	else Address_Type2=Address_Type;
 
	length MF_Match_Key WE_Match_Key $10 multi_seg bus_resid $20.;

	%** Create Multi-Segmentation for field ops report;
	if (substr(billplan,1,1)='1') then multi_seg="Corporate";
	else if (sub_type='4M') then multi_seg="Inst Sales";
	else if num_copies > 1 then multi_seg="Multi Copy";
	else multi_seg="Remaining";

	%** Create Business Residential Flag;
	/*bus_resid=put(acct_No,$busres.);
	if bus_resid not in ('Business','Residential') then do;
		if Company ^=" " then bus_resid="Business";
		else bus_resid="Residential";
	end; 
*/
	%** Create Carrier Mail Flag;

	Carrier_Flag=(substr(Carrier_Num,1,1) ^=" ");
	WE_Carrier_Flag=(substr(WE_Carrier_Num,1,1) ^=" ");

	%** Create drop;
	MF_Match_Key=substr(Carrier_Num,1,10);
	WE_Match_Key=substr(WE_Carrier_Num,1,10);

	format Print_Center $pctr. 
	WE_Carrier_Flag Carrier_Flag carrier. Address_Type2 $Addtyp.;
run;


data price_&date_mmddyy; set price_&date_mmddyy;
	length term_1 $3.;
	length SK $2.;
	length expire_yr $4;
	length expire_mth $2;
	length Renewal_grp $15.;
	length  subscription_Type $22.;
	length Src_grp2 $12.;
	length  Carrier $10.;
	length  Print_Ctr $30.;
	length  Payment $20.;
	length AR_flag $8.;

	if billplan in('3A','3P') then AR_flag = 'AR';
	else AR_flag ='Non AR';
	
	
	current_term1=current_term+0;
	
	IF 000<=CURRENT_TERM1<= 035 THEN Term_1='5'; 
	IF 36<=CURRENT_TERM1<= 070 THEN Term_1='8'; 
	IF 071<=CURRENT_TERM1<= 098 THEN Term_1='13'; 
	IF 099<=CURRENT_TERM1<= 140 THEN Term_1='17'; 
	IF 141<=CURRENT_TERM1<= 182 THEN Term_1='26';
	IF 183<=CURRENT_TERM1<= 315 THEN Term_1='30';
	IF 316<=CURRENT_TERM1<= 537 THEN Term_1='52';
	IF 538<=CURRENT_TERM1 THEN Term_1='104'; 

	if substr(Curr_Source_Key,1,1)='9' then SK=substr(Curr_Source_Key,1,2);
	else if Substr(Curr_Source_Key,1,1)^='9' then SK=substr(Curr_Source_Key,1,1);


	if renewals=0 then Renewal_grp='Acqu_Unpaid';
	if renewals=1 then Renewal_grp='Acqu_Paid';
	if renewals=2 then Renewal_grp='Conversions';
	if renewals=3 then Renewal_grp='1st Renewal';
	if renewals=4 then Renewal_grp='2nd Renewal';
	if renewals>=5 then Renewal_grp='3+ Renewals';

	expire_yr=COMPBL(substr(expire_date,5,4));
	expire_mth=COMPBL(substr(expire_date,9,2));
	_expire_yr=expire_yr+0;
	_expire_mth=expire_mth+0;
	Src_grp=put(SK,$sd.); 
	Subscription_Type=put(Last_Sub_Type,$subtyp.);
	term_wk=term_1+0;
	Carrier=put(carrier_flag,carrier.);
	Print_Ctr=put(Print_Center,$pctr.); 
	payment=put(curr_pay_stat,$paytyp.);
	Last_Sub_Type2=last_Sub_Type;

	if Last_Sub_Type2='1P' then Src_grp2='MA';
	else if Last_Sub_Type2 in ('1X','1G') then Src_grp2='GIFT';
	else if Last_Sub_Type2 in ('1T','1S') then Src_grp2='JIE TRANS';
	else Src_grp2=Src_grp;

	if ABC_Rpt_Type in ('B','H') then ABC_status='Traditional';
	else if ABC_Rpt_Type in ('L') then ABC_status='Discount';
	else ABC_status='Unknown';
    run;
  
data price_&date_mmddyy; set price_&date_mmddyy.;
length _yr $2.;
length _yr2 $4.;
length date $8.;
length _calcdate $4.;
_yr=substr(Acct_No,5,2);
_calcdate=substr(Acct_No,1,4);

if _yr>='60' then _yr2=('19' || _yr);
else _yr2= ('20' || _yr);
date=(_yr2 || _calcdate);
Start_date=date+0;
run;

data price_&date_mmddyy.; set price_&date_mmddyy.;
length SK2 $2.;
	length datepd_yr $4;
	length datepd_mth $2;
	length startdate_yr $4;
	length startdate_mth $2;
	length Ori_Src_grp $12.;
	length Ori_Src_grp2 $12.;
	Length Q1$6.;

	if substr(Orig_Source,1,1)='9' then SK2=substr(Orig_Source,1,2);
	else if Substr(Orig_Source,1,1)^='9' then SK2=substr(Orig_Source,1,1);

	datepd_yr=COMPBL(substr(date_paid,5,4));
	datepd_mth=COMPBL(substr(date_paid,9,2));
	_datepd_yr=datepd_yr+0;
	_datepd_mth=datepd_mth+0;
	startdate_yr=COMPBL(substr(start_date,5,4));
	startdate_mth=COMPBL(substr(start_date,9,2));
	_startdate_yr=startdate_yr+0;
	_startdate_mth=startdate_mth+0;
	Ori_Src_grp=put(SK2,$sd.); 

	if Last_Sub_Type2='1P' then Ori_Src_grp2='MA';
	else if Last_Sub_Type2 in ('1X','1G') then Ori_Src_grp2='GIFT';
	else if Last_Sub_Type2 in ('1T','1S') then Ori_Src_grp2='JIE TRANS';
	else Ori_Src_grp2=Ori_Src_grp;
	
	Q1=startdate_yr||startdate_mth;
  
run;

data price_&date_mmddyy.; set price_&date_mmddyy.;
	length stop_yr $4;
	length stop_mth $2;
	Length Q1stop $6.;

stop_yr=COMPBL(substr(stop_date,5,4));
	stop_mth=COMPBL(substr(stop_date,9,2));
	_stop_yr=stop_yr+0;
	_stop_mth=stop_mth+0;
	
Q1stop=stop_yr||stop_mth;
exp_date=expire_yr||expire_mth;
date_pd=datepd_yr||datepd_mth;

length CK2 $2.;
length cur_Src_grp $12.;
length cur_Src_grp2 $12.;

if substr(curr_Source_key,1,1)='9' then CK2=substr(curr_Source_key,1,2);
else if Substr(curr_Source_key,1,1)^='9' then CK2=substr(curr_Source_key,1,1);
cur_Src_grp=put(CK2,$sd.); 

	if Last_Sub_Type2='1P' then cur_Src_grp2='MA';
	else if Last_Sub_Type2 in ('1X','1G') then cur_Src_grp2='GIFT';
	else if Last_Sub_Type2 in ('1T','1S') then cur_Src_grp2='JIE TRANS';
	else cur_Src_grp2=cur_Src_grp;
run;
data price_&date_mmddyy.; set price_&date_mmddyy.;
	length csk $2.;
	length 	Print_status $25.;
	length 	Print_status2 $25.;
	length 	new_print_stat $25.;
	length cust_status $10.;

	csk=substr(Curr_Source_Key,2,3);


				if Icu_Ind in ('D','W') & substr(Online_Acct_No,1,1)  in ("A")
		then Print_status="Combo Registered";
		else if Icu_Ind in ('D','W') & substr(Online_Acct_No,1,1) not in ("A") 
		then Print_status="Combo Unregistered";
		else if Icu_Ind in ('Q') then Print_status="ComboJIE";
		else if Icu_Ind ^ in ('D','Q','W') & substr(Online_Acct_No,1,1) = "A"
		then Print_status="Dig Ent/Dual";
		else Print_status="Print Only";

		
		if Icu_Ind in ('D','Q','W') & substr(Online_Acct_No,1,1) in ("A")
		then Print_status2="Registered";
		else if Icu_Ind in ('D','Q','W') & substr(Online_Acct_No,1,1) not in ("A")  
		then Print_status2="Unregistered";
		else Print_status2="Other";

		
		if icu_ind in ("D") then new_print_stat='COMBO';
		else if icu_ind in ("W") then new_print_stat='BUNDLE';
		else if icu_ind in ("Q") then new_print_stat='ED BUNDLE';
			else if icu_ind ^ in ("D","W","Q")& substr(Online_Acct_No,1,1) = "A" then new_Print_stat="DIG ENT/DUAL";
	 	else new_print_stat="Print Only";
		run;


data price_&date_mmddyy.; set price_&date_mmddyy.;
tenure=input(put(Start_date,8.),yymmdd8.);
format tenure date9.;
length todaydate 8;
todaydatex=input("&date_mmddyy.",mmddyy8.);
todaydate=put(year(todaydatex),z4.)||put(month(todaydatex),z2.)||put(day(todaydatex),z2.);
date2=input(put(todaydate,8.),yymmdd8.);
format date2 date9.;
numdays=intck('day',tenure,date2);
numyrs=intck('year',tenure,date2);
nummth=(intck('month',tenure,date2)/12);
nummthb=(intck('month',tenure,date2));
run;


data price_&date_mmddyy._actives; set price_&date_mmddyy.; where cust_status ^ = "Stopped" ; run;

data price_&date_mmddyy._stops; set price_&date_mmddyy.; where cust_status = "Stopped"; run;
data price_&date_mmddyy._stops;  set price_&date_mmddyy._stops;
tenure=input(put(Start_date,8.),yymmdd8.);
format tenure date9.;
stop_date2=input(put(stop_date,8.),yymmdd8.);
format stop_date2 date9.;
numdays=intck('day',tenure,stop_date2);
numyrs=intck('year',tenure,stop_date2);
nummth=(intck('month',tenure,stop_date2)/12);
nummthb=(intck('month',tenure,date2));
run;
data price_&date_mmddyy._final; set price_&date_mmddyy._actives price_&date_mmddyy._stops; 
run;
data work.price_&date_mmddyy._final; set price_&date_mmddyy._final;
run;
data price_&date_mmddyy._final; set work.price_&date_mmddyy._final;
if substr(Last_Sub_Type,1,1) ='4' & (substr(Curr_Source_Key,1,2)in ('7A','7B','7I','7K','7M'))
then Subscription_type='Education Other';
run;

proc sort data= price_&date_mmddyy._final; by Curr_Source_Key; run;
proc sort nodupkey data=slow_chg.Wsj_cap_agency_043016; by Curr_Source_Key; run;

data price_&date_mmddyy._final;
merge price_&date_mmddyy._final (in=in1) slow_chg.Wsj_cap_agency_043016 (in=in2 keep=Curr_Source_Key flip);
by Curr_Source_Key;
if in1;
run;

data price_&date_mmddyy._final; set price_&date_mmddyy._final;
if flip="CAP" then subscription_type="CAP";
else subscription_type=subscription_type;

if substr(billplan,1,1)='1' then Subscription_type='Corporate';
else Subscription_Type= Subscription_Type;
run;

data price_&date_mmddyy._final; set price_&date_mmddyy._final;
if (substr(Curr_Source_Key,1,4 = ("9CLP")))then cur_Src_grp2="CAP";
else cur_Src_grp2=cur_Src_grp2;

if  cur_Src_grp2= "CAP" then subscription_type="CAP";
else subscription_type=subscription_type;
run;



data price_&date_mmddyy._final; set price_&date_mmddyy._final;
if prod_code = ('J') then do;
if term_commmit>0 and commit_date ^ = 99999999 then do;
         rate=round(rate_commmit/num_copies,1);
		 term=term_commmit;

end;

else do;

          rate=round(cumm_sub_amt/num_copies,1);
		  term=current_term;

end;

end;


if 0<=term<=35 then term_week=5;
else if 36<=term<=70 then term_week=8;
else if 71<=term<=98 then term_week=13;
else if 99<=term<=140 then term_week=17;
else if 141<=term<=182 then term_week=26;
else if 183<=term<=315 then term_week=30;
else if 316<=term<=537 then term_week=52;
else if term>=538 then term_week=104;

rate3=rate;
rate3 = (360/term)*rate;
rate3=Round(rate3,1);


	length Price_range4 $22.;
if term_week=104 then rate3=rate;
if rate_commmit > 0 then rate3 = rate_commmit;
	
/*if rate3 < 99 then Price_range3 = "< $99";
else if rate3 >= 99 and rate3 < 125 then Price_range3 = "$119";
else if rate3 >= 125 and rate3 < 145 then Price_range3 = "$140";
else if rate3 >= 145 and rate3 < 195 then Price_range3 = "$181";
else if rate3 >= 195 and rate3 < 235 then Price_range3 = "$207";
else if rate3 >= 235 And rate3 < 285 then Price_range3 = "$249";
else if rate3 >= 285 And rate3 < 350 then Price_range3 = "$299";
else if rate3 >= 350 And rate3 < 400 then Price_range3 = "$363";
else if rate3 >= 400 And rate3 < 425 then Price_range3 = "$424";
else if rate3 >= 425 And rate3 < 453 then Price_range3 = "$452";
else if rate3 >= 453 And rate3 < 490 then Price_range3 = "$489";
/*else if rate2 >= 400 then Price_range2 = "$441";
else Price_range3 = "490+";


if rate3 < 99 then Price_range4= "< $99";
else if rate3 >= 99 and rate3 < 125 then Price_range4= "$119";
else if rate3 >= 125 and rate3 < 145 then Price_range4= "$140";
else if rate3 >= 145 and rate3 < 195 then Price_range4= "$187";
else if rate3 >= 195 and rate3 < 235 then Price_range4= "$214";
else if rate3 >= 235 And rate3 < 285 then Price_range4= "$259";
else if rate3 >= 285 And rate3 < 350 then Price_range4= "$299";
else if rate3 >= 350 And rate3 < 400 then Price_range4= "$374";
else if rate3 >= 400 And rate3 < 425 then Price_range4= "$413";
else if rate3 >= 425 And rate3 < 460 then Price_range4= "$455";
else if rate3 >= 460 And rate3 < 502 then Price_range4= "$501";
else Price_range4= ">$501";*/


/*if rate3 < 99 then Price_range4= "$99 and below";
else if rate3 >= 99 and rate3 < 125 then Price_range4= "$119";
else if rate3 >= 125 and rate3 < 145 then Price_range4= "$140";
else if rate3 >= 145 and rate3 < 195 then Price_range4= "$187";
else if rate3 >= 195 and rate3 < 235 then Price_range4= "$214";
else if rate3 >= 235 And rate3 < 300 then Price_range4= "$259";
else if rate3 >= 300 And rate3 < 323 then Price_range4= "$309";
else if rate3 >= 323 And rate3 < 360 then Price_range4= "$324";
else if rate3 >= 360 And rate3 < 400 then Price_range4= "$374";
else if rate3 >= 400 And rate3 < 425 then Price_range4= "$413";
else if rate3 >= 425 And rate3 < 460 then Price_range4= "$455";
else if rate3 >= 460 And rate3 < 503 then Price_range4= "$502";
else Price_range4= ">$502";*/

/*if rate3 < 99 then Price_range4= "$99 and below";
else if rate3 >= 99 and rate3 < 125 then Price_range4= "$119";
else if rate3 >= 125 and rate3 < 145 then Price_range4= "$140";
else if rate3 >= 145 and rate3 < 195 then Price_range4= "$187";
else if rate3 >= 195 and rate3 < 235 then Price_range4= "$214";
else if rate3 >= 235 And rate3 < 300 then Price_range4= "$259";
else if rate3 >= 300 And rate3 < 323 then Price_range4= "$309";
else if rate3 >= 323 And rate3 < 360 then Price_range4= "$324";
else if rate3 >= 360 And rate3 < 400 then Price_range4= "$374";
else if rate3 >= 400 And rate3 < 425 then Price_range4= "$413";
else if rate3 >= 425 And rate3 < 460 then Price_range4= "$455";
else if rate3 >= 460 And rate3 < 503 then Price_range4= "$502";
else Price_range4= ">$502";*/

/*if rate3 < 99 then Price_range4= "$99 and below";
else if rate3 >= 99 and rate3 < 100 then Price_range4= "$99";
else if rate3 >= 100 and rate3 < 125 then Price_range4= "$119";
else if rate3 >= 125 and rate3 < 145 then Price_range4= "$140";
else if rate3 >= 145 and rate3 < 150 then Price_range4= "$149";
else if rate3 >= 150 and rate3 < 180 then Price_range4= "$179";
else if rate3 >= 180 and rate3 < 195 then Price_range4= "$187";
else if rate3 >= 195 and rate3 < 235 then Price_range4= "$214";
else if rate3 >= 235 And rate3 < 300 then Price_range4= "$259";
else if rate3 >= 300 And rate3 < 323 then Price_range4= "$309";

else if rate3 >= 323 And rate3 < 346 then Price_range4= "$324";
else if rate3 >= 346 And rate3 < 360 then Price_range4= "$348";
else if rate3 >= 360 And rate3 < 392 then Price_range4= "$374";
else if rate3 >= 392 And rate3 < 400 then Price_range4= "$396";

else if rate3 >= 400 And rate3 < 425 then Price_range4= "$413";
else if rate3 >= 425 And rate3 < 460 then Price_range4= "$455";
else if rate3 >= 460 And rate3 < 503 then Price_range4= "$502";
else Price_range4= ">$502";*/


/*012215*/
/*if rate3 < 99 then Price_range4= "$99 and below";
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
else Price_range4= ">$502"; */

/*032516*/

/*if rate3 < 99 then Price_range4= "$99 and below";
else if rate3 >= 99 and rate3 < 101 then Price_range4= "$99";
else if rate3 >= 101 and rate3 < 125 then Price_range4= "$119";
else if rate3 >= 125 and rate3 < 145 then Price_range4= "$140";
else if rate3 >= 145 and rate3 < 152 then Price_range4= "$149";
else if rate3 >= 152 and rate3 < 173 then Price_range4= "$165";
else if rate3 >= 173 and rate3 < 175 then Price_range4= "$174Save";
else if rate3 >= 175 and rate3 < 180 then Price_range4= "$179";
else if rate3 >= 180 and rate3 < 198 then Price_range4= "$187";
else if rate3 >= 198 and rate3 < 199 then Price_range4= "198";

else if rate3 >= 199 And rate3 < 323 then Price_range4= "$259";
else if rate3 >= 323 And rate3 < 346 then Price_range4= "$324";
else if rate3 >= 346 And rate3 < 360 then Price_range4= "$348";
else if rate3 >= 360 And rate3 < 392 then Price_range4= "$374";
else if rate3 >= 392 And rate3 < 400 then Price_range4= "$396";

else if rate3 >= 400 And rate3 < 419 then Price_range4= "$413";
else if rate3 >= 419 And rate3 < 431 then Price_range4= "$420";
else if rate3 >= 431 And rate3 < 460 then Price_range4= "$455";
else if rate3 >= 460 And rate3 < 518 then Price_range4= "$501";
else if rate3 >= 519 And rate3 < 526 then Price_range4= "$525";
else Price_range4= ">$525"; 
*/

/*012917*/
if rate3 < 99 then Price_range4= "$99 and below";
else if rate3 >= 99 and rate3 < 101 then Price_range4= "$99";
else if rate3 >= 101 and rate3 < 125 then Price_range4= "$119";
else if rate3 >= 125 and rate3 < 145 then Price_range4= "$140";
else if rate3 >= 145 and rate3 < 152 then Price_range4= "$149";
else if rate3 >= 152 and rate3 < 173 then Price_range4= "$165";
else if rate3 >= 173 and rate3 < 175 then Price_range4= "$174Save";
else if rate3 >= 175 and rate3 < 180 then Price_range4= "$179";
else if rate3 >= 180 and rate3 < 198 then Price_range4= "$187";
else if rate3 >= 198 and rate3 < 199 then Price_range4= "198";

else if rate3 >= 199 And rate3 < 323 then Price_range4= "$259";
else if rate3 >= 323 And rate3 < 346 then Price_range4= "$324";
else if rate3 >= 346 And rate3 < 360 then Price_range4= "$348";
else if rate3 >= 360 And rate3 < 392 then Price_range4= "$374";
else if rate3 >= 392 And rate3 < 400 then Price_range4= "$396";

else if rate3 >= 400 And rate3 < 419 then Price_range4= "$413";
else if rate3 >= 419 And rate3 < 431 then Price_range4= "$420";

else if rate3 >= 431 And rate3 < 452 then Price_range4= "$443";

else if rate3 >= 452 And rate3 < 460 then Price_range4= "$455";
else if rate3 >= 460 And rate3 < 518 then Price_range4= "$501";
else if rate3 >= 519 And rate3 < 526 then Price_range4= "$525";
else Price_range4= ">$525"; 


if CUMM_SUB_AMT in (44.75,179) then Price_range4= "$179"; 
if CUMM_SUB_AMT in (32.99) then Price_range4= "$396"; 
if CUMM_SUB_AMT in (35,105) then Price_range4= "$420"; 
if CUMM_SUB_AMT in (43.75,131.25) then Price_range4= "$525"; 

if (substr(curr_Source_key,1,2)in ("9A","9B","92","93","94","9P","9A","9B","9R","9T","9J","9L","9K","9N","9H","9S","9E","9F","91","95"))then cur_Src_grp2="RETENTION";
else cur_Src_grp2=Src_grp;

/*if (substr(curr_Source_key,1,2)in ("9J","9L","9K"))then cur_Src_grp2="HULBERT";
else cur_Src_grp2=Src_grp;*/

if Curr_Source_Key in("9BFSB4","9AFSB3","9PFSP4","98FSP3","9BFBC2","9PFPC2","9BFBC3","9PFPC3","9AFBC5","94FPC5","9BFCS2","9PFCS2",
"9LBNA2","9BPNA3","9LCSR1","93CSR1","9RBNA4","98PNA5","9LCSR2","92CSR2")
then cur_Src_grp2="SOLUTIONS" ;
else cur_Src_grp2=cur_Src_grp2;

if Curr_Source_Key in("9LBCC1","9LBCC2","9LBCC5","9LBCC3","9LBCC4","9JBCC5","9JNARJ",
	"9JBCC7","9BFAR1","9BFAR2","9BFAR9","9BFAR3","9BFAR4","9AFAR5","9AFARJ","9AFAR7",
"9PFAR1","9PFAR2","9PFAR9","9PFAR3","9PFAR4","9PFAR8","94FAR6")
then cur_Src_grp2="CALL CTR";
if Curr_Source_Key in("9C3PCJ","9C3PER","9C3PHJ","9C3PKJ","9C3PSJ","9C7PBJ","9C7PCJ","9CEREW")
then cur_Src_grp2="REWARDS";
else cur_Src_grp2=cur_Src_grp2;
run;
/*data test; set price_032516_final; where rate3 in (26,49.50,43.75); run;*/
data price_&date_mmddyy._final; set price_&date_mmddyy._final; 
if substr(Curr_Source_Key,2,1)="T" then cur_Src_grp2= "SAT Only";
else cur_Src_grp2= cur_Src_grp2; 

if flip = "AGENCY" then cur_Src_grp2= "AGENCY";
else cur_Src_grp2= cur_Src_grp2; 

if cur_Src_grp2= "AGENCY" then subscription_type="AGENCY";
else subscription_type=subscription_type;

if cur_Src_grp2= "SAT Only" then subscription_type="WKND ONLY";
else subscription_type=subscription_type;
run;

data price_&date_mmddyy._final; set price_&date_mmddyy._final;
length Payment_Type $8.;
length Frequency_Desc $15.;
if curr_pay_stat in ("0","2") then Payment_Type = "OTHER";
else if curr_pay_stat in ("1") then Payment_Type = "CASH";
else if curr_pay_stat in ("3") then Payment_Type = "CC";
else payment_type = "OTHER";

if term_wk in (5,8) then Frequency_Desc = "Monthly";
else if term_wk in (13,17) then Frequency_Desc = "Quarterly";
else if term_wk in (26,30) then Frequency_Desc = "Semi_Annual";
else if term_wk in (52) then Frequency_Desc = "Annual";
else if term_wk in (104) then Frequency_Desc = "2 Year";
else Frequency_Desc = "OTHER";
run;


data price_&date_mmddyy._final; set price_&date_mmddyy._final;

		length new_print_stat2 $25.;

		if icu_ind in ("D") then new_print_stat2='COMBO';
		else if icu_ind in ("W") then new_print_stat2='BUNDLE';
		else if icu_ind in ("Q") then new_print_stat2='ED BUNDLE';
			else if icu_ind ^ in ("D","W","Q")& substr(Online_Acct_No,1,1) = "A" then new_Print_stat2="DIG ENT/DUAL";
	 	else new_print_stat2="Print Only";
Run;


data price_&date_mmddyy._final; set price_&date_mmddyy._final;
length Cust_Status3 $25.;
if prod_code = ('J') then do;
	if term_commmit>0 and commit_date ^ = 99999999  then do;
		if cust_status="Live" and 20170112 >= commit_date then Cust_Status3='Grace'; 
		else Cust_Status3=cust_status;

	end;

	else do;

		if cust_status="Live" and '12jan2017'd >= expire_date then Cust_Status3='Grace'; 
		else Cust_Status3=cust_status;

	end;

end;
run;



data work.price_&date_mmddyy._final; set price_&date_mmddyy._final; run;
proc freq data = work.price_&date_mmddyy._final; tables subscription_type *cur_Src_grp2; where cust_status3 = "Live"; run;
proc freq data=price_&date_mmddyy._final; tables cust_status*cust_Status3 / list;run;


data test2;
set price_&date_mmddyy._final;
where cust_status='Live';
keep commit_date expire_date cust_Status3 cust_Status todaydate date2;
run;

%let dsnout=work.wsjemail_&date_mmddyy.; 


******************************************************************************
*  STEP 2: Read ER2OLF  File                                                 *
******************************************************************************;

 
data myemail;
   	set stg.LOADFILE;
	label	Acct_No="Customer Unique Account Number"
            Prod_Code="Product Code"
			Email_Address="Subscribers Email Address"
			Renew_Via_Email="Renewal Subscription via email"
			Req_Ques_Via_Email="Receive Questioniare via email"
			Req_Djps_Via_Email="Receive Solicitations via email"  
			Wsjie_Sub="Subscription to WSJIE Indicator";
run;



data &dsnout ;
	set myemail (where=(Prod_Code='J' &  
	(email_address ^ ? 'spam' & email_address ^ ? 'abuse' &
	email_address ^ ? 'dowjones' & email_address ^ ? 'DOWJONES' &
	email_address ^ ? 'wsj' & email_address ^ ? 'WSJ' &
	email_address ^ ? 'fmr' & email_address ^ ? 'FMR' &
	email_address ^ ? 'factiva' & email_address ^ ? 'FACTIVA' &
	email_address ^ ? 'SPAM' & email_address ^ ? 'ABUSE')) ) ;
	keep Acct_No acct_no_hash
	 Email_Address email_address_hash Req_Djps_Via_Email Req_Ques_Via_Email;
run;

data work.wsjemail_&date_mmddyy.; set work.wsjemail_&date_mmddyy.; email_address=(email_address); run;
data work.wsjemail_&date_mmddyy.; set work.wsjemail_&date_mmddyy.; email_address=(email_address); run;
proc sort nodupkey data=work.wsjemail_&date_mmddyy.; by email_address_hash; run;
data work.tmp_email; set work.wsjemail_&date_mmddyy.; run;
data work.wsjemail_&date_mmddyy.; set work.wsjemail_&date_mmddyy.; where email_address_hash  not in (' ',"&hash_null"); run;


data work.wsjemail_&date_mmddyy.; set work.wsjemail_&date_mmddyy.; where acct_no ^="000000000000"; run;


PROC SORT NODUPKEY DATA=work.wsjemail_&date_mmddyy.; BY acct_no_hash; RUN;
PROC SORT NODUPKEY DATA=work.price_&date_mmddyy._final; BY acct_no_hash; RUN;

data work.price_&date_mmddyy._final_email;
merge work.price_&date_mmddyy._final(in=in1) work.wsjemail_&date_mmddyy.(in=in2);
by acct_no_hash;
if in1;
run;

data work.price_&date_mmddyy._final_email; set work.price_&date_mmddyy._final_email;
where subscription_type ^= "Migrated";
run;

%macro skip;
data _price (keep=start fmtname type label);
	set work.price_&date_mmddyy._final_email(rename=(Acct_No_hash=start)); /*(prevmonth)*/;
	length label $25.;
	label="KEEP";
	fmtname='post';
	type='C';
run;
proc sort data=_price nodupkey;
	by start fmtname;
run;
proc format library=work cntlin=_price;
run;


data OLFtomcs; 
set stg.mosaic_cust_subscription; 
/*where (PRINT_ACCT_NUM,Post.)='KEEP';*/
Match= put(PRINT_ACCT_NUM_hash,Post.)='KEEP';
run;

data OLFtomcs; set OLFtomcs; where match = 1; run; 

%mend skip;


	proc sort data=work.price_&date_mmddyy._final_email (keep=acct_no_hash acct_no)
		out=_price;
		by  acct_no_hash;
	run;

		proc sort data=stg.mosaic_cust_subscription(keep=PRINT_ACCT_NUM_hash PRINT_ACCT_NUM   uu_id_hash uu_id 
								   prod_cd SBSCR_CRE_DT)
			out=sorted_mcs;
				by PRINT_ACCT_NUM_hash;
				where PRINT_ACCT_NUM_hash ne '' and PRINT_ACCT_NUM_hash ne "&hash_null";
		run;


data OLFtomcs;
		merge sorted_mcs(in=a )
			_price(in=b rename=(acct_no_hash=PRINT_ACCT_NUM_hash
                          acct_no = print_Acct_num));
		by  PRINT_ACCT_NUM_hash;
		
		if a and b;
	run;


proc sort data = OLFtomcs; by UU_ID_hash; run;

data OLFtomcs2; set OLFtomcs; run;
proc sort data=OLFtomcs2; by UU_ID_hash descending prod_cd SBSCR_CRE_DT; run;
proc sort nodupkey; by UU_ID_hash; run;

data OLFtomcs2; set OLFtomcs2;
length acct_no $200.;
length acct_no_hash $128.;
acct_no = PRINT_ACCT_NUM;
acct_no_hash = PRINT_ACCT_NUM_hash;

run;


proc sort nodupkey data = work.price_&date_mmddyy._final_email; by acct_no_hash; run;
proc sort nodupkey data = OLFtomcs2; by acct_no_hash; run;
/* file 1 match by printacct OLFtomcs2 is all OLf with uuId's from Print acct num */
data work.price_&date_mmddyy._final_acct; 
merge work.price_&date_mmddyy._final_email (in=in1) OLFtomcs2 (in=in2 keep = acct_no acct_no_hash uu_id uu_id_hash);
by acct_no_hash;
if in1 and in2;
run;

/* file 2 match by email OLFtomcs2 is all OLf without uuId's from Print acct num match by email into Mosaic*/

proc sort nodupkey data = work.price_&date_mmddyy._final_email; by acct_no_hash; run;
proc sort nodupkey data = work.price_&date_mmddyy._final_acct; by acct_no_hash; run;

data work.price_&date_mmddyy._final_email2; 
merge work.price_&date_mmddyy._final_email (in=in1) work.price_&date_mmddyy._final_acct (in=in2 keep = acct_no_hash);
by acct_no_hash;
if in1 and not in2;
run;
/* file 4 no uuid or emai match no mosaic find*/
data work.price_&date_mmddyy._final_noemail; set work.price_&date_mmddyy._final_email2;
where email_address_hash   in (' ',"&hash_null");
run;
data work.price_&date_mmddyy._final_yesemail; set work.price_&date_mmddyy._final_email2;
where email_address_hash  not in (' ',"&hash_null");
run;



	proc sort data=work.price_&date_mmddyy._final_yesemail(keep= Email_Address_hash email_address )
		out=_email2 nodupkey;
		
		by Email_Address&hash ;
	run;

proc sort data=stg.mosaic_cust_subscription (keep= Email_Addr_hash uu_id_hash email_addr uu_id PROD_CD)
		out=mcs_email;
			by EMAIL_ADDR&hash;
			where EMAIL_ADDR&hash ne '' and EMAIL_ADDR&hash ne "&hash_null"
				and PROD_CD   in (
						"prod10002",
						" prod80002",
						" prod830009",
						"prod10004");
		
	run;
	
	data OLFtomcs_email2;
	
			length email_hash $200.;
			length email  $200.;
      
		merge mcs_email(in=a)
			_email2(in=b rename=(Email_Address_hash= Email_Addr_hash
                           Email_Address = Email_Addr  ));
		by  Email_Addr&hash;
		
		if a and b;

			email_hash=(EMAIL_ADDR_hash);
			email =(EMAIL_ADDR );
			*email=lowcase(EMAIL_ADDR_hash);
	run;



%macro skip2;
data _email2 (keep=start fmtname type label);
	set work.price_&date_mmddyy._final_yesemail (rename=(Email_Address_hash=start)); /*(prevmonth)*/;
	length label $25.;
	label="KEEP";
	fmtname='Epost';
	type='C';
run;
proc sort data=_email2 nodupkey;
	by start fmtname;
run;
proc format library=work cntlin=_email2;
run;

data OLFtomcs_email; 
set stg.mosaic_cust_subscription;
Match= put(EMAIL_ADDR_hash,Epost.)='KEEP';
/*if put(PRINT_ACCT_NUM,Post.)='KEEP';*/
run;
data OLFtomcs_email2; set OLFtomcs_email; where match = 1; run;

%mend skip2;


data OLFtomcs_email2; set OLFtomcs_email2; 
where PROD_CD   in (
"prod10002",
" prod80002",
" prod830009",
"prod10004"); run;

data OLFtomcs_email2;
length email $200.;
length email_hash $200.;
 set OLFtomcs_email2; 
email=(EMAIL_ADDR);
email_hash=(EMAIL_ADDR_hash);

run;

data work.price_&date_mmddyy._final_yesemail; 
length email $200.;
length email_hash $200.;
set work.price_&date_mmddyy._final_yesemail; 
email=(EMAIL_ADDRess);
email_hash=(EMAIL_ADDRess_hash);
run;
data work.price_&date_mmddyy._final_acct; 
length email $200.;
length email_hash $200.;
set work.price_&date_mmddyy._final_acct; 
email=(EMAIL_ADDRess);
email_hash=(EMAIL_ADDRess_hash);

/*file 2 email match */
proc sort nodupkey data = work.price_&date_mmddyy._final_yesemail; by email_hash; run;
proc sort nodupkey data = OLFtomcs_email2; by email_hash; run;
data work.price_&date_mmddyy._final_yesemail2; 
merge work.price_&date_mmddyy._final_yesemail (in=in1) OLFtomcs_email2 (in=in2 keep = email email_hash uu_id_hash uu_id);
by email_hash;
if in1 and in2;
run;
/* file 3 email no match */
proc sort nodupkey data = work.price_&date_mmddyy._final_yesemail2; by acct_no_hash; run;
proc sort nodupkey data = work.price_&date_mmddyy._final_yesemail; by acct_no_hash; run;
data work.price_&date_mmddyy._final_noemail2; 
merge work.price_&date_mmddyy._final_yesemail (in=in1) work.price_&date_mmddyy._final_yesemail2 (in=in2 keep = acct_no acct_no_hash);
by acct_no_hash;
if in1 and not in2;
run;
/*ALL OLF FILES*/
data work.price_&date_mmddyy._final_acct; set work.price_&date_mmddyy._final_acct; length filename $15.; filename = "OLF_ACCT";run;
data work.price_&date_mmddyy._final_yesemail2; set work.price_&date_mmddyy._final_yesemail2;length filename $15.;filename = "OLF_EMAIL";run;
data work.price_&date_mmddyy._final_noemail2; set work.price_&date_mmddyy._final_noemail2;length filename $15.;filename = "OLF_NOEMAIL1";run;
data work.price_&date_mmddyy._final_noemail; set work.price_&date_mmddyy._final_noemail;length filename $15.;filename = "OLF_NOEMAIL2";run;

data work.price_&date_mmddyy._final_ALLOLF; 
set  work.price_&date_mmddyy._final_acct work.price_&date_mmddyy._final_yesemail2 work.price_&date_mmddyy._final_noemail2 work.price_&date_mmddyy._final_noemail;
run;
proc freq data= work.price_&date_mmddyy._final_ALLOLF; tables subscription_type; run;



data work.price_&date_mmddyy._final_ALLOLF; set work.price_&date_mmddyy._final_ALLOLF; 

length source $25.;source = "OLF";run;

/**** 04/20/2017 save copy in archived folder in case need to re-create the final dataset in future***/

data keep.price_&date_mmddyy._final_ALLOLF;
set work.price_&date_mmddyy._final_ALLOLF;
run;

data cap; set work.price_&date_mmddyy._final_ALLOLF; 
where subscription_type in ("CAP","AGENCY",'Commission/Cap+'); 
run;

data cap2; 
set cap(keep = Acct_no acct_no_hash Sub_Start_Date  current_term commit_date Expire_date cumm_sub_amt term_wk Frequency_Desc Curr_Source_Key cur_Src_grp2 subscription_type num_copies Renewal_grp Price_range4 Ar_flag rate3 Cust_Status Cust_Status3 
Renewals);
run;
 data cap3; set cap2;
 where subscription_type in ("CAP",'Commission/Cap+') and cust_status ^= "Stopped" and curr_source_key ^ =  "9CLKGJ"
 and (substr(curr_source_key,1,1) ^in ("A","B","1","2","3","4","5","6","7","8","0" )) 

 and Price_range4 in ("$99 and below","$119","$140" "$149","$179"); run;
  data cap3; set cap3; where CUMM_SUB_AMT ^ in (26.99,28.99,32.99,31.20,34.45,35,37.91,37.92,41.81,41.82,99,43.75,44.75,105,131.25);
 run;
/* CAP CONVERSIONS*/
 data cap3a;
set work.price_&date_mmddyy._final_ALLOLF(keep = Acct_no acct_no_hash Sub_Start_Date  current_term commit_date Expire_date cumm_sub_amt term_wk Frequency_Desc Curr_Source_Key cur_Src_grp2 subscription_type num_copies Renewal_grp Price_range4 Ar_flag rate3 Cust_Status Cust_Status3 
Renewals);
where subscription_type = 'Regular' and cust_status ^= 'Stopped' and curr_source_key ^= '9CLKGJ'
and CUMM_SUB_AMT in (44.75,179,1); 
run;


 data cap_wsjplus_remove_&date_mmddyy.; set cap3 cap3a; run;

 data cap_wsjplus_remove_&date_mmddyy.; set cap_wsjplus_remove_&date_mmddyy.;
 Length WSJPLUSsub_Type $22.;
 if subscription_Type = "Regular" then WSJPLUSsub_Type = "CAP CONV"; 
else WSJPLUSsub_Type = subscription_Type;
run;

 proc sort nodupkey data = cap_wsjplus_remove_&date_mmddyy.; by acct_no_hash; run;
 proc sort nodupkey data = work.price_&date_mmddyy._final_ALLOLF; by acct_no_hash; run;
 data work.price_&date_mmddyy._final_ALLOLF;
 merge work.price_&date_mmddyy._final_ALLOLF (in=in1) cap_wsjplus_remove_&date_mmddyy. (keep=acct_no acct_no_hash WSJPLUSsub_Type);
 by acct_no_hash;
 if in1;
 run;


 data work.price_&date_mmddyy._final_ALLOLF; set work.price_&date_mmddyy._final_ALLOLF;
 if WSJPLUSsub_Type = "Corporate" and new_print_stat2 = "Print Only" then WSJPLUSsub_Type = "Corp_Print_Only";
 else WSJPLUSsub_Type =WSJPLUSsub_Type;
 run;
  data work.price_&date_mmddyy._final_ALLOLF; set work.price_&date_mmddyy._final_ALLOLF;
 if WSJPLUSsub_Type = " "  then WSJPLUSsub_Type = subscription_Type;

 run;
 /*
 proc freq data = work.price_&date_mmddyy._final_ALLOLF; tables state; run;
proc freq data = work.price_&date_mmddyy._final_ALLOLF; tables state; run;  */
 

/*PREPARE ACTIVE FILE*/


 data work.price_&date_mmddyy._final_ALLOLF2; set work.price_&date_mmddyy._final_ALLOLF;
 where cust_status3 ^ = "Stopped";
 run;
 

/*PROGRAM CODES*/
data olf_trial&date_mmddyy.; set work.price_&date_mmddyy._final_ALLOLF2; 

run;
data prg_cd4; set slow_chg.prg_cd4; run;
proc sort nodupkey data= prg_cd4; by curr_source_key; run;
 proc sort data= olf_trial&date_mmddyy.; by curr_source_key; run;

 data olf_trial_ck;
 merge olf_trial&date_mmddyy.(in=in1) prg_cd4 (in=in2 keep= curr_source_key marketing_program campaign_name sk_Start_Date);
 by curr_source_key;
 if in1;
 run;
 data olf_trial_ck1; set olf_trial_ck; 
 where marketing_program ^ = " ";
 run;
 data olf_trial_ok; set olf_trial_ck; 
 where marketing_program = " ";
 run;
 proc sort nodupkey data= prg_cd4; by orig_source; run;
 proc sort data= olf_trial_ok; by orig_source; run;

 data olf_trial_ok;
 merge olf_trial_ok(in=in1) prg_cd4 (in=in2 keep= orig_source marketing_program campaign_name sk_Start_Date);
 by orig_source;
 if in1;
 run;
 data work.price_&date_mmddyy._final_ALLOLF2(drop = campaign_name sk_Start_Date);
set olf_trial_ck1 olf_trial_ok; run;


data work.price_&date_mmddyy._final_ALLOLF2_reg; 
set work.price_&date_mmddyy._final_ALLOLF2(keep= acct_no acct_no_hash uu_id uu_id_hash); 
run;





proc sql;

create table work.Price_&date_mmddyy._final_op
as
select m.*, d.register_dt
from
   work.price_&date_mmddyy._final_allolf2_reg m left outer join
   stg.PROV_REG_DATA d
on m.uu_id_hash = d.UU_ID_hash and m.uu_id_hash not in ('',"&hash_null") ;


quit;

data work.Price_&date_mmddyy._final_op;
   set work.Price_&date_mmddyy._final_op;

   length prov_reg $25.;

   if register_dt ^= "" then prov_reg = "Registered"; else prov_reg = "Unregistered";
run;

proc freq data=work.Price_&date_mmddyy._final_op;
tables prov_reg;
run;


/*registration file bump up*/
proc sort nodupkey data= work.price_&date_mmddyy._final_ALLOLF2; by acct_no_hash; run;
 proc sort nodupkey data= work.price_&date_mmddyy._final_op; by acct_no_hash; run;


 data work.price_&date_mmddyy._final_ALLOLF2;
 merge work.price_&date_mmddyy._final_ALLOLF2(in=in1) work.price_&date_mmddyy._final_op (in=in2 drop = uu_id uu_id_hash);
 by acct_no_hash;
 if in1;
 run;

 
 data work.price_&date_mmddyy._final_ALLOLF2;  set work.price_&date_mmddyy._final_ALLOLF2;
 registration = prov_reg;
 run;


/*ALL MOSAIC*/
/*BUNDLEPRINT ONLY SUBS*/

data mcs; 
set stg.mosaic_cust_subscription; 
where PROD_CD in ("prod830009") and BUSINESS_OWNER= "CONSUMER US" and
BILL_SYS_ID in (1,8) and SBSCR_TRMNT_DT is missing;
run;

data mig; set mcs;
where BILL_SYS_ID in (1,8) and IS_MIGRATED = 'Y' /*and EXTL_SYS_ID = 3 and INIT_ORD_SOURCE_CD ^ = "ICS"*/;

dat2=SBSCR_TERM_STRT_DT ;
format dat2 date9.;
dat2_mth=month(dat2);
dat2_yr=year(dat2);
tdat2= SBSCR_TRMNT_DT;
format tdat2 date9.;
tdat2_mth=month(tdat2);
tdat2_yr=year(tdat2);
run;

data mig; set mig;
length AR_flag $8.;
length 	new_print_stat $25.;
length Cust_Status3 $25.;
length Renewal_grp $15.;
length Payment_Type $8.;
length Frequency_Desc $15.;
length cur_Src_grp2 $12.;
length  subscription_Type $22.;

if AUTORENEW_IND in (1,0) then AR_flag = 'AR';
else AR_flag ='Non AR';

if PAY_TYPE in ("Credit") then Payment_Type = "CC";
else payment_type = "CASH";

/* 072613y*/

if term_wks in (5) then Frequency_Desc = "Monthly";
else if term_wks in (8,13,17) then Frequency_Desc = "Quarterly";
else if term_wks in (26,30) then Frequency_Desc = "Semi_Annual";
else if term_wks in (52) then Frequency_Desc = "Annual";
else if term_wks in (104) then Frequency_Desc = "2 Year";
else Frequency_Desc = "OTHER";

term_wk = term_wks;

if DELVR_STAT_NAME in ("Active", " " ) then Cust_Status3= "Live"; 
else if DELVR_STAT_NAME = "Suspended" then Cust_Status3 = "Suspended";
else if DELVR_STAT_NAME = "Stopped" then Cust_Status3 = "Stopped";
else Cust_Status2 = "OTHER";


if GRP_TYPE_NAME = "BUNDLE" then new_print_stat ='BUNDLE';
else if GRP_TYPE_NAME = "SINGLE" then new_print_stat = 'Print Only';
else new_print_stat = 'OTHER';

cur_Src_grp2= CHANNEL; 
num_copies = NUMBER_OF_COPIES;

if CHANNEL in("AFFILIATE","EXTERNAL EMAIL","INTERNAL EMAIL","MEDIA","MISC INTERNET","ONSITE","SEARCH")
then cur_Src_grp2 = "INTERNET MARKETING"; 
else cur_Src_grp2=cur_Src_grp2;


if DELVR_CALENDAR_NAME = "WSJ_SAT" then cur_Src_grp2= "SAT Only";
else cur_Src_grp2= cur_Src_grp2; 

if BUSINESS_OWNER= "CONSUMER US" then subscription_Type = "Regular";
else if BUSINESS_OWNER= "EDUCATION" then subscription_Type = "Educational";
else if BUSINESS_OWNER= "CORPORATE US" then subscription_Type = "Corporate";
else if BUSINESS_OWNER= "CONSUMER US" and FREE_PAID_TYPE = "REWARDS" then subscription_Type = "CAP";
else subscription_Type = "OTHER";


if CAMPAIGN_RENEWAL_CNT=0 and FREE_PAID_TYPE = "FREE" then Renewal_grp='Acqu_Unpaid';/*coupon never come out of ifp*/
if ((CAMPAIGN_RENEWAL_CNT =0) |(CAMPAIGN_RENEWAL_CNT < 0))  and /*FREE_PAID_TYPE = "IFP" and */OUT_OF_IFP_FLAG in(" ","N") then Renewal_grp='Acqu_Unpaid';
if CAMPAIGN_RENEWAL_CNT=0 and /*FREE_PAID_TYPE in ("IFP", "PAID") and*/ OUT_OF_IFP_FLAG = "Y" then Renewal_grp='Acqu_paid';
if CAMPAIGN_RENEWAL_CNT=1 then Renewal_grp='Conversions';
if CAMPAIGN_RENEWAL_CNT=2 then Renewal_grp='1st Renewal';
if CAMPAIGN_RENEWAL_CNT=3 then Renewal_grp='2nd Renewal';
if CAMPAIGN_RENEWAL_CNT>=4 then Renewal_grp='3+ Renewals'; 

if DELVR_CALENDAR_NAME = "WSJ_SAT" then subscription_Type = "SAT Only";
rate3 = EDW_OFFER_PRICE;
length todaydate 8;
todaydatex=input("&date_mmddyy.",mmddyy8.);
todaydate=put(year(todaydatex),z4.)||put(month(todaydatex),z2.)||put(day(todaydatex),z2.);
date3=input(put(todaydate,8.),yymmdd8.);
format date3 date9.;
numdays=intck('day',INIT_ORD_CRE_DT,date3);
numyrs=intck('year',INIT_ORD_CRE_DT,date3);
nummth=(intck('month',INIT_ORD_CRE_DT,date3)/12);
nummthb=(intck('month',INIT_ORD_CRE_DT,date3));
run;

data mig; set mig;
length FileName $15.;
FileName = "MOSAIC MIGRATED";
run;
data mig; set mig;
if CAMPAIGN_RENEWAL_CNT=0 and FREE_PAID_TYPE = "FREE" then Renewal_grp='Acqu_Unpaid';/*coupon never come out of ifp*/
if ((CAMPAIGN_RENEWAL_CNT =0) |(CAMPAIGN_RENEWAL_CNT < 0))  and /*FREE_PAID_TYPE = "IFP" and */OUT_OF_IFP_FLAG in(" ","N") then Renewal_grp='Acqu_Unpaid';
if CAMPAIGN_RENEWAL_CNT=0 and /*FREE_PAID_TYPE in ("IFP", "PAID") and*/ OUT_OF_IFP_FLAG = "Y" then Renewal_grp='Acqu_paid';
if CAMPAIGN_RENEWAL_CNT=1 then Renewal_grp='Conversions';
if CAMPAIGN_RENEWAL_CNT=2 then Renewal_grp='1st Renewal';
if CAMPAIGN_RENEWAL_CNT=3 then Renewal_grp='2nd Renewal';
if CAMPAIGN_RENEWAL_CNT>=4 then Renewal_grp='3+ Renewals'; 

if DELVR_CALENDAR_NAME = "WSJ_SAT" then subscription_type="WKND ONLY";
run;

data mig; set mig;
length curr_Source_key $6.;
curr_Source_key = CURR_CONTRACT_SBSCR_SRCE_KEY;
run;
data mig; set mig;
length email $200.;
length email_hash $200.;
email_hash=(EMAIL_ADDR_hash);
email=(EMAIL_ADDR);

cur_Src_grp3= cur_Src_grp2; 
run;


data new; set mcs;
where BILL_SYS_ID in (1,8) and IS_MIGRATED = 'N' and EXTL_SYS_ID ^ = 3 and INIT_ORD_SOURCE_CD ^ = "ICS";

dat2=SBSCR_TERM_STRT_DT ;
format dat2 date9.;
dat2_mth=month(dat2);
dat2_yr=year(dat2);
tdat2= SBSCR_TRMNT_DT;
format tdat2 date9.;
tdat2_mth=month(tdat2);
tdat2_yr=year(tdat2);
run;


data new; set new;
length AR_flag $8.;
length 	new_print_stat $25.;
length Cust_Status3 $25.;
length Renewal_grp $15.;
length Payment_Type $8.;
length Frequency_Desc $15.;
length cur_Src_grp2 $12.;
length  subscription_Type $22.;

if AUTORENEW_IND in (1,0) then AR_flag = 'AR';
else AR_flag ='Non AR';

if PAY_TYPE in ("Credit") then Payment_Type = "CC";
else payment_type = "CASH";

/* 072613*/

if term_wks in (5) then Frequency_Desc = "Monthly";
else if term_wks in (8,13,17) then Frequency_Desc = "Quarterly";
else if term_wks in (26,30) then Frequency_Desc = "Semi_Annual";
else if term_wks in (52) then Frequency_Desc = "Annual";
else if term_wks in (104) then Frequency_Desc = "2 Year";
else Frequency_Desc = "OTHER";

term_wk = term_wks;

if DELVR_STAT_NAME in ("Active", " " ) then Cust_Status3= "Live"; 
else if DELVR_STAT_NAME = "Suspended" then Cust_Status3 = "Suspended";
else if DELVR_STAT_NAME = "Stopped" then Cust_Status3 = "Stopped";
else Cust_Status3 = "OTHER";


if GRP_TYPE_NAME = "BUNDLE" then new_print_stat ='BUNDLE';
else if GRP_TYPE_NAME = "SINGLE" then new_print_stat = 'Print Only';
else new_print_stat = 'OTHER';

cur_Src_grp2= CHANNEL; 
num_copies = NUMBER_OF_COPIES;

if CHANNEL in("AFFILIATE","EXTERNAL EMAIL","INTERNAL EMAIL","MEDIA","MISC INTERNET","ONSITE","SEARCH")
then cur_Src_grp2 = "INTERNET MARKETING"; 
else cur_Src_grp2=cur_Src_grp2;


if DELVR_CALENDAR_NAME = "WSJ_SAT" then cur_Src_grp2= "SAT Only";
else cur_Src_grp2= cur_Src_grp2; 

if BUSINESS_OWNER= "CONSUMER US" then subscription_Type = "Regular";
else if BUSINESS_OWNER= "EDUCATION" then subscription_Type = "Educational";
else if BUSINESS_OWNER= "CORPORATE US" then subscription_Type = "Corporate";
else if BUSINESS_OWNER= "CONSUMER US" and FREE_PAID_TYPE = "REWARDS" then subscription_Type = "CAP";
else subscription_Type = "OTHER";


if CAMPAIGN_RENEWAL_CNT=0 and FREE_PAID_TYPE = "FREE" then Renewal_grp='Acqu_Unpaid';/*coupon never come out of ifp*/
if ((CAMPAIGN_RENEWAL_CNT =0) |(CAMPAIGN_RENEWAL_CNT < 0))  and /*FREE_PAID_TYPE = "IFP" and */OUT_OF_IFP_FLAG in(" ","N") then Renewal_grp='Acqu_Unpaid';
if CAMPAIGN_RENEWAL_CNT=0 and /*FREE_PAID_TYPE in ("IFP", "PAID") and*/ OUT_OF_IFP_FLAG = "Y" then Renewal_grp='Acqu_paid';
if CAMPAIGN_RENEWAL_CNT=1 then Renewal_grp='Conversions';
if CAMPAIGN_RENEWAL_CNT=2 then Renewal_grp='1st Renewal';
if CAMPAIGN_RENEWAL_CNT=3 then Renewal_grp='2nd Renewal';
if CAMPAIGN_RENEWAL_CNT>=4 then Renewal_grp='3+ Renewals'; 

if DELVR_CALENDAR_NAME = "WSJ_SAT" then subscription_Type = "SAT Only";


rate3 = EDW_OFFER_PRICE;
length todaydate 8;
todaydatex=input("&date_mmddyy.",mmddyy8.);
todaydate=put(year(todaydatex),z4.)||put(month(todaydatex),z2.)||put(day(todaydatex),z2.);
date3=input(put(todaydate,8.),yymmdd8.);
format date3 date9.;
numdays=intck('day',EDW_FIRST_OUT_IFP_DT,date3);
numyrs=intck('year',EDW_FIRST_OUT_IFP_DT,date3);
nummth=(intck('month',EDW_FIRST_OUT_IFP_DT,date3)/12);
nummthb=(intck('month',EDW_FIRST_OUT_IFP_DT,date3));
run;


data new; set new;
if DUR_QTY = 2 and DUR_UNIT = "Month" then do;
        TERM_WKS = 8 ;
		Frequency_Desc ="Bi-Monthly";
end;
run;


data new; set new;
length curr_Source_key $6.;
length FileName $15.;
length email $200.;
length email_hash $200.;
curr_Source_key = SRCE_KEY;
if DELVR_CALENDAR_NAME = "WSJ_SAT" then subscription_type="WKND ONLY";
email=(EMAIL_ADDR);
email_hash=(EMAIL_ADDR_hash);
FileName = "MOSAIC NEW";
cur_Src_grp3= cur_Src_grp2; 
run;


data allmcs; set mig new;
run;

data allmcs; set allmcs;

if CAMPAIGN_RENEWAL_CNT=0 and FREE_PAID_TYPE = "FREE" then Renewal_grp='Acqu_Unpaid';/*coupon never come out of ifp*/
if ((CAMPAIGN_RENEWAL_CNT =0) |(CAMPAIGN_RENEWAL_CNT < 0))  and /*FREE_PAID_TYPE = "IFP" and */OUT_OF_IFP_FLAG in(" ","N") then Renewal_grp='Acqu_Unpaid';
if CAMPAIGN_RENEWAL_CNT=0 and /*FREE_PAID_TYPE in ("IFP", "PAID") and*/ OUT_OF_IFP_FLAG = "Y" then Renewal_grp='Acqu_paid';
if CAMPAIGN_RENEWAL_CNT=1 then Renewal_grp='Conversions';
if CAMPAIGN_RENEWAL_CNT=2 then Renewal_grp='1st Renewal';
if CAMPAIGN_RENEWAL_CNT=3 then Renewal_grp='2nd Renewal';
if CAMPAIGN_RENEWAL_CNT>=4 then Renewal_grp='3+ Renewals'; 

if AUTORENEW_IND in (1,0) then AR_flag = 'AR';
else AR_flag ='Non AR';

if DELVR_STAT_NAME in ("Active", " " ) then Cust_Status3= "Live"; 
else if DELVR_STAT_NAME = "Suspended" then Cust_Status3 = "Suspended";
else if DELVR_STAT_NAME = "Stopped" then Cust_Status3 = "Stopped";
else Cust_Status3 = "OTHER";
run;

data allmcs; set allmcs;
length acct_no $200.;
length acct_no_hash $128.;
acct_no = PRINT_ACCT_NUM;
acct_no_hash=print_acct_num_hash;
new_print_stat2 = new_print_stat;
run;

data allmcs; set allmcs;
 if CHANNEL = " " and  CURR_CONTRACT_SBSCR_SRCE_KEY ^ = " " then CHANNEL = CURR_CONTRACT_CHANNEL;
 else CHANNEL = CHANNEL;
 if CHANNEL in("AFFILIATE","EXTERNAL EMAIL","INTERNAL EMAIL","MEDIA","MISC INTERNET","ONSITE","SEARCH")
then cur_Src_grp3 = "INTERNET MARKETING"; 
else cur_Src_grp3 = CHANNEL;
 run;

 data allmcs2; set allmcs (keep=

UU_ID uu_id_hash LOGIN_NAME FIRST_NAME	LAST_NAME	EMAIL_ADDR	email_addr_hash EMAIL_STAT_CD	SBSCR_ID	SBSCR_TYPE_CODE	SBSCR_STATE_CODE	
PROD_CD	PROD_NAME	AUTORENEW_IND	SBSCR_CRE_DT	SBSCR_TERM_ID	SBSCR_TERM_TYPE_CODE	SBSCR_TERM_STRT_DT	
SBSCR_TERM_END_DT	SBSCR_TRMNT_DT	DUR_UNIT	DUR_QTY	PURCH_AMT	PURCH_CURRENCY	BRAND_NAME	DEAL_NAME	
SBSCR_ORDER_ACTION_DT	ORDER_STATUS_CD	PURCH_DT	RENEWAL_DT	SBSCR_GRP_ID	GRP_TYPE_NAME	GRP_TYPE_CD	
GRP_OFFER_CD	TRACK_CD	CHANNEL	CAMPAIGN_NAME	CAMPAIGN_TYPE	FREE_PAID_TYPE	PHONE_NUM	BUNDLE_NAME	
IS_MIGRATED	EXTL_SYS_ID	ICS_CUST_ID	ICS_FIRST_OUT_IFP_DT	PRINT_ACCT_NUM Print_acct_num_hash	COUPON_CD	INIT_ORD_SOURCE_CD	
ICS_OOT	EDW_PREV_TERM_PURCH_AMT	BUSINESS_OWNER	BUSINESS_OWNER_CL OFFER_TYPE	BILL_SYS_ID	EDW_OFFER_PRICE	PREV_OFFER_TYPE	
EDW_FIRST_OUT_IFP_DT	OUT_OF_IFP_FLAG	CHARGE_STATUS	SOFT_DECLINE_HARD_DECLINE_FLG	DELVR_CALENDAR_NAME	
NUMBER_OF_COPIES	SUBSCR_STATE_NAME	SUBSCR_EVENT_TYPE_NAME	DELVR_STAT_NAME	INIT_ORD_CRE_DT	SBSCR_STRT_DT	
FRE_ORDER_ID	TAX_AMT	SRCHG_AMT	SRCHG_AMT_CHANGE_DT	MARKETING_PROGRAM	PROD_TYPE	PREV_CHANNEL	
PREV_TRACK_CD	PREV_OFFER_PRICE	RENEWAL_CNT	CAMPAIGN_RENEWAL_CNT	SRCE_KEY	TERM_WKS	dat2	dat2_mth	
dat2_yr	tdat2	tdat2_mth	tdat2_yr	AR_flag	new_print_stat	Renewal_grp	Payment_Type	Frequency_Desc	
cur_Src_grp2	subscription_Type	term_wk	num_copies	todaydate	nummth	email email_hash	curr_Source_key	new_print_stat2
FileName	cur_Src_grp3 acct_no acct_no_hash	Cust_Status3 OFFER_TYPE);
run;


proc freq data =allmcs; tables subscription_type; where DELVR_STAT_NAME = "Active"; run;
/*ONLINE ONLY*/

data online; 
set stg.mosaic_cust_subscription; 
where PROD_CD in ("prod10004") and BILL_SYS_ID in (1,8) and SBSCR_TRMNT_DT is missing;
run;

/*NATIVE BRM*/
data online2; set online;
where ((BUSINESS_OWNER =  "CONSUMER US" and FREE_PAID_TYPE ^= "FREE")| (BUSINESS_OWNER_CL = "CORPORATE US" and FREE_PAID_TYPE = "FREE")) 
and OFFER_TYPE in ("DIGITAL PLUS","ONLINE UPGRADE","PRINT UPGRADE","STANDALONE","DIGITAL PACKAGE","ELECTION SPECIAL");
run;
data online2b; set online2; run;
proc sort nodupkey data=online2b; by uu_id_hash; run;

/*data x1;
set online2a;
by UU_ID;
if first.uu_id and last.uu_id then flag='u';
else do;
  flag='d';
  output;
end;
run;*/

data online2b; set online2b;
dat2=SBSCR_TERM_STRT_DT ;
format dat2 date9.;
dat2_mth=month(dat2);
dat2_yr=year(dat2);
tdat2= SBSCR_TRMNT_DT;
format tdat2 date9.;
tdat2_mth=month(tdat2);
tdat2_yr=year(tdat2);
run;

data online2b; set online2b;
length AR_flag $8.;
length 	new_print_stat $25.;
length Cust_Status3 $25.;
length Renewal_grp $15.;
length Payment_Type $8.;
length Frequency_Desc $15.;
length cur_Src_grp2 $12.;
length  subscription_Type $22.;

if AUTORENEW_IND in (1,0) then AR_flag = 'AR';
else AR_flag ='Non AR';

if PAY_TYPE in ("Credit") then Payment_Type = "CC";
else payment_type = "CASH";

if term_wks in (5) then Frequency_Desc = "Monthly";
else if term_wks in (8,13,17) then Frequency_Desc = "Quarterly";
else if term_wks in (26,30) then Frequency_Desc = "Semi_Annual";
else if term_wks in (52) then Frequency_Desc = "Annual";
else if term_wks in (104) then Frequency_Desc = "2 Year";
else Frequency_Desc = "OTHER";

term_wk = term_wks;

if DELVR_STAT_NAME in ("Active", " " ) then Cust_Status3= "Live"; 
else if DELVR_STAT_NAME = "Suspended" then Cust_Status3 = "Suspended";
else if DELVR_STAT_NAME = "Stopped" then Cust_Status3 = "Stopped";
else Cust_Status3 = "OTHER";


if GRP_TYPE_NAME = "BUNDLE" then new_print_stat ='BUNDLE';
else if GRP_TYPE_NAME = "SINGLE" then new_print_stat = 'Print Only';
else new_print_stat = 'OTHER';

cur_Src_grp2= CHANNEL; 
num_copies = NUMBER_OF_COPIES;

if CHANNEL in("AFFILIATE","EXTERNAL EMAIL","INTERNAL EMAIL","MEDIA","ONSITE/MEDIA","MISC INTERNET","ONSITE","SEARCH")
then cur_Src_grp2 = "INTERNET MARKETING"; 
else cur_Src_grp2=cur_Src_grp2;


if DELVR_CALENDAR_NAME = "WSJ_SAT" then cur_Src_grp2= "SAT Only";
else cur_Src_grp2= cur_Src_grp2; 

if BUSINESS_OWNER= "CONSUMER US" then subscription_Type = "Regular";
else if BUSINESS_OWNER= "EDUCATION" then subscription_Type = "Educational";
else if BUSINESS_OWNER= "CONSUMER US" and FREE_PAID_TYPE = "REWARDS" then subscription_Type = "CAP";
else if BUSINESS_OWNER_CL= "CORPORATE US" then subscription_Type = "Corporate";
else subscription_Type = "OTHER";


if CAMPAIGN_RENEWAL_CNT=0 and FREE_PAID_TYPE = "FREE" then Renewal_grp='Acqu_Unpaid';/*coupon never come out of ifp*/
if ((CAMPAIGN_RENEWAL_CNT =0) |(CAMPAIGN_RENEWAL_CNT < 0))  and /*FREE_PAID_TYPE = "IFP" and */OUT_OF_IFP_FLAG in(" ","N") then Renewal_grp='Acqu_Unpaid';
if CAMPAIGN_RENEWAL_CNT=0 and /*FREE_PAID_TYPE in ("IFP", "PAID") and*/ OUT_OF_IFP_FLAG = "Y" then Renewal_grp='Acqu_paid';
if CAMPAIGN_RENEWAL_CNT=1 then Renewal_grp='Conversions';
if CAMPAIGN_RENEWAL_CNT=2 then Renewal_grp='1st Renewal';
if CAMPAIGN_RENEWAL_CNT=3 then Renewal_grp='2nd Renewal';
if CAMPAIGN_RENEWAL_CNT>=4 then Renewal_grp='3+ Renewals'; 

if DELVR_CALENDAR_NAME = "WSJ_SAT" then subscription_Type = "SAT Only";


rate3 = EDW_OFFER_PRICE;
length todaydate 8;
todaydatex=input("&date_mmddyy.",mmddyy8.);
todaydate=put(year(todaydatex),z4.)||put(month(todaydatex),z2.)||put(day(todaydatex),z2.);
date3=input(put(todaydate,8.),yymmdd8.);
format date3 date9.;
numdays=intck('day',INIT_ORD_CRE_DT,date3);
numyrs=intck('year',INIT_ORD_CRE_DT,date3);
nummth=(intck('month',INIT_ORD_CRE_DT,date3)/12);
nummthb=(intck('month',INIT_ORD_CRE_DT,date3));


length curr_Source_key $6.;
length FileName $15.;
length email $200.;
length email_hash $200.;
curr_Source_key = SRCE_KEY;
if DELVR_CALENDAR_NAME = "WSJ_SAT" then subscription_type="WKND ONLY";
email=(EMAIL_ADDR);
email_hash=(EMAIL_ADDR_hash);
FileName = "MOSAIC ONLINE";
cur_Src_grp3= cur_Src_grp2; 
run;

data online2b; set online2b;
if DUR_QTY = 2 and DUR_UNIT = "Month" then do;
        TERM_WKS = 8 ;
		Frequency_Desc ="Bi-Monthly";
end;
run;

data online2a; set online2b;
length OOT_CODE $50.;
ICS_OOT=left(ICS_OOT);
ICS_OOT=lowcase(ICS_OOT);

if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "w" then OOT_CODE = "Onsite";
else if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "a" then OOT_CODE = "Affiliate";
else if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "f" then OOT_CODE = "Formers Email";
else if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "h" then OOT_CODE = "House Email";
else if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "l" then OOT_CODE = "Co-Reg Unpaid";
else if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "m" then OOT_CODE = "External Email";
else if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "n" then OOT_CODE = "Media";
else if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "p" then OOT_CODE = "Services/Barron's Mag";
else if substr(ICS_OOT,1,1) = "6" and substr(ICS_OOT,4,1) = "s" then OOT_CODE = "Search";

else OOT_CODE = ICS_OOT;

OOT_CODE=left(OOT_CODE);
OOT_CODE=upcase(OOT_CODE);


if CHANNEL ^ = " " then OOT_CODE = CHANNEL;
else CHANNEL = CHANNEL;
run;


data online2a; set online2a;
IF IS_MIGRATED = "Y" and CHANNEL = " " 
and OOT_CODE in ("ONSITE","AFFILIATE","FORMERS EMAIL","HOUSE EMAIL","CO-REG UNPAID","EXTERNAL EMAIL","MEDIA",
"SERVICES/BARRON'S MAG","SEARCH")then CHANNEL = OOT_CODE; run;
/*else if IS_MIGRATED = "Y" and CHANNEL = " " 
and OOT_CODE = " " then CHANNEL = " "; 
else CHANNEL = "OTHER";
run;*/


data work.Price_&date_mmddyy._final_online; set online2a; 
length acct_no $200.;
length new_print_stat2 $25.;
acct_no = PRINT_ACCT_NUM;
acct_no_hash=PRINT_ACCT_NUM_Hash;

if OFFER_TYPE = "PRINT UPGRADE" then new_print_stat2 = "DUAL";
else new_print_stat2 = "ONLINE";


if SBSCR_TRMNT_DT > 0 then Cust_Status3= "Stopped"; 
else Cust_Status3 = "Live";
run;
data work.Price_&date_mmddyy._final_online; set work.Price_&date_mmddyy._final_online; where SBSCR_TRMNT_DT is missing; run;


/**** 04/20/2017 save copy in archived folder in case need to re-create the final dataset in future***/

data keep.PRICE_&date_mmddyy._final_online;
set work.PRICE_&date_mmddyy._final_online;
run;


data work.Price_&date_mmddyy._final_online; set work.Price_&date_mmddyy._final_online(keep=

UU_ID uu_id_hash LOGIN_NAME FIRST_NAME	LAST_NAME	EMAIL_ADDR email_addr_hash	EMAIL_STAT_CD	SBSCR_ID	SBSCR_TYPE_CODE	SBSCR_STATE_CODE	
PROD_CD	PROD_NAME	AUTORENEW_IND	SBSCR_CRE_DT	SBSCR_TERM_ID	SBSCR_TERM_TYPE_CODE	SBSCR_TERM_STRT_DT	
SBSCR_TERM_END_DT	SBSCR_TRMNT_DT	DUR_UNIT	DUR_QTY	PURCH_AMT	PURCH_CURRENCY	BRAND_NAME	DEAL_NAME	
SBSCR_ORDER_ACTION_DT	ORDER_STATUS_CD	PURCH_DT	RENEWAL_DT	SBSCR_GRP_ID	GRP_TYPE_NAME	GRP_TYPE_CD	
GRP_OFFER_CD	TRACK_CD	CHANNEL	CAMPAIGN_NAME	CAMPAIGN_TYPE	FREE_PAID_TYPE	PHONE_NUM	BUNDLE_NAME	
IS_MIGRATED	EXTL_SYS_ID	ICS_CUST_ID	ICS_FIRST_OUT_IFP_DT	PRINT_ACCT_NUM PRINT_ACCT_NUM_HASH	COUPON_CD	INIT_ORD_SOURCE_CD	
ICS_OOT	EDW_PREV_TERM_PURCH_AMT	BUSINESS_OWNER	BUSINESS_OWNER_CL OFFER_TYPE	BILL_SYS_ID	EDW_OFFER_PRICE	PREV_OFFER_TYPE	
EDW_FIRST_OUT_IFP_DT	OUT_OF_IFP_FLAG	CHARGE_STATUS	SOFT_DECLINE_HARD_DECLINE_FLG	DELVR_CALENDAR_NAME	
NUMBER_OF_COPIES	SUBSCR_STATE_NAME	SUBSCR_EVENT_TYPE_NAME	DELVR_STAT_NAME	INIT_ORD_CRE_DT	SBSCR_STRT_DT	
FRE_ORDER_ID	TAX_AMT	SRCHG_AMT	SRCHG_AMT_CHANGE_DT	MARKETING_PROGRAM	PROD_TYPE	PREV_CHANNEL	
PREV_TRACK_CD	PREV_OFFER_PRICE	RENEWAL_CNT	CAMPAIGN_RENEWAL_CNT	SRCE_KEY	TERM_WKS	dat2	dat2_mth	
dat2_yr	tdat2	tdat2_mth	tdat2_yr	AR_flag	new_print_stat	Renewal_grp	Payment_Type	Frequency_Desc	
cur_Src_grp2	subscription_Type	term_wk	num_copies	todaydate	nummth	email  email_hash curr_Source_key	new_print_stat2
FileName	cur_Src_grp3	OOT_CODE	acct_no acct_no_hash Cust_Status3 OFFER_TYPE);
run;


data work.price_&date_mmddyy._final_mcs; set allmcs2; 
if DUR_QTY = 2 and DUR_UNIT = "Month" then do;
        TERM_WKS = 8 ;
		Frequency_Desc ="Bi-Monthly";
end;
run;

data work.price_&date_mmddyy._final_mcs2; set work.price_&date_mmddyy._final_mcs;
Length FILENAME2 $25.;
Length SOURCE $25.;
FILENAME2 = "Print/Bundle";
SOURCE = "MOSAIC";

SUB_START_DATE = INIT_ORD_CRE_DT;
format SUB_START_DATE date9.; 
EXPIRE_DATE = SBSCR_TERM_END_DT;
format EXPIRE_DATE date9.; 
RENEWAL_DATE= SBSCR_TERM_STRT_DT;
format RENEWAL_DATE date9.; 
STOP_DATE= SBSCR_TRMNT_DT;
format STOP_DATE date9.; 

run;

/**** 04/20/2017 save copy in archived folder in case need to re-create the final dataset in future***/

data keep.price_&date_mmddyy._final_mcs2;
set work.price_&date_mmddyy._final_mcs2;
run;


data work.price_&date_mmddyy._final_mcs2; 
set work.price_&date_mmddyy._final_mcs2( keep = ACCT_NO acct_no_hash
UU_ID uu_id_hash SBSCR_ID EMAIL email_hash 
PHONE_NUM CUST_STATUS3 SUBSCRIPTION_TYPE SUB_START_DATE EDW_OFFER_PRICE FREQUENCY_DESC PAYMENT_TYPE MARKETING_PROGRAM EXPIRE_DATE RENEWAL_DATE NUMMTH new_print_stat2 cur_Src_grp3 AR_flag SBSCR_STATE_CODE FILENAME2 SOURCE STOP_DATE OFFER_TYPE);
run; 


proc sort nodupkey data = work.Price_&date_mmddyy._final; by acct_no_hash; run;
proc sort data = work.price_&date_mmddyy._final_mcs2; by acct_no_hash; run;

data work.price_&date_mmddyy._final_mcs2; 
merge work.price_&date_mmddyy._final_mcs2 (in=in1)  work.Price_&date_mmddyy._final (in =in2 keep= Acct_No acct_no_hash
Company Prefix Firstname Lastname Suffix Street_number Street_name	Extra_address City State Zipcode Zipplus4 Zipplus3 Carrier);
by acct_no_hash;
if in1;
run;


data work.Price_&date_mmddyy._final_online; set work.Price_&date_mmddyy._final_online (drop = new_print_stat2); run;

data work.Price_&date_mmddyy._final_online2; set work.Price_&date_mmddyy._final_online;
Length FILENAME2 $25.;
FILENAME2 = "Digital";
Length SOURCE $25.;
SOURCE = "MOSAIC";

SUB_START_DATE = INIT_ORD_CRE_DT;
format SUB_START_DATE date9.; 
EXPIRE_DATE = SBSCR_TERM_END_DT;
format EXPIRE_DATE date9.; 
RENEWAL_DATE= SBSCR_TERM_STRT_DT;
format RENEWAL_DATE date9.; 
STOP_DATE= SBSCR_TRMNT_DT;
format STOP_DATE date9.; 

length new_print_stat2 $25.;
acct_no=print_acct_num;
acct_no_hash = PRINT_ACCT_NUM_hash;
if OFFER_TYPE = "PRINT UPGRADE" then new_print_stat2 = "DUAL";
else new_print_stat2 = "ONLINE";

run;

/*
data work.Price_&date_mmddyy._final_online2; 
set work.Price_&date_mmddyy._final_online2 (keep =ACCT_NO ACCT_NO_hash UU_ID uu_id_hash EMAIL email_hash); run;
*/
data work.Price_&date_mmddyy._final_online2; 
length Firstname $150.;
length Lastname	$150.;

set work.Price_&date_mmddyy._final_online2( keep = ACCT_NO ACCT_NO_hash UU_ID uu_id_hash EMAIL email_hash
 FIRST_NAME LAST_NAME  SBSCR_ID  PHONE_NUM CUST_STATUS3 SUBSCRIPTION_TYPE SUB_START_DATE EDW_OFFER_PRICE FREQUENCY_DESC PAYMENT_TYPE MARKETING_PROGRAM EXPIRE_DATE RENEWAL_DATE NUMMTH new_print_stat2 cur_Src_grp3 AR_flag SBSCR_STATE_CODE FILENAME2 STOP_DATE SOURCE OFFER_TYPE);
Firstname=FIRST_NAME;
Lastname = LAST_NAME;
Firstname=(Firstname);
Lastname=(Lastname);

run; 



proc sql;

create table Price_&date_mmddyy._final_online2_1
as
select m.*, mcs.pymt_instr_id
from
    work.Price_&date_mmddyy._final_online2 m left outer join
	stg.mosaic_cust_subscription mcs
on m.sbscr_id = mcs.sbscr_id;

quit;

proc sql;

create table Price_&date_mmddyy._final_online2_op
as
select m.*, mb.PYMT_ADDR, PYMT_ADDR_LINE1, PYMT_ADDR_LINE2, CITY, STATE, USPS_ZIP5, USPS_ZIP4, COUNTRY
from
    Price_&date_mmddyy._final_online2_1 m left outer join
	stg.mosaic_cust_billing_address mb
on m.pymt_instr_id = mb.pymt_instr_id;

quit;

proc sort data=Price_&date_mmddyy._final_online2_op nodupkey;
  by sbscr_id;
run;

data work.Price_&date_mmddyy._final_online2_op;
   set Price_&date_mmddyy._final_online2_op;

   rename PYMT_ADDR = street_num;
   rename PYMT_ADDR_LINE1 = addr1;
   rename PYMT_ADDR_LINE2 = addr2;
   rename CITY = city2;
   rename STATE = state2;
   rename USPS_ZIP5 = zip_5;

 run;

data work.Price_&date_mmddyy._final_online2_opb; 

length Street_number $100.;
length Street_name	$100.;
length Extra_address $100.;
length City $100.;
length State $100.;
length Zipcode $100.;

set work.Price_&date_mmddyy._final_online2_op;
Street_number=street_num;
Street_name = addr1;
Extra_address = addr2;
City = City2;
State = State2;
Zipcode = zip_5;
	run;
data work.Price_&date_mmddyy._final_online2_opb; set work.Price_&date_mmddyy._final_online2_opb;
/*Firstname=upcase(Firstname);
Lastname=upcase(Lastname);*/
Street_number=(Street_number);
Extra_address=(Extra_address);
City=(City);
State=(State);
run;

proc sort nodupkey data = work.Price_&date_mmddyy._final_online2_opb; by SBSCR_ID; run;
proc sort data = work.Price_&date_mmddyy._final_online2; by SBSCR_ID; run;

data work.Price_&date_mmddyy._final_online2b; 
merge work.Price_&date_mmddyy._final_online2 (in=in1)  work.Price_&date_mmddyy._final_online2_opb (in =in2 keep= SBSCR_ID Street_number Street_name Extra_address City State Zipcode);
by SBSCR_ID;
if in1;
run;


data work.price_&date_mmddyy._final_ALLMCS; set work.Price_&date_mmddyy._final_online2b work.Price_&date_mmddyy._final_mcs2; run;

/*pull out outlier records*/
proc freq data =  work.price_&date_mmddyy._final_ALLMCS; tables SBSCR_STATE_CODE; run;
data work.price_&date_mmddyy._final_ALLMCS; set work.price_&date_mmddyy._final_ALLMCS; where 
SBSCR_STATE_CODE  = "ACTIVE" and SBSCR_ID > 0; run;

data work.price_&date_mmddyy._final_ALLMCS; set work.price_&date_mmddyy._final_ALLMCS; 
Length registration $25.;
Length WSJPLUSsub_Type $22.;
WSJPLUSsub_Type = subscription_Type;
Registration = "Registered";
run;



data work.price_&date_mmddyy._final_ALLMCS2; 
set work.price_&date_mmddyy._final_ALLMCS(keep = ACCT_NO ACCT_NO_hash UU_ID uu_id_hash EMAIL email_hash
PHONE_NUM CUST_STATUS3 SUBSCRIPTION_TYPE SUB_START_DATE stop_date EDW_OFFER_PRICE FREQUENCY_DESC PAYMENT_TYPE 
MARKETING_PROGRAM EXPIRE_DATE RENEWAL_DATE STOP_DATE NUMMTH new_print_stat2 cur_Src_grp3 AR_flag SBSCR_STATE_CODE registration FILENAME2 SOURCE WSJPLUSsub_Type OFFER_TYPE Firstname Lastname Street_number Street_name Extra_address City State Zipcode Zipplus4 SBSCR_ID) ; 
where WSJPLUSsub_Type ="Regular";
run;

data work.price_&date_mmddyy._final_ALLMCS2; set work.price_&date_mmddyy._final_ALLMCS2;
 length _SBSCR_ID $200.;
 _SBSCR_ID = SBSCR_ID; 
Cumm_sub_amt = EDW_OFFER_PRICE;
run;

 data work.price_&date_mmddyy._final_ALLOLF2a( keep = ACCT_NO ACCT_NO_hash UU_ID uu_id_hash EMAIL email_hash
PHONE_NUM CUST_STATUS3 SUBSCRIPTION_TYPE SUB_START_DATE stop_date CUMM_SUB_AMT FREQUENCY_DESC PAYMENT_TYPE 
MARKETING_PROGRAM EXPIRE_DATE RENEWAL_DATE NUMMTH new_print_stat2 cur_Src_grp3 AR_flag registration rate3 FILENAME2 SOURCE WSJPLUSsub_Type Firstname Lastname Street_number Street_name Extra_address City State Zipcode);  
set work.price_&date_mmddyy._final_ALLOLF2;
LENGTH FILENAME2 $25.;
LENGTH PHONE_NUM $200.;
LENGTH cur_Src_grp3 $12.;
PHONE_NUM = HOME_PHONE;
FILENAME2 = FILENAME;
cur_Src_grp3 = cur_Src_grp2;

*SUB_START_DATE=input(put(SUB_START_DATE,8.),yymmdd8.);
format SUB_START_DATE date9.; 

EXPIRE_DATE=input(put(EXPIRE_DATE,8.),yymmdd8.);
format EXPIRE_DATE date9.; 

RENEWAL_DATE=input(put(RENEWAL_DATE,8.),yymmdd8.);
format RENEWAL_DATE date9.; 

STOP_DATE=input(put(STOP_DATE,8.),yymmdd8.);
format STOP_DATE date9.; 
run;


 data work.price_&date_mmddyy._final_ALLOLF2a; set work.price_&date_mmddyy._final_ALLOLF2a (rename=(rate3=price));
 length SBSCR_STATE_CODE  $40.;
 length _SBSCR_ID $200.;
 _SBSCR_ID = acct_no_hash; 
 SBSCR_STATE_CODE ="ACTIVE";
 run;

data work.price_&date_mmddyy._final_ALLMCS2; set work.price_&date_mmddyy._final_ALLMCS2;
  length _SBSCR_ID $200.;
 _SBSCR_ID = SBSCR_ID; 
 price = EDW_OFFER_PRICE;
run;

data work.price_&date_mmddyy._final_ALLOLF2a; set work.price_&date_mmddyy._final_ALLOLF2a;
  length OFFER_TYPE $100.;
  if new_print_stat2 = "Print Only" and registration = "Unregistered" then do;
  		OFFER_TYPE =" ";
end;
else do;
OFFER_TYPE ="WSJ Ultimate Package";

end;


 data  keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.;
set work.price_&date_mmddyy._final_ALLOLF2a work.price_&date_mmddyy._final_ALLMCS2; run;



 data  keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.;  set keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy. (rename=(nummth=_ORI_TENURE_ys));
length todaydate 8;
todaydatex=input("&date_mmddyy.",mmddyy8.);
todaydate=put(year(todaydatex),z4.)||put(month(todaydatex),z2.)||put(day(todaydatex),z2.);
date3=input(put(todaydate,8.),yymmdd8.);
format date3 date9.;
_CUR_TENURE_yrs=(intck('month',SUB_START_DATE,date3)/12);
_CUR_TENURE_mths=(intck('month',SUB_START_DATE,date3));
run;



data keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.; set keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.;
where cust_status3 ^ = "Stopped";
run;

 data keep.WSJIP_SINGLvw_ACTIVE_&date_mmddyy._reg; set keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.;
where subscription_type = "Regular";
run;

data keep.WSJ_ACTIVE_&date_mmddyy._Custview_BLUEK; set keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.  
(drop= 
date3
EDW_OFFER_PRICE
_ORI_TENURE_ys
WSJPLUSsub_Type
_CUR_TENURE_yrs
_CUR_TENURE_mths
SBSCR_ID
source);
run;
data keep.WSJ_ACTIVE_&date_mmddyy._Custview_SFMC; set keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy. (drop= 
date3
EDW_OFFER_PRICE
_ORI_TENURE_ys
WSJPLUSsub_Type
_CUR_TENURE_yrs
_CUR_TENURE_mths
SBSCR_ID
source);
run;
 data work.WSJ_ACTIVE_&date_mmddyy._Custview_SFMC; set keep.WSJ_ACTIVE_&date_mmddyy._Custview_SFMC (
	rename=(_SBSCR_ID=DJID)); 
	run;

data keep.Wsjip_active_&date_mmddyy._custview_reg; set keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.; 
where subscription_type = "Regular";
run;
data keep.WSJ_ACTIVE_&date_mmddyy._Custview_BKAI; set keep.Wsjip_active_&date_mmddyy._custview_reg 
(drop= 
date3
EDW_OFFER_PRICE
_ORI_TENURE_ys
WSJPLUSsub_Type
_CUR_TENURE_yrs
_CUR_TENURE_mths
SBSCR_ID
source);
run;

/* RENAME CUR_SRC_GRP3 to Channel, bring offer type, prod_code later add WSJ+ flag, EMEA, APAC, INAPP*/

data keep.Wsj_active_&date_mmddyy._custview_ind; set keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.
(drop= 

Firstname
Lastname
Street_number
Street_name
Extra_address
City
State
email
PHONE_NUM
SBSCR_ID
EDW_OFFER_PRICE
Zipplus4
date3

);
run;

/**** keep only WSJIP_SINGLEVIEW_ACTIVE and delete intrim datasets*****/

proc datasets lib=keep;
      save price_&date_mmddyy._final_ALLOLF
      PRICE_&date_mmddyy._final_online
      price_&date_mmddyy._final_mcs2 
      WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy./ memtype=data;
 run;

/****  This section is now part of 177 program ******
proc sql;
create table indicative_file2 as
select Acct_No	,
Zipcode	,
Sub_Start_Date	,
Renewal_Date	,
Expire_Date	,
Stop_Date	,
cumm_sub_amt	,
subscription_Type	,
AR_flag	,
_ORI_TENURE_ys	,
price	,
Payment_Type	,
Frequency_Desc	,
new_print_stat2	,
Cust_Status3	,
UU_ID	,
source	,
WSJPLUSsub_Type	,
marketing_program	,
registration	,
FILENAME2	,
cur_Src_grp3	,
SBSCR_STATE_CODE	,
_SBSCR_ID	,
todaydate	,
date3	,
 _CUR_TENURE_yrs	,
 _CUR_TENURE_mths	

from work.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.;

quit;

proc export data=indicative_file2 
            outfile="&root./&user./&env./&project./raw-data/indicative_active_file_&&today_yyyymmdd..csv"
			dbms=csv replace;
run;



%macro report_missing_file;
  
  

		
	filename mymail email 
      to = &to_email 
      from = "SASCDM &env_email. Alerts <noreply@dowjones.com>"
			cc = &cc_email
			subject="SASCDM &env_email. Alerts: SUCCESS - INDICATIVE Actives file for asofdate &today_MMDDYY10. has been created"
			replyto=&reply_to_email;
			
		data _null_;
			file mymail;

			dt=  put(&run_date, MMDDYY10.) || ' ' || put ((&start_time), timeampm11.);

			format message message1 message2 message3  $100.;
			message = "INDICATIVE Active file for asofdate &today_MMDDYY10. has been created";
      message1 = "&root./&user./&env./&project./raw-data/indicative_active_file_&&today_yyyymmdd..csv has been created." ;
			message2 = " ";
      		Message3 = "Please do not reply to this email.  This mailbox is not monitored and you will not receive a response.";

				put message;
            put message1;
		        put message2;
      			put message3;
		run; 	
 
 
%mend report_missing_file;
%report_missing_file;

*/

%let outfile=&root./&user./&env./&project./report/freq_chart_&date_mmddyy..csv;
%let source_data=keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy.;


proc datasets library=work nolist nodetails;
delete member freq_chart;
run;
options mprint;


%macro run_freq(var_name);

proc freq data=&source_data. noprint;
tables &var_name. /missing out=tb_&var_name.;
run;

proc sql;
create table tb_&var_name.2 as

select "&var_name" as variable_name length=100,
	   &var_name. as variable_value length=300,
       count,
	   percent
from tb_&var_name.;
quit;


proc append base=freq_chart data=tb_&var_name.2 force ;
run;
 

%mend run_freq;

%run_Freq(subscription_type);
%run_Freq(AR_flag);
%run_Freq(Payment_Type);
%run_Freq(Frequency_Desc);
%run_Freq(new_print_stat2);
%run_Freq(WSJPLUSsub_Type);
%run_Freq(marketing_program);
%run_Freq(registration);
%run_Freq(FILENAME2);
%run_Freq(cur_Src_grp3);
%run_Freq(offer_type);



proc export data=freq_chart outfile="&outfile." dbms=csv replace;
run;


proc freq data=&source_data ;
tables subscription_type*registration / list missing out=sub_reg_cross;
run;

proc export data=sub_reg_cross outfile="&outfile._sub_reg.csv" dbms=csv replace;
run;




****************************************************************************************************;