* this program will create the marketing_db tables, these tables are re-assemble from wsjplus and cust_view;

*%include "&root./&user./&env./&project./sas-code/20_process_configuration.sas";
*%include "&root./&user./&env./&project./sas-code/50_convert_table_2_format.sas";

data WP_OP.wsjplus_data_&date_mmddyy;
	set WP_OP.wsjplus_data_&date_mmddyy;
	
	*SBSCR_ID takes acct_num_hash value which can be big and presented in scientific notation,
	 will blank it out so it can be loaded to cdb, this only happans in dev. 
	 In prod, the max of SBSCR_ID is 12 digits;
	;
	if "&hash" ne '' and sbscr_id > 999999999999999 then sbscr_id = .;
run;
 
proc sort data=WP_OP.wsjplus_data_&date_mmddyy out=wsj;
	by uu_id&hash acct_num&hash.;
run;

proc sort data=keep.WSJIP_SINGLEVIEW_ACTIVE_&date_mmddyy. out=cust_vw;
	by uu_id&hash acct_no&hash.;
run;

data wsjplus_cust_view;
	merge wsj (in=a
			rename=(
				sbscr_term_strt_dt_var = sub_start_date
				sbscr_term_end_dt_var = expire_date
				sbscr_trmnt_dt_var = stop_date
				)
			)
		cust_vw (in=b
			rename=(acct_no&hash.=acct_num&hash.)
			keep=
				uu_id&hash
				uu_id
				acct_no&hash.
				acct_no
				renewal_date
                cumm_sub_amt
				ZIPCODE
				_ORI_TENURE_ys
				price
				Payment_Type
				Frequency_Desc
				new_print_stat2
				Cust_Status3
				source
				WSJPLUSsub_Type
				marketing_program
				registration
				FILENAME2
				PHONE_NUM
				cur_Src_grp3
				SBSCR_STATE_CODE
				_SBSCR_ID
				OFFER_TYPE
				edw_offer_price
				ZIPPLUS4
				todaydate
				todaydatex
				date3
				_CUR_TENURE_yrs
				_CUR_TENURE_mths

		);
	by uu_id&hash acct_num&hash.;
	
	if a;
run;

data  wsjplus.subscription_&date_mmddyy  
  	(keep=
		uu_id_hash
		uu_id
		email_addr_hash
		email_addr
		acct_num_hash
		acct_no
		prod_cd
		mosaic_prod_cd
		first_name
		last_name
		sbscr_status
		is_migrated
		ar_flag
		payment_type
		offer_type
		business_owner
		subscription_type
		channel
		channel_2
		frequency_desc
		company_name
		coupon_cd
		source
		print_stat
		free_paid_type
		sbscr_id
		offer_price
		bill_sys_id
		tenure
		wsjplus_eligible
		pymt_instr_id
		pymt_street_num
		pymt_addr
		pymt_addr2
		pymt_city
		pymt_state
		pymt_zip_5
		pymt_zip_plus_4
		pymt_country
		pymt_rpt_region
		cbs_code
		cbs_name
		fips_county_code
		fips_county_name
		identity_uuid_hash

		uu_id_cnvrt
		client_org_id
		vxid
		activated_30_dy_ind
		engaged_30_dy_ind
		load_dt
		init_ord_cre_dt_var
		sub_start_date
		expire_date
		stop_date
		wsjplus_activate_dt_var
	/****  cust_view ****/	
        renewal_date
        cumm_sub_amt
		ZIPCODE
		_ORI_TENURE_ys
		price
		Payment_Type
		Frequency_Desc
		new_print_stat2
		Cust_Status3
		source
		WSJPLUSsub_Type
		marketing_program
		registration
		FILENAME2
		PHONE_NUM
		cur_Src_grp3
		SBSCR_STATE_CODE
		_SBSCR_ID
		OFFER_TYPE
		edw_offer_price
		ZIPPLUS4
		todaydate
		todaydatex
		date3
		_CUR_TENURE_yrs
		_CUR_TENURE_mths

	)
	
	wsjplus.address_&date_mmddyy 
         (keep= uu_id&hash
                uu_id  
      			acct_num&hash
				acct_num
				street_num
				addr
				addr2
				city
				state 

				zip_5
				zip_plus_4
				postal_cd
				country
				rpt_region
				dma
			   rename=(rpt_region=region)		
			   where = (street_num ne '' or addr ne '' or zip_5 ne '' or city ne '')
			)
			
	wsjplus.entitlement_&date_mmddyy 
	  		(keep=
			uu_id&hash
			uu_id
			acct_num&hash
			acct_num
			ent_name
			wsjplus_ent_exist
			wsjplus_add_ent_dt_var
			clock_opt_in_date_var
			clock_opt_in
			opt_in_type	
		where = (opt_in_type ne '' or clock_opt_in ne '' or wsjplus_ent_exist ne .)
		)
		
	wsjplus.registration_&date_mmddyy  
    		(keep=uu_id&hash
		      uu_id 
			   acct_num&hash
			   acct_num
			    registration_date_var
		where = (registration_date_var ne '')
		)
		
	wsjplus.omniture_&date_mmddyy  
        
		(keep=
			uu_id&hash
			uu_id
			acct_num&hash
			acct_num
			tot_num_visits
			tot_unique_visitors
			tot_pageviews
			first_access_dt_var
			latest_access_dt_var
		 where=(tot_num_visits ne . or tot_pageviews ne .)
		)
	;
	set wsjplus_cust_view;
run;







/*
data wsjplus_cust_view;
	merge wsj (in=a
			rename=(
				sbscr_term_strt_dt_var = sub_start_date
				sbscr_term_end_dt_var = expire_date
				sbscr_trmnt_dt_var = stop_date
				)
			)
		cust_vw (in=b
			rename=(acct_no&hash.=acct_num&hash.)
			keep=
				uu_id&hash
				acct_no&hash.
				billplan
				carrier_flag 
				commit_date
				cur_src_grp2
				curr_source_key
				current_term
				cust_status
				cust_status3
				edw_offer_price
				exp_date
				icu_ind
				install_code
				last_sub_type
				no_sell_type
				num_copies
				online_acct_no
				ori_src_grp2
				orig_source
				price_range4
				print_ctr
				rate_commmit
				price
				renewal_grp
				renewals
				req_djps_via_email
				req_ques_via_email
				stop_reason
				sub_type
				term_commmit
				term_week
				term_wk
				user_id
		);
	by uu_id&hash acct_num&hash.;
	
	if a;
run;

data  wsjplus.subscription_&date_mmddyy
	(keep=
		uu_id_hash
		email_addr_hash
		acct_num_hash
		prod_cd
		mosaic_prod_cd
		first_name
		last_name
		sbscr_status
		is_migrated
		ar_flag
		payment_type
		offer_type
		business_owner
		subscription_type
		channel
		channel_2
		frequency_desc
		company_name
		coupon_cd
		source
		print_stat
		free_paid_type
		sbscr_id
		offer_price
		bill_sys_id
		tenure
		wsjplus_eligible
		pymt_instr_id
		pymt_street_num
		pymt_addr
		pymt_addr2
		pymt_city
		pymt_state
		pymt_zip_5
		pymt_zip_plus_4
		pymt_country
		pymt_rpt_region
		cbs_code
		cbs_name
		fips_county_code
		fips_county_name
		identity_uuid_hash

		uu_id_cnvrt
		client_org_id
		vxid
		activated_30_dy_ind
		engaged_30_dy_ind
		load_dt
		init_ord_cre_dt_var
		sub_start_date
		expire_date
		stop_date

		wsjplus_activate_dt_var
		
		billplan
		Carrier_Flag
		commit_date
		cur_Src_grp2
		curr_source_key
		current_term
		cust_status
		Cust_Status3
		edw_offer_price
		exp_date
		icu_ind
		install_code
		last_sub_type
		no_sell_type
		num_copies
		online_acct_no
		ori_src_grp2
		orig_source
		price_range4
		print_ctr
		rate_commmit
		price
		renewal_grp
		renewals
		req_djps_via_email
		req_ques_via_email
		stop_reason
		sub_type
		term_commmit
		term_week
		term_wk
		user_id
	)
	
	wsjplus.address_&date_mmddyy (keep= uu_id&hash
				acct_num&hash
				street_num
				addr
				addr2
				city
				state 

				zip_5
				zip_plus_4
				postal_cd
				country
				rpt_region
				dma
			   rename=(rpt_region=region)		
			   where = (street_num ne '' or addr ne '' or zip_5 ne '' or city ne '')
			)
			
	wsjplus.entitlement_&date_mmddyy 
		(keep=
			uu_id&hash
			acct_num&hash
			ent_name
			wsjplus_ent_exist
			wsjplus_add_ent_dt_var
			clock_opt_in_date_var
			clock_opt_in
			opt_in_type	
		where = (opt_in_type ne '' or clock_opt_in ne '' or wsjplus_ent_exist ne .)
		)
		
	wsjplus.registration_&date_mmddyy
		(keep=uu_id&hash
			acct_num&hash
			registration_date_var
		where = (registration_date_var ne '')
		)
		
	wsjplus.omniture_&date_mmddyy
		(keep=
			uu_id&hash
			acct_num&hash
			tot_num_visits
			tot_unique_visitors
			tot_pageviews
			first_access_dt_var
			latest_access_dt_var
		 where=(tot_num_visits ne . or tot_pageviews ne .)
		)
	;
	set wsjplus_cust_view;
run;
*/

****************************************************************************;