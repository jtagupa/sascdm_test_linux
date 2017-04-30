%macro create_tmp_tables(connection,redshift_schema,temp_string,uat_flag);


proc sql;

			connect using &connection;
			   


execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.prov_identity_mqt";
) by &connection;

execute (
				
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.prov_identity_mqt"
(
	"identity_uuid" VARCHAR(150)   ENCODE lzo
	,"legacy_uuid" VARCHAR(150)   ENCODE lzo
	,"username" VARCHAR(150)   ENCODE lzo
	,"first_name" VARCHAR(200)   ENCODE lzo
	,"last_name" VARCHAR(200)   ENCODE lzo
	,"email" VARCHAR(200)   ENCODE lzo
	,"realm" VARCHAR(32)   ENCODE bytedict
	,"acct_validated_ind" VARCHAR(10)   ENCODE lzo
	,"acct_validated_dt_tm" TIMESTAMP WITHOUT TIME ZONE   
	,"modified_dt_tm" TIMESTAMP WITHOUT TIME ZONE   
	,"creation_dt_tm" TIMESTAMP WITHOUT TIME ZONE   
	,"initial_ent" VARCHAR(400)   ENCODE lzo
	,"old_username" VARCHAR(300)   ENCODE lzo
	,"remove_reason_cd" VARCHAR(255)   ENCODE lzo
	,"uu_id" VARCHAR(150)   ENCODE lzo
	,"identity_uuid&hash" VARCHAR(150)   ENCODE lzo
	,"legacy_uuid&hash" VARCHAR(150)   ENCODE lzo
	,"first_name&hash" VARCHAR(128)   ENCODE lzo
	,"last_name&hash" VARCHAR(128)   ENCODE lzo
	,"email&hash" VARCHAR(128)   ENCODE lzo
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("identity_uuid&hash")
SORTKEY (	"uu_id&hash"	)
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.customer_login_vw";
	) by &connection;

execute (

CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.customer_login_vw"
(
	"uu_id" VARCHAR(150)   ENCODE lzo
	,"uu_id_cnvrt" VARCHAR(150)   ENCODE lzo
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("uu_id&hash")
SORTKEY ("uu_id&hash")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.mosaic_cust_billing_address";
	) by &connection;

execute (

CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.mosaic_cust_billing_address"
(
	"uu_id" VARCHAR(150)   ENCODE lzo
	,"acct_id" VARCHAR(100)   ENCODE lzo
	,"pymt_addr" VARCHAR(300)   ENCODE lzo
	,"pymt_addr_line1" VARCHAR(300)   ENCODE lzo
	,"pymt_addr_line2" VARCHAR(300)   ENCODE lzo
	,"pymt_addr_line3" VARCHAR(300)   ENCODE lzo
	,"pymt_addr_line4" VARCHAR(300)   ENCODE lzo
	,"pymt_addr_line5" VARCHAR(300)   ENCODE lzo
	,"city" VARCHAR(200)   ENCODE lzo
	,"state" VARCHAR(200)   ENCODE lzo
	,"usps_zip5" VARCHAR(20)   ENCODE lzo
	,"usps_zip4" VARCHAR(16)   ENCODE lzo
	,"province" VARCHAR(200)   ENCODE lzo
	,"postal_cd" VARCHAR(100)   ENCODE lzo
	,"country" VARCHAR(6)   ENCODE lzo
	,"company_name" VARCHAR(100)   ENCODE lzo
	,"name" VARCHAR(300)   ENCODE lzo
	,"first_name" VARCHAR(200)   ENCODE lzo
	,"last_name" VARCHAR(200)   ENCODE lzo
	,"registration_dt" DATE   ENCODE delta32k
	,"identity_cre_dt" DATE   ENCODE delta32k
	,"pymt_instr_id" VARCHAR(100)   ENCODE lzo
	,"prefix_name" VARCHAR(60)   ENCODE lzo
	,"suffix_name" VARCHAR(60)   ENCODE lzo
	,"payment_mode" VARCHAR(50)   ENCODE lzo
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
	,"pymt_addr&hash" VARCHAR(128)   ENCODE lzo
	,"pymt_addr_line1&hash" VARCHAR(128)   ENCODE lzo
	,"pymt_addr_line2&hash" VARCHAR(128)   ENCODE lzo
	,"pymt_addr_line3&hash" VARCHAR(128)   ENCODE lzo
	,"pymt_addr_line4&hash" VARCHAR(128)   ENCODE lzo
	,"pymt_addr_line5&hash" VARCHAR(128)   ENCODE lzo
	,"city&hash" VARCHAR(128)   ENCODE lzo
	,"state&hash" VARCHAR(128)   ENCODE lzo
	,"name&hash" VARCHAR(128)   ENCODE lzo
	,"first_name&hash" VARCHAR(128)   ENCODE lzo
	,"last_name&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("uu_id&hash")
SORTKEY ("uu_id&hash")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.prov_reg_data";
	) by &connection;
execute (

CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.prov_reg_data"
(
	"uu_id" VARCHAR(150)   ENCODE lzo
	,"realm" VARCHAR(50)   ENCODE lzo
	,"creation_dt" varchar(50)   ENCODE lzo
	,"register_dt" varchar(50)   ENCODE lzo
	,"modify_dt" varchar(50)   ENCODE lzo
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("uu_id&hash")
SORTKEY ("uu_id&hash")
;
) by &connection;

execute (

DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.prov_client_organization_mqt";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.prov_client_organization_mqt"
(
	"client_organization_id" VARCHAR(50)   ENCODE lzo
	,"client_organization_name" VARCHAR(255)   ENCODE lzo
	,"access_expiry_days" INTEGER   ENCODE runlength
	,"identity_uuid" VARCHAR(150)   ENCODE lzo
	,"first_add_entitlement_tmstmp" TIMESTAMP WITHOUT TIME ZONE   ENCODE runlength
	,"last_access_tmstmp" TIMESTAMP WITHOUT TIME ZONE   ENCODE runlength
	,"email_dt" DATE   ENCODE runlength
	,"email" VARCHAR(200)   ENCODE lzo
	,"first_name" VARCHAR(200)   ENCODE lzo
	,"last_name" VARCHAR(200)   ENCODE lzo
	,"username" VARCHAR(200)   ENCODE lzo
	,"feature_name" VARCHAR(100)   ENCODE lzo
	,"is_migrated" VARCHAR(3)   ENCODE lzo
	,"enable_welcome_email" CHAR(1)   ENCODE lzo
	,"active_flag" VARCHAR(10)   ENCODE lzo
	,"first_client_registration_dt" DATE   ENCODE runlength
	,"business_owner" VARCHAR(40)   ENCODE lzo
	,"identity_uuid&hash" VARCHAR(128)   ENCODE lzo
	,"email&hash" VARCHAR(128)   ENCODE lzo
	,"first_name&hash" VARCHAR(128)   ENCODE lzo
	,"last_name&hash" VARCHAR(128)   ENCODE lzo
	,"username&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("identity_uuid&hash")
SORTKEY ("identity_uuid&hash"	)
;
) by &connection;

%if &uat_flag=Y %then %do;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.tmp_wsjplus_omniture";
	) by &connection;
execute (

CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.tmp_wsjplus_omniture"
(
	"access_dt" DATE   ENCODE delta
	,"vxid_encrypt" VARCHAR(300)   ENCODE lzo
	,"site_section" VARCHAR(100)   ENCODE lzo
	,"headline_article" VARCHAR(255)   ENCODE lzo
	,"visits" INTEGER   ENCODE delta
	,"unique_visitors" INTEGER   ENCODE runlength
	,"pageviews" INTEGER   ENCODE delta
	,"device" VARCHAR(10)   ENCODE lzo
	,"last_rec_dt" DATE   ENCODE runlength
	,"uuid_upd_dt" DATE   ENCODE runlength
	,"run_dt" DATE   ENCODE runlength
)
DISTSTYLE KEY
DISTKEY ("vxid_encrypt")
SORTKEY ("access_dt")
;
) by &connection;

%end;


execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.ref_country";
	) by &connection;
execute (

CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.ref_country"
(
	"edw_cntry_seq" INTEGER   
	,"edw_ver_num" INTEGER   
	,"iso_cntry_name" VARCHAR(128)   
	,"atex_cntry_name" VARCHAR(128)   
	,"olf_cntry_name" VARCHAR(128)   
	,"ics_cntry_name" VARCHAR(128)   
	,"alpha_cd2" CHAR(2)   
	,"alpha_cd3" CHAR(3)   
	,"iso_num" CHAR(100)   
	,"olf_cntry_cd" CHAR(3)   
	,"olf_cntry_abbr" VARCHAR(64)   
	,"atex_cntry_id" INTEGER   
	,"edw_srce_sys_cd" VARCHAR(3)   
	,"edw_crt_proc" VARCHAR(11)   
	,"edw_eff_dt" DATE   
	,"edw_end_dt" DATE   
	,"edw_row_stat_cd" CHAR(1)   
)
DISTSTYLE KEY
DISTKEY ("edw_cntry_seq")
SORTKEY ("edw_cntry_seq")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.country_dim";
	) by &connection;
execute (

CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.country_dim"
(
	"country_id" INTEGER   
	,"country_name" VARCHAR(100)   
	,"continent" VARCHAR(100)   
	,"rpt_region" VARCHAR(100)   
)
DISTSTYLE KEY
DISTKEY ("country_id")
SORTKEY ("country_id")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.zip_dim";
	) by &connection;
execute (

CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.zip_dim"
(
	"zip_id" INTEGER   ENCODE delta
	,"zip" VARCHAR(10)   ENCODE lzo
	,"city" VARCHAR(100)   ENCODE lzo
	,"state" VARCHAR(200)   ENCODE lzo
	,"dma_cd" VARCHAR(3)   ENCODE bytedict
	,"dma" VARCHAR(100)   ENCODE lzo
	,"country_id" INTEGER   ENCODE bytedict
)
DISTSTYLE KEY
DISTKEY ("zip_id")
SORTKEY ("zip_id")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.mosaic_cust_subscription";
	) by &connection;
execute (

CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.mosaic_cust_subscription"
(
	"uu_id" VARCHAR(150)   ENCODE lzo
	,"login_name" VARCHAR(255)   ENCODE lzo
	,"first_name" VARCHAR(200)   ENCODE lzo
	,"last_name" VARCHAR(200)   ENCODE lzo
	,"email_addr" VARCHAR(200)   ENCODE lzo
	,"email_stat_cd" CHAR(1)   ENCODE lzo
	,"sbscr_id" INTEGER   ENCODE delta
	,"sbscr_type_id" INTEGER   ENCODE delta
	,"sbscr_type_code" VARCHAR(40)   ENCODE lzo
	,"sbscr_state_id" INTEGER   ENCODE delta
	,"sbscr_state_code" VARCHAR(40)   ENCODE lzo
	,"prod_cd" VARCHAR(40)   ENCODE lzo
	,"prod_name" VARCHAR(255)   ENCODE lzo
	,"autorenew_ind" SMALLINT   ENCODE runlength
	,"sbscr_cre_dt" DATE   ENCODE delta32k
	,"sbscr_term_id" INTEGER   ENCODE delta
	,"sbscr_term_type_id" INTEGER   ENCODE runlength
	,"sbscr_term_type_code" VARCHAR(40)   ENCODE lzo
	,"sbscr_term_strt_dt" DATE   ENCODE delta32k
	,"sbscr_term_end_dt" DATE   ENCODE delta32k
	,"sbscr_trmnt_dt" DATE   ENCODE delta32k
	,"sku_id" VARCHAR(40)   ENCODE lzo
	,"dur_unit" VARCHAR(19)   ENCODE lzo
	,"dur_qty" INTEGER   ENCODE bytedict
	,"purch_amt" NUMERIC(10,2)   ENCODE bytedict
	,"purch_currency" VARCHAR(3)   ENCODE lzo
	,"brand_name" VARCHAR(40)   ENCODE lzo
	,"deal_name" VARCHAR(60)   ENCODE lzo
	,"cmpgn_cd" VARCHAR(40)   ENCODE lzo
	,"ord_num" VARCHAR(100)   ENCODE lzo
	,"sbscr_order_action_id" INTEGER   ENCODE runlength
	,"sbscr_order_action_code" VARCHAR(40)   ENCODE lzo
	,"sbscr_order_action_dt" DATE   ENCODE delta32k
	,"order_status_id" INTEGER   ENCODE runlength
	,"order_status_cd" VARCHAR(40)   ENCODE lzo
	,"purch_dt" DATE   ENCODE delta32k
	,"renewal_dt" DATE   ENCODE delta32k
	,"sbscr_grp_id" INTEGER   ENCODE runlength
	,"sbscr_grp_type_id" INTEGER   ENCODE runlength
	,"grp_type_name" VARCHAR(60)   ENCODE lzo
	,"grp_type_cd" VARCHAR(40)   ENCODE lzo
	,"grp_offer_cd" VARCHAR(40)   ENCODE lzo
	,"grp_renwl_rate_sched_id" VARCHAR(60)   ENCODE lzo
	,"track_cd" VARCHAR(50)   ENCODE lzo
	,"report_prod" VARCHAR(100)   ENCODE lzo
	,"prod_distr" VARCHAR(100)   ENCODE lzo
	,"business_seg" VARCHAR(100)   ENCODE lzo
	,"channel" VARCHAR(100)   ENCODE lzo
	,"campaign_name" VARCHAR(100)   ENCODE lzo
	,"campaign_type" VARCHAR(100)   ENCODE lzo
	,"campaign_subtype" VARCHAR(100)   ENCODE lzo
	,"campaign_subtype_detl" VARCHAR(100)   ENCODE lzo
	,"prod_subtype" VARCHAR(100)   ENCODE lzo
	,"prod_subtype_detl" VARCHAR(100)   ENCODE lzo
	,"free_paid_type" VARCHAR(50)   ENCODE lzo
	,"sbscr_status" CHAR(1)   ENCODE lzo
	,"phone_num" VARCHAR(200)   ENCODE lzo
	,"mktplace_id" VARCHAR(40)   ENCODE lzo
	,"edw_report_offer_cd" VARCHAR(40)   ENCODE lzo
	,"print_pub_code" CHAR(1)   ENCODE lzo
	,"bundle_id" VARCHAR(25)   ENCODE lzo
	,"bundle_sku_id" VARCHAR(25)   ENCODE bytedict
	,"bundle_product_id" VARCHAR(25)   ENCODE bytedict
	,"bundle_name" VARCHAR(25)   ENCODE bytedict
	,"promotion_id" VARCHAR(100)   ENCODE lzo
	,"term_coupon_cd" VARCHAR(40)   ENCODE lzo
	,"sku_type" VARCHAR(25)   ENCODE bytedict
	,"is_prorated" VARCHAR(1)   ENCODE lzo
	,"is_transitioned" VARCHAR(1)   ENCODE lzo
	,"is_reactivated" VARCHAR(1)   ENCODE lzo
	,"is_migrated" CHAR(1)   ENCODE lzo
	,"extl_sys_id" INTEGER   ENCODE runlength
	,"extl_sys_sub_id" VARCHAR(100)   ENCODE lzo
	,"ics_cust_id" INTEGER   ENCODE runlength
	,"ics_origl_strt_dt" DATE   ENCODE delta32k
	,"ics_origl_first_out_ifp_dt" DATE   ENCODE delta32k
	,"ics_first_out_ifp_dt" DATE   ENCODE delta32k
	,"sab_placement_id" VARCHAR(40)   ENCODE lzo
	,"print_acct_num" VARCHAR(200)   ENCODE lzo
	,"coupon_cd" VARCHAR(40)   ENCODE lzo
	,"init_ord_source_cd" VARCHAR(25)   ENCODE bytedict
	,"init_ord_agent_uuid" VARCHAR(255)   ENCODE lzo
	,"rcnt_source_cd" VARCHAR(25)   ENCODE bytedict
	,"rcnt_agent_uuid" VARCHAR(255)   ENCODE lzo
	,"assoc_subscr_id" INTEGER   ENCODE delta
	,"assoc_subscr_strt_dt" DATE   ENCODE delta32k
	,"assoc_sub_init_sab_track_cd" VARCHAR(40)   ENCODE lzo
	,"assoc_sub_init_sab_ord_cre_dt" DATE   ENCODE bytedict
	,"assoc_sub_first_out_ifp_dt" DATE   ENCODE delta32k
	,"ics_oot" VARCHAR(50)   ENCODE lzo
	,"edw_futr_term_purch_amt" NUMERIC(11,2)   ENCODE bytedict
	,"edw_futr_term_dur_unit" VARCHAR(19)   ENCODE lzo
	,"edw_futr_term_dur_qty" INTEGER   ENCODE runlength
	,"edw_prev_term_purch_amt" NUMERIC(11,2)   ENCODE bytedict
	,"edw_prev_term_dur_unit" VARCHAR(19)   ENCODE lzo
	,"edw_prev_term_dur_qty" INTEGER   
	,"business_owner" VARCHAR(100)   ENCODE lzo
	,"offer_type" VARCHAR(100)   ENCODE lzo
	,"pay_type" VARCHAR(40)   ENCODE lzo
	,"card_type" VARCHAR(2)   ENCODE lzo
	,"card_num" VARCHAR(25)   ENCODE lzo
	,"exp_month" SMALLINT   ENCODE delta
	,"exp_year" SMALLINT   ENCODE delta
	,"ics_user_type" VARCHAR(50)   ENCODE lzo
	,"ics_user_type_conv_dt" DATE   
	,"bill_sys_id" INTEGER   ENCODE delta
	,"fre_first_out_ifp_dt" DATE   ENCODE bytedict
	,"edw_offer_price" NUMERIC(10,2)   ENCODE bytedict
	,"store_id" VARCHAR(255)   ENCODE lzo
	,"subscr_cancel_event_type_id" INTEGER   ENCODE delta
	,"aup_tmpl_cd" VARCHAR(40)   ENCODE lzo
	,"aup_tracking_cd" VARCHAR(40)   ENCODE lzo
	,"aup_reg_type_cd" INTEGER   ENCODE runlength
	,"prev_offer_type" VARCHAR(100)   ENCODE lzo
	,"offer_type_conversion_dt" DATE   
	,"edw_future_chrg_dt" DATE   
	,"edw_prev_chrg_dt" DATE   
	,"edw_first_out_ifp_dt" DATE   ENCODE delta32k
	,"out_of_ifp_flag" CHAR(1)   ENCODE lzo
	,"card_change_dt" DATE   ENCODE delta
	,"charge_status" VARCHAR(50)   ENCODE lzo
	,"soft_decline_hard_decline_flg" CHAR(1)   ENCODE lzo
	,"delvr_calendar_name" VARCHAR(50)   ENCODE lzo
	,"abc_type" VARCHAR(50)   ENCODE lzo
	,"number_of_copies" SMALLINT   ENCODE runlength
	,"delvr_stat_id" BIGINT   ENCODE delta
	,"subscr_state_name" VARCHAR(60)   ENCODE lzo
	,"subscr_event_type_name" VARCHAR(60)   ENCODE lzo
	,"delvr_stat_name" VARCHAR(60)   ENCODE lzo
	,"edw_proc_error_stat_cd" VARCHAR(1)   ENCODE lzo
	,"init_ord_cre_dt" DATE   ENCODE delta32k
	,"sbscr_strt_dt" DATE   ENCODE delta32k
	,"voucher_cd" VARCHAR(255)   ENCODE lzo
	,"voucher_status" VARCHAR(20)   ENCODE lzo
	,"donor_uu_id" VARCHAR(255)   ENCODE lzo
	,"donor_acct_id" VARCHAR(100)   ENCODE lzo
	,"donor_pymt_instr_id" VARCHAR(100)   ENCODE lzo
	,"voucher_redeem_dt" DATE   
	,"voucher_purch_dt" DATE   
	,"voucher_purch_amt" NUMERIC(11,2)   
	,"first_renewal_sku" VARCHAR(25)   ENCODE bytedict
	,"subscr_reactivation_dt" DATE   
	,"subscr_prior_cancel_event_dt" DATE   ENCODE delta32k
	,"fre_order_id" INTEGER   ENCODE runlength
	,"tax_amt" NUMERIC(11,2)   ENCODE bytedict
	,"srchg_amt" NUMERIC(11,2)   ENCODE runlength
	,"srchg_amt_change_dt" DATE   
	,"base_rate" NUMERIC(19,2)   ENCODE bytedict
	,"price_to_base_percent" SMALLINT   
	,"abc_sub_cat" VARCHAR(40)   ENCODE lzo
	,"marketing_program" VARCHAR(100)   ENCODE lzo
	,"cib_customer_id" INTEGER   ENCODE runlength
	,"company_organization" VARCHAR(100)   ENCODE lzo
	,"business_owner_cl" VARCHAR(100)   ENCODE lzo
	,"prod_type" VARCHAR(100)   ENCODE lzo
	,"program" VARCHAR(100)   ENCODE lzo
	,"program_subtype" VARCHAR(100)   ENCODE lzo
	,"prev_channel" VARCHAR(100)   ENCODE lzo
	,"prev_track_cd" VARCHAR(50)   ENCODE lzo
	,"prev_offer_cd" VARCHAR(40)   ENCODE lzo
	,"prev_offer_price" NUMERIC(10,2)   ENCODE bytedict
	,"prev_srchg_amt" NUMERIC(11,2)   ENCODE runlength
	,"prev_tax_amt" NUMERIC(11,2)   ENCODE bytedict
	,"prev_base_rate" NUMERIC(11,2)   ENCODE bytedict
	,"prev_abc_sub_cat" VARCHAR(40)   ENCODE lzo
	,"prev_marketing_program" VARCHAR(100)   ENCODE lzo
	,"fulfilled_tenure" INTEGER   ENCODE delta32k
	,"tenure_by_campaign" VARCHAR(40)   ENCODE lzo
	,"tenure_by_prod" VARCHAR(40)   ENCODE lzo
	,"tenure_by_cust" VARCHAR(40)   ENCODE lzo
	,"renewal_cnt" INTEGER   ENCODE delta
	,"campaign_renewal_cnt" SMALLINT   ENCODE delta
	,"subscr_renewal_cnt" SMALLINT   ENCODE delta
	,"srce_key" VARCHAR(6)   ENCODE lzo
	,"curr_contract_channel" VARCHAR(50)   ENCODE lzo
	,"curr_contract_sub_channel" VARCHAR(75)   ENCODE lzo
	,"prior_contract_channel" VARCHAR(50)   ENCODE lzo
	,"prior_contract_sub_channel" VARCHAR(75)   ENCODE lzo
	,"origl_channel" VARCHAR(50)   ENCODE lzo
	,"origl_sub_channel" VARCHAR(75)   ENCODE lzo
	,"curr_contract_sbscr_srce_key" VARCHAR(6)   ENCODE lzo
	,"prior_contract_sbscr_srce_key" VARCHAR(6)   ENCODE lzo
	,"origl_srce_key" VARCHAR(6)   ENCODE lzo
	,"pymt_instr_id" VARCHAR(100)   ENCODE lzo
	,"prefix_name" VARCHAR(60)   ENCODE lzo
	,"suffix_name" VARCHAR(60)   ENCODE lzo
	,"identity_acct_num" VARCHAR(100)   ENCODE lzo
	,"acct_num_type" CHAR(1)   ENCODE lzo
	,"term_wks" SMALLINT   ENCODE delta
	,"is_acct_merged" VARCHAR(1)   ENCODE lzo
	,"acct_merge_failure_reason" VARCHAR(255)   ENCODE lzo
	,"tenure_by_campaign_mth" SMALLINT   ENCODE bytedict
	,"tenure_by_prod_mth" SMALLINT   ENCODE bytedict
	,"tenure_by_cust_mth" SMALLINT   ENCODE bytedict
	,"renewal_count" INTEGER   ENCODE delta
	,"current_tracking_cd" VARCHAR(40)   ENCODE lzo
	,"created_date" DATE   ENCODE delta
	,"modified_date" DATE   ENCODE bytedict
	,"conversion_series" VARCHAR(100)   ENCODE lzo
	,"parent_term_dur_unit" VARCHAR(19)   ENCODE lzo
	,"parent_term_dur_qty" INTEGER   ENCODE bytedict
	,"campaign_marketing_program" VARCHAR(100)   ENCODE lzo
	,"pymt_grp_class_type" VARCHAR(40)   ENCODE lzo
	,"pymt_meth" VARCHAR(40)   ENCODE lzo
	,"payment_mode" VARCHAR(50)   ENCODE lzo
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
	,"login_name&hash" VARCHAR(128)   ENCODE lzo
	,"first_name&hash" VARCHAR(128)   ENCODE lzo
	,"last_name&hash" VARCHAR(128)   ENCODE lzo
	,"email_addr&hash" VARCHAR(128)   ENCODE lzo
	,"phone_num&hash" VARCHAR(128)   ENCODE lzo
	,"print_acct_num&hash" VARCHAR(128)   ENCODE lzo
	,"donor_uu_id&hash" VARCHAR(128)   ENCODE lzo
	,"donor_acct_id&hash" VARCHAR(128)   ENCODE lzo
	,"identity_acct_num&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("uu_id&hash")
SORTKEY ("uu_id&hash")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.wsjplus_apac";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.wsjplus_apac"
(
	"uu_id" VARCHAR(150)   
	,"ord_num" VARCHAR(40)   ENCODE lzo
	,"dur_qty" SMALLINT   
	,"dur_unit" VARCHAR(40)   ENCODE lzo
	,"purch_currency" VARCHAR(10)   ENCODE bytedict
	,"purch_amt" NUMERIC(19,2)   ENCODE bytedict
	,"no_of_copies" SMALLINT   
	,"sbscr_first_start_dt" DATE   ENCODE delta32k
	,"sbscr_term_end_dt" DATE   ENCODE delta32k
	,"sbscr_trmnt_dt" DATE   
	,"offer_type" VARCHAR(50)   ENCODE lzo
	,"city" VARCHAR(200)   
	,"province" VARCHAR(100)   ENCODE lzo
	,"postal_cd" VARCHAR(50)   ENCODE lzo
	,"country" VARCHAR(100)   ENCODE lzo
	,"sub_type" VARCHAR(50)   ENCODE lzo
	,"cpn_cd" VARCHAR(50)   ENCODE lzo
	,"corp_name" VARCHAR(100)   
	,"ebm_sbscr_id" VARCHAR(25)   ENCODE lzo
	,"ebm_customer_id" VARCHAR(25)   ENCODE lzo
	,"ebm_first_name" VARCHAR(200)   
	,"ebm_last_name" VARCHAR(200)   
	,"mos_first_name" VARCHAR(200)   
	,"mos_last_name" VARCHAR(200)   
	,"ebm_email_addr" VARCHAR(200)   
	,"mos_email_addr" VARCHAR(200)   
	,"proc_date" DATE   ENCODE runlength
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
	,"city&hash" VARCHAR(128)   ENCODE lzo
	,"ebm_first_name&hash" VARCHAR(128)   ENCODE lzo
	,"ebm_last_name&hash" VARCHAR(128)   ENCODE lzo
	,"mos_first_name&hash" VARCHAR(128)   ENCODE lzo
	,"mos_last_name&hash" VARCHAR(128)   ENCODE lzo
	,"ebm_email_addr&hash" VARCHAR(128)   ENCODE lzo
	,"mos_email_addr&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("uu_id&hash")
SORTKEY ("uu_id&hash")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.wsjplus_emea_indiv";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.wsjplus_emea_indiv"
(
	"slx_contact_id" VARCHAR(25)   
	,"first_name" VARCHAR(200)   
	,"last_name" VARCHAR(200)   
	,"mosaic_ord_num" VARCHAR(30)   
	,"uu_id" VARCHAR(150)   
	,"coupon_code" VARCHAR(60)   ENCODE lzo
	,"mosaic_access_code" VARCHAR(60)   
	,"email" VARCHAR(200)   
	,"sales_ord_prod" VARCHAR(100)   ENCODE lzo
	,"first_sub_start_date" VARCHAR(30)   
	,"subscription_type" VARCHAR(35)   ENCODE lzo
	,"num_copy" NUMERIC(5,1)   
	,"order_total_gross" NUMERIC(12,2)   
	,"order_total_net" NUMERIC(12,2)   
	,"sbscr_term_strt_dt" VARCHAR(30)   ENCODE lzo
	,"sbscr_term_end_dt" VARCHAR(30)   ENCODE lzo
	,"sbscr_trmnt_dt" VARCHAR(30)   
	,"offer_type" VARCHAR(25)   
	,"postal_cd" VARCHAR(25)   
	,"city" VARCHAR(100)   
	,"country" VARCHAR(100)   
	,"slx_ord_det_id" VARCHAR(50)   
	,"first_name&hash" VARCHAR(128)   ENCODE lzo
	,"last_name&hash" VARCHAR(128)   ENCODE lzo
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
	,"email&hash" VARCHAR(128)   ENCODE lzo
	,"city&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("slx_contact_id")
SORTKEY ("slx_contact_id")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.wsjplus_emea_corp";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.wsjplus_emea_corp"
(
	"slx_contact_id" VARCHAR(25)   
	,"first_name" VARCHAR(200)   
	,"last_name" VARCHAR(200)   
	,"mosaic_ord_num" VARCHAR(30)   
	,"uu_id" VARCHAR(150)   
	,"coupon_code" VARCHAR(60)   
	,"mosaic_access_code" VARCHAR(60)   
	,"email" VARCHAR(200)   
	,"secondary_email" VARCHAR(200)   
	,"sales_ord_prod" VARCHAR(100)   
	,"first_sub_start_date" VARCHAR(30)   
	,"subscription_type" VARCHAR(30)   
	,"num_copy" NUMERIC(5,1)   
	,"order_total_gross" NUMERIC(12,2)   
	,"order_total_net" NUMERIC(12,2)   
	,"sbscr_term_strt_dt" VARCHAR(30)   
	,"sbscr_term_end_dt" VARCHAR(30)   
	,"sbscr_trmnt_dt" VARCHAR(30)   
	,"offer_type" VARCHAR(25)   
	,"postal_cd" VARCHAR(25)   
	,"city" VARCHAR(200)   
	,"country" VARCHAR(100)   
	,"slx_ord_det_id" VARCHAR(50)   
	,"organization_name" VARCHAR(100)   
	,"first_name&hash" VARCHAR(128)   
	,"last_name&hash" VARCHAR(128)   
	,"uu_id&hash" VARCHAR(128)   
	,"email&hash" VARCHAR(128)   
	,"secondary_email&hash" VARCHAR(128)   
	,"city&hash" VARCHAR(128)   
)
DISTSTYLE KEY
DISTKEY ("slx_contact_id")
SORTKEY ("slx_contact_id")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.wsjplus_clock";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.wsjplus_clock"
(
	"vxid" VARCHAR(300)   ENCODE lzo
	,"opt_in" VARCHAR(25)   ENCODE lzo
	,"opt_in_tm" TIMESTAMP WITHOUT TIME ZONE   
)
DISTSTYLE KEY
DISTKEY ("vxid")
SORTKEY ("vxid")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.omniture_data";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.omniture_data"
(
	"access_dt" DATE   
	,"vxid_encrypt" VARCHAR(300)   ENCODE lzo
	,"site_section" VARCHAR(100)   
	,"headline_article" VARCHAR(255)   
	,"visits" INTEGER   
	,"unique_visitors" INTEGER   
	,"pageviews" INTEGER   
)
DISTSTYLE KEY
DISTKEY ("vxid_encrypt")
SORTKEY ("access_dt")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.wsj_dailymf";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.wsj_dailymf"
(
	"prod_code" VARCHAR(1)   ENCODE lzo
	,"acct_no" VARCHAR(200)   ENCODE lzo
	,"company" VARCHAR(100)   ENCODE lzo
	,"prefix" VARCHAR(15)   ENCODE lzo
	,"firstname" VARCHAR(150)   ENCODE lzo
	,"lastname" VARCHAR(150)   ENCODE lzo
	,"suffix" VARCHAR(9)   ENCODE lzo
	,"street_number" VARCHAR(100)   ENCODE lzo
	,"street_name" VARCHAR(100)   ENCODE lzo
	,"extra_address" VARCHAR(100)   ENCODE lzo
	,"city" VARCHAR(100)   ENCODE lzo
	,"state" VARCHAR(100)   ENCODE lzo
	,"zipcode_12" VARCHAR(100)   ENCODE lzo
	,"country_code" VARCHAR(10)   ENCODE lzo
	,"system_dt" VARCHAR(14)   ENCODE lzo
	,"trans_date_n_seq" BIGINT   ENCODE bytedict
	,"trans_type" VARCHAR(3)   ENCODE bytedict
	,"trans_source_sub_source" VARCHAR(2)   ENCODE bytedict
	,"user_id" VARCHAR(100)   ENCODE lzo
	,"no_sell_type" VARCHAR(1)   ENCODE lzo
	,"carrier_num" VARCHAR(18)   ENCODE lzo
	,"carrier_instr" VARCHAR(23)   ENCODE lzo
	,"postal_route" VARCHAR(5)   ENCODE bytedict
	,"mail_reason" VARCHAR(2)   ENCODE lzo
	,"print_center" VARCHAR(2)   ENCODE bytedict
	,"request_23star" VARCHAR(1)   ENCODE lzo
	,"request_edition" VARCHAR(2)   ENCODE lzo
	,"olf_edition" VARCHAR(2)   ENCODE bytedict
	,"star_23" VARCHAR(1)   ENCODE lzo
	,"zone" VARCHAR(1)   ENCODE lzo
	,"sub_type" VARCHAR(2)   ENCODE bytedict
	,"last_sub_type" VARCHAR(2)   ENCODE bytedict
	,"restr_access" VARCHAR(1)   ENCODE lzo
	,"super_over" VARCHAR(1)   ENCODE lzo
	,"num_copies" SMALLINT   ENCODE runlength
	,"curr_source_key" VARCHAR(6)   ENCODE bytedict
	,"expire_key" VARCHAR(6)   ENCODE lzo
	,"lead_key" VARCHAR(6)   ENCODE lzo
	,"orig_source" VARCHAR(6)   ENCODE bytedict
	,"original_sales_month" VARCHAR(2)   ENCODE lzo
	,"original_sales_year" VARCHAR(2)   ENCODE lzo
	,"tel_mktg_rep_id" VARCHAR(2)   ENCODE lzo
	,"tel_mktg_offcd" VARCHAR(1)   ENCODE lzo
	,"current_term" VARCHAR(4)   ENCODE bytedict
	,"start_century" VARCHAR(2)   ENCODE lzo
	,"start_year" VARCHAR(2)   ENCODE bytedict
	,"start_month" VARCHAR(2)   ENCODE bytedict
	,"start_day" VARCHAR(2)   ENCODE bytedict
	,"payment_rec" VARCHAR(1)   ENCODE lzo
	,"renewals" INTEGER   ENCODE bytedict
	,"bill_renewals" INTEGER   ENCODE bytedict
	,"renewal_date" BIGINT   ENCODE delta32k
	,"expire_date" DATE   ENCODE delta32k
	,"stop_date" INTEGER   ENCODE bytedict
	,"stop_reason" VARCHAR(2)   ENCODE bytedict
	,"bus_phone" VARCHAR(100)   ENCODE lzo
	,"home_phone" VARCHAR(100)   ENCODE lzo
	,"home_unlist" VARCHAR(1)   ENCODE lzo
	,"fax_number" VARCHAR(100)   ENCODE lzo
	,"group_sale_cd" VARCHAR(1)   ENCODE lzo
	,"abc_rpt_type" VARCHAR(1)   ENCODE lzo
	,"refund_reason" VARCHAR(2)   ENCODE lzo
	,"bill_cycle_ind" VARCHAR(1)   ENCODE bytedict
	,"bill_effort" VARCHAR(2)   ENCODE bytedict
	,"rate_delvy_typ" VARCHAR(1)   ENCODE lzo
	,"series_id" VARCHAR(10)   ENCODE bytedict
	,"series_id_stat" VARCHAR(1)   ENCODE lzo
	,"tran_typ_adj_id" VARCHAR(1)   ENCODE lzo
	,"billplan" VARCHAR(2)   ENCODE lzo
	,"replace_inv_id" VARCHAR(1)   ENCODE lzo
	,"req_inv_id" VARCHAR(1)   ENCODE lzo
	,"special_pro_typ" VARCHAR(1)   ENCODE lzo
	,"pay_del_billto" VARCHAR(1)   ENCODE lzo
	,"premium_chosen" VARCHAR(6)   ENCODE lzo
	,"premium_qualification" VARCHAR(6)   ENCODE lzo
	,"date_paid" DATE   ENCODE delta32k
	,"sub_amt" NUMERIC(10,2)   ENCODE bytedict
	,"spc_postage" NUMERIC(10,2)   ENCODE runlength
	,"sales_tax" NUMERIC(10,2)   ENCODE bytedict
	,"cumm_sub_amt" NUMERIC(10,2)   ENCODE bytedict
	,"cumm_spc_post" NUMERIC(10,2)   ENCODE runlength
	,"cumm_sales_tax" NUMERIC(10,2)   ENCODE bytedict
	,"sale_tax_exdate" VARCHAR(4)   ENCODE lzo
	,"cash_type_code" VARCHAR(1)   ENCODE lzo
	,"effort_paid_on" VARCHAR(2)   ENCODE bytedict
	,"paid_by" VARCHAR(1)   ENCODE lzo
	,"curr_pay_stat" VARCHAR(1)   ENCODE bytedict
	,"credit_card_typ" VARCHAR(100)   ENCODE lzo
	,"credit_card_num" VARCHAR(100)   ENCODE lzo
	,"credit_cd_xdate" VARCHAR(100)   ENCODE lzo
	,"bill_sub_amt" NUMERIC(10,2)   ENCODE bytedict
	,"bill_sales_tax" NUMERIC(10,2)   ENCODE bytedict
	,"bill_spc_post" NUMERIC(10,2)   ENCODE runlength
	,"tran_prod_cd" VARCHAR(1)   ENCODE lzo
	,"tran_acct_num" VARCHAR(12)   ENCODE lzo
	,"rev_acct_date" DATE   ENCODE bytedict
	,"rev_sum_date" DATE   ENCODE runlength
	,"icu_ind" VARCHAR(1)   ENCODE bytedict
	,"chg_add_reason" VARCHAR(1)   ENCODE lzo
	,"add_sub_verf_cd" VARCHAR(1)   ENCODE lzo
	,"valid_add_cd" VARCHAR(4)   ENCODE lzo
	,"bill_to_fields" VARCHAR(500)   ENCODE lzo
	,"corp_acct_no" VARCHAR(200)   ENCODE lzo
	,"purchase_order_flds" VARCHAR(100)   ENCODE lzo
	,"school_number" VARCHAR(5)   ENCODE lzo
	,"course_type" VARCHAR(2)   ENCODE lzo
	,"class_level" VARCHAR(1)   ENCODE lzo
	,"grad_date" DATE   ENCODE bytedict
	,"prof_acct_number" VARCHAR(12)   ENCODE lzo
	,"major_type" VARCHAR(1)   ENCODE lzo
	,"prof_last_ord_date" DATE   ENCODE bytedict
	,"prof_number_copies" INTEGER   ENCODE runlength
	,"prof_qual_copies" INTEGER   ENCODE runlength
	,"prof_type" VARCHAR(1)   ENCODE lzo
	,"online_acct_no" VARCHAR(12)   ENCODE bytedict
	,"cult_upsll_term" VARCHAR(4)   ENCODE lzo
	,"cult_stop_ind" VARCHAR(1)   ENCODE lzo
	,"cult_callback" VARCHAR(1)   ENCODE lzo
	,"request_name" VARCHAR(100)   ENCODE lzo
	,"author_code" VARCHAR(6)   ENCODE lzo
	,"install_code" VARCHAR(2)   ENCODE bytedict
	,"commit_date" INTEGER   ENCODE bytedict
	,"install_paid" SMALLINT   ENCODE runlength
	,"total_install" SMALLINT   ENCODE runlength
	,"term_commmit" INTEGER   ENCODE bytedict
	,"rate_commmit" NUMERIC(10,2)   ENCODE bytedict
	,"line_of_travel" VARCHAR(5)   ENCODE lzo
	,"walk_seq_no" VARCHAR(2)   ENCODE bytedict
	,"we_fields" VARCHAR(500)   ENCODE lzo
	,"address_type" VARCHAR(1)   ENCODE bytedict
	,"addr_source" VARCHAR(1)   ENCODE bytedict
	,"addr_source_typ" VARCHAR(1)   ENCODE bytedict
	,"promotion_id" VARCHAR(15)   ENCODE lzo
	,"promotion_type" VARCHAR(1)   ENCODE bytedict
	,"multi_buyer_ind" VARCHAR(1)   ENCODE lzo
	,"issue_choice" VARCHAR(54)   ENCODE lzo
	,"comp_street_nm" VARCHAR(23)   ENCODE lzo
	,"indiv_prefix" VARCHAR(100)   ENCODE lzo
	,"indiv_firstname" VARCHAR(100)   ENCODE lzo
	,"indiv_lastname" VARCHAR(100)   ENCODE lzo
	,"indiv_suffix" VARCHAR(100)   ENCODE lzo
	,"filler_40" VARCHAR(100)   ENCODE lzo
	,"indiv_title" VARCHAR(16)   ENCODE lzo
	,"we_st_name_compressed" VARCHAR(100)   ENCODE lzo
	,"acct_no&hash" VARCHAR(128)   ENCODE lzo
	,"firstname&hash" VARCHAR(128)   ENCODE lzo
	,"lastname&hash" VARCHAR(128)   ENCODE lzo
	,"street_number&hash" VARCHAR(128)   ENCODE lzo
	,"street_name&hash" VARCHAR(128)   ENCODE lzo
	,"extra_address&hash" VARCHAR(128)   ENCODE lzo
	,"city&hash" VARCHAR(128)   ENCODE lzo
	,"state&hash" VARCHAR(128)   ENCODE lzo
	,"user_id&hash" VARCHAR(128)   ENCODE lzo
	,"bus_phone&hash" VARCHAR(128)   ENCODE lzo
	,"home_phone&hash" VARCHAR(128)   ENCODE lzo
	,"fax_number&hash" VARCHAR(128)   ENCODE lzo
	,"credit_card_typ&hash" VARCHAR(128)   ENCODE lzo
	,"credit_card_num&hash" VARCHAR(128)   ENCODE lzo
	,"credit_cd_xdate&hash" VARCHAR(128)   ENCODE lzo
	,"bill_to_fields&hash" VARCHAR(128)   ENCODE lzo
	,"request_name&hash" VARCHAR(128)   ENCODE lzo
	,"we_fields&hash" VARCHAR(128)   ENCODE lzo
	,"st_name_compressed&hash" VARCHAR(128)   ENCODE lzo
	,"indiv_name&hash" VARCHAR(128)   ENCODE lzo
	,"we_st_name_compressed&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("acct_no&hash")
SORTKEY (
	"prod_code"
	)
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.bar_dailymf";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.bar_dailymf"
(
	"prod_code" VARCHAR(10)   ENCODE lzo
	,"acct_no" VARCHAR(200)   ENCODE lzo
	,"company" VARCHAR(40)   ENCODE lzo
	,"prefix" VARCHAR(15)   ENCODE lzo
	,"firstname" VARCHAR(150)   ENCODE lzo
	,"lastname" VARCHAR(150)   ENCODE lzo
	,"suffix" VARCHAR(9)   ENCODE lzo
	,"street_number" VARCHAR(100)   ENCODE lzo
	,"street_name" VARCHAR(100)   ENCODE lzo
	,"extra_address" VARCHAR(100)   ENCODE lzo
	,"city" VARCHAR(100)   ENCODE lzo
	,"state" VARCHAR(100)   ENCODE lzo
	,"zipcode_12" VARCHAR(120)   ENCODE lzo
	,"country_code" VARCHAR(10)   ENCODE lzo
	,"system_dt" VARCHAR(14)   ENCODE lzo
	,"trans_date_n_seq" BIGINT   ENCODE bytedict
	,"trans_type" VARCHAR(3)   ENCODE bytedict
	,"trans_source_sub_source" VARCHAR(2)   ENCODE bytedict
	,"user_id" VARCHAR(100)   ENCODE lzo
	,"no_sell_type" VARCHAR(1)   ENCODE lzo
	,"carrier_num" VARCHAR(18)   ENCODE lzo
	,"carrier_instr" VARCHAR(23)   ENCODE lzo
	,"postal_route" VARCHAR(5)   ENCODE bytedict
	,"mail_reason" VARCHAR(2)   ENCODE lzo
	,"print_center" VARCHAR(2)   ENCODE bytedict
	,"request_23star" VARCHAR(1)   ENCODE lzo
	,"request_edition" VARCHAR(2)   ENCODE lzo
	,"olf_edition" VARCHAR(2)   ENCODE bytedict
	,"star_23" VARCHAR(1)   ENCODE lzo
	,"zone" VARCHAR(1)   ENCODE lzo
	,"sub_type" VARCHAR(2)   ENCODE bytedict
	,"last_sub_type" VARCHAR(2)   ENCODE bytedict
	,"restr_access" VARCHAR(1)   ENCODE lzo
	,"super_over" VARCHAR(1)   ENCODE lzo
	,"num_copies" SMALLINT   ENCODE runlength
	,"curr_source_key" VARCHAR(6)   ENCODE bytedict
	,"expire_key" VARCHAR(6)   ENCODE lzo
	,"lead_key" VARCHAR(6)   ENCODE lzo
	,"orig_source" VARCHAR(6)   ENCODE bytedict
	,"original_sales_month" VARCHAR(2)   ENCODE lzo
	,"original_sales_year" VARCHAR(2)   ENCODE lzo
	,"tel_mktg_rep_id" VARCHAR(2)   ENCODE lzo
	,"tel_mktg_offcd" VARCHAR(1)   ENCODE lzo
	,"current_term" VARCHAR(4)   ENCODE bytedict
	,"start_century" VARCHAR(2)   ENCODE lzo
	,"start_year" VARCHAR(2)   ENCODE bytedict
	,"start_month" VARCHAR(2)   ENCODE bytedict
	,"start_day" VARCHAR(2)   ENCODE bytedict
	,"payment_rec" VARCHAR(1)   ENCODE lzo
	,"renewals" INTEGER   ENCODE bytedict
	,"bill_renewals" INTEGER   ENCODE delta
	,"renewal_date" BIGINT   ENCODE bytedict
	,"expire_date" DATE   ENCODE delta32k
	,"stop_date" INTEGER   ENCODE bytedict
	,"stop_reason" VARCHAR(2)   ENCODE lzo
	,"bus_phone" VARCHAR(100)   ENCODE lzo
	,"home_phone" VARCHAR(100)   ENCODE lzo
	,"home_unlist" VARCHAR(1)   ENCODE lzo
	,"fax_number" VARCHAR(100)   ENCODE lzo
	,"group_sale_cd" VARCHAR(1)   ENCODE lzo
	,"abc_rpt_type" VARCHAR(1)   ENCODE lzo
	,"refund_reason" VARCHAR(2)   ENCODE lzo
	,"bill_cycle_ind" VARCHAR(1)   ENCODE lzo
	,"bill_effort" VARCHAR(2)   ENCODE bytedict
	,"rate_delvy_typ" VARCHAR(1)   ENCODE lzo
	,"series_id" VARCHAR(10)   ENCODE bytedict
	,"series_id_stat" VARCHAR(1)   ENCODE lzo
	,"tran_typ_adj_id" VARCHAR(1)   ENCODE lzo
	,"billplan" VARCHAR(2)   ENCODE lzo
	,"replace_inv_id" VARCHAR(1)   ENCODE lzo
	,"req_inv_id" VARCHAR(1)   ENCODE lzo
	,"special_pro_typ" VARCHAR(1)   ENCODE lzo
	,"pay_del_billto" VARCHAR(1)   ENCODE lzo
	,"premium_chosen" VARCHAR(6)   ENCODE lzo
	,"premium_qualification" VARCHAR(6)   ENCODE lzo
	,"date_paid" DATE   ENCODE delta32k
	,"sub_amt" NUMERIC(10,2)   ENCODE bytedict
	,"spc_postage" NUMERIC(10,2)   ENCODE runlength
	,"sales_tax" NUMERIC(10,2)   ENCODE bytedict
	,"cumm_sub_amt" NUMERIC(10,2)   ENCODE bytedict
	,"cumm_spc_post" NUMERIC(10,2)   ENCODE runlength
	,"cumm_sales_tax" NUMERIC(10,2)   ENCODE bytedict
	,"sale_tax_exdate" VARCHAR(4)   ENCODE lzo
	,"cash_type_code" VARCHAR(1)   ENCODE lzo
	,"effort_paid_on" VARCHAR(2)   ENCODE bytedict
	,"paid_by" VARCHAR(1)   ENCODE lzo
	,"curr_pay_stat" VARCHAR(1)   ENCODE bytedict
	,"credit_card_typ" VARCHAR(100)   ENCODE lzo
	,"credit_card_num" VARCHAR(100)   ENCODE lzo
	,"credit_cd_xdate" VARCHAR(100)   ENCODE lzo
	,"bill_sub_amt" NUMERIC(10,2)   ENCODE bytedict
	,"bill_sales_tax" NUMERIC(10,2)   ENCODE runlength
	,"bill_spc_post" NUMERIC(10,2)   ENCODE runlength
	,"tran_prod_cd" VARCHAR(1)   ENCODE lzo
	,"tran_acct_num" VARCHAR(12)   ENCODE lzo
	,"rev_acct_date" DATE   ENCODE bytedict
	,"rev_sum_date" DATE   ENCODE runlength
	,"icu_ind" VARCHAR(1)   ENCODE bytedict
	,"chg_add_reason" VARCHAR(1)   ENCODE lzo
	,"add_sub_verf_cd" VARCHAR(1)   ENCODE lzo
	,"valid_add_cd" VARCHAR(4)   ENCODE lzo
	,"bill_to_fields" VARCHAR(500)   ENCODE lzo
	,"corp_acct_no" VARCHAR(20)   ENCODE lzo
	,"purchase_order_flds" VARCHAR(100)   ENCODE lzo
	,"school_number" VARCHAR(5)   ENCODE lzo
	,"course_type" VARCHAR(2)   ENCODE lzo
	,"class_level" VARCHAR(1)   ENCODE lzo
	,"grad_date" DATE   
	,"prof_acct_number" VARCHAR(12)   ENCODE lzo
	,"major_type" VARCHAR(1)   ENCODE lzo
	,"prof_last_ord_date" DATE   
	,"prof_number_copies" INTEGER   ENCODE runlength
	,"prof_qual_copies" INTEGER   ENCODE runlength
	,"prof_type" VARCHAR(1)   ENCODE lzo
	,"online_acct_no" VARCHAR(12)   ENCODE lzo
	,"cult_upsll_term" VARCHAR(4)   ENCODE lzo
	,"cult_stop_ind" VARCHAR(1)   ENCODE lzo
	,"cult_callback" VARCHAR(1)   ENCODE lzo
	,"request_name" VARCHAR(100)   ENCODE lzo
	,"author_code" VARCHAR(6)   ENCODE lzo
	,"install_code" VARCHAR(2)   ENCODE lzo
	,"commit_date" INTEGER   ENCODE runlength
	,"install_paid" SMALLINT   ENCODE runlength
	,"total_install" SMALLINT   ENCODE runlength
	,"term_commmit" INTEGER   ENCODE runlength
	,"rate_commmit" NUMERIC(10,2)   ENCODE bytedict
	,"line_of_travel" VARCHAR(5)   ENCODE lzo
	,"walk_seq_no" VARCHAR(2)   ENCODE bytedict
	,"we_fields" VARCHAR(200)   ENCODE lzo
	,"address_type" VARCHAR(1)   ENCODE lzo
	,"addr_source" VARCHAR(1)   ENCODE lzo
	,"addr_source_typ" VARCHAR(1)   ENCODE lzo
	,"promotion_id" VARCHAR(15)   ENCODE lzo
	,"promotion_type" VARCHAR(1)   ENCODE lzo
	,"multi_buyer_ind" VARCHAR(1)   ENCODE lzo
	,"issue_choice" VARCHAR(54)   ENCODE lzo
	,"comp_street_nm" VARCHAR(23)   ENCODE lzo
	,"indiv_prefix" VARCHAR(9)   ENCODE lzo
	,"indiv_firstname" VARCHAR(16)   ENCODE lzo
	,"indiv_lastname" VARCHAR(23)   ENCODE lzo
	,"indiv_suffix" VARCHAR(100)   ENCODE lzo
	,"filler_40" VARCHAR(100)   ENCODE lzo
	,"indiv_title" VARCHAR(16)   ENCODE lzo
	,"we_st_name_compressed" VARCHAR(100)   ENCODE lzo
	,"acct_no&hash" VARCHAR(128)   ENCODE lzo
	,"firstname&hash" VARCHAR(128)   ENCODE lzo
	,"lastname&hash" VARCHAR(128)   ENCODE lzo
	,"street_number&hash" VARCHAR(128)   ENCODE lzo
	,"street_name&hash" VARCHAR(128)   ENCODE lzo
	,"extra_address&hash" VARCHAR(128)   ENCODE lzo
	,"city&hash" VARCHAR(128)   ENCODE lzo
	,"state&hash" VARCHAR(128)   ENCODE lzo
	,"user_id&hash" VARCHAR(128)   ENCODE lzo
	,"bus_phone&hash" VARCHAR(128)   ENCODE lzo
	,"home_phone&hash" VARCHAR(128)   ENCODE lzo
	,"fax_number&hash" VARCHAR(128)   ENCODE lzo
	,"credit_card_typ&hash" VARCHAR(128)   ENCODE lzo
	,"credit_card_num&hash" VARCHAR(128)   ENCODE lzo
	,"credit_cd_xdate&hash" VARCHAR(128)   ENCODE lzo
	,"bill_to_fields&hash" VARCHAR(128)   ENCODE lzo
	,"request_name&hash" VARCHAR(128)   ENCODE lzo
	,"we_fields&hash" VARCHAR(128)   ENCODE lzo
	,"st_name_compressed&hash" VARCHAR(128)   ENCODE lzo
	,"indiv_name&hash" VARCHAR(128)   ENCODE lzo
	,"we_st_name_compressed&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("acct_no&hash")
SORTKEY ("prod_code")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.loadfile";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.loadfile"
(
	"acct_no" VARCHAR(200)   ENCODE lzo
	,"prod_code" VARCHAR(1)   ENCODE bytedict
	,"email_address" VARCHAR(200)   ENCODE lzo
	,"renew_via_email" VARCHAR(1)   ENCODE lzo
	,"req_ques_via_email" VARCHAR(1)   ENCODE lzo
	,"req_djps_via_email" VARCHAR(1)   ENCODE lzo
	,"wsjie_sub" VARCHAR(1)   ENCODE lzo
	,"acct_no&hash" VARCHAR(128)   ENCODE lzo
	,"email_address&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("acct_no&hash")
SORTKEY ("acct_no&hash")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.prov_identity_prim_ent_mqt";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.prov_identity_prim_ent_mqt"
(
	"identity_uuid" VARCHAR(150)   ENCODE lzo
	,"legacy_uuid" VARCHAR(200)   ENCODE lzo
	,"app_name" VARCHAR(100)   ENCODE lzo
	,"ent_name" VARCHAR(100)   ENCODE lzo
	,"ent_active_ind" VARCHAR(1)   ENCODE lzo
	,"first_add_ent_dt_tm" TIMESTAMP WITHOUT TIME ZONE   ENCODE runlength
	,"add_ent_dt_tm" TIMESTAMP WITHOUT TIME ZONE   ENCODE runlength
	,"remove_ent_dt_tm" TIMESTAMP WITHOUT TIME ZONE   ENCODE runlength
	,"identity_ent_status" VARCHAR(1)   ENCODE lzo
	,"uu_id" VARCHAR(150)   ENCODE lzo
	,"identity_uuid&hash" VARCHAR(128)   ENCODE lzo
	,"legacy_uuid&hash" VARCHAR(128)   ENCODE lzo
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("identity_uuid&hash")
SORTKEY ("identity_uuid&hash")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.olf_master";
	) by &connection;
execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.olf_master"
(
	"uu_id" VARCHAR(150)   ENCODE lzo
	,"prod_cd" VARCHAR(1)   ENCODE lzo
	,"acct_num" VARCHAR(128)   ENCODE lzo
	,"co_name" VARCHAR(128)   ENCODE lzo
	,"prefix" VARCHAR(9)   ENCODE lzo
	,"first_name" VARCHAR(200)   ENCODE lzo
	,"last_name" VARCHAR(200)   ENCODE lzo
	,"suffix" VARCHAR(9)   ENCODE lzo
	,"email_addr" VARCHAR(200)   ENCODE lzo
	,"street_num" VARCHAR(100)   ENCODE lzo
	,"addr" VARCHAR(100)   ENCODE lzo
	,"addr2" VARCHAR(100)   ENCODE lzo
	,"city" VARCHAR(100)   ENCODE lzo
	,"state" VARCHAR(100)   ENCODE lzo
	,"zip_5" VARCHAR(5)   ENCODE lzo
	,"zip_plus_4" VARCHAR(4)   ENCODE lzo
	,"zip_plus_3" VARCHAR(3)   ENCODE lzo
	,"postal_cd" VARCHAR(12)   ENCODE lzo
	,"cntry_cd" VARCHAR(3)   ENCODE lzo
	,"sys_dt" DATE   ENCODE bytedict
	,"sys_time" VARCHAR(10)   ENCODE lzo
	,"trans_dt" DATE   ENCODE bytedict
	,"trans_seq_num" INTEGER   ENCODE bytedict
	,"trans_type_cd" VARCHAR(3)   ENCODE bytedict
	,"trans_type" VARCHAR(1)   ENCODE bytedict
	,"tran_rsn_type" VARCHAR(2)   ENCODE bytedict
	,"trans_srce" VARCHAR(1)   ENCODE lzo
	,"trans_sub_srce" VARCHAR(1)   ENCODE bytedict
	,"rep_user_id" VARCHAR(8)   ENCODE bytedict
	,"no_sell_type" VARCHAR(1)   ENCODE lzo
	,"delvy_ind" VARCHAR(1)   ENCODE lzo
	,"carrier_type" VARCHAR(1)   ENCODE bytedict
	,"carrier_rte" VARCHAR(4)   ENCODE bytedict
	,"carrier_city_cd" VARCHAR(2)   ENCODE bytedict
	,"carrier_rte_rem" VARCHAR(3)   ENCODE bytedict
	,"carrier_sequence_num" VARCHAR(5)   ENCODE lzo
	,"carrier_floor_num" VARCHAR(8)   ENCODE lzo
	,"carrier_num" VARCHAR(18)   ENCODE lzo
	,"carrier_delvy_instruct" VARCHAR(23)   ENCODE lzo
	,"postal_rte" VARCHAR(5)   ENCODE bytedict
	,"mail_rsn_cd" VARCHAR(2)   ENCODE lzo
	,"print_cntr" VARCHAR(2)   ENCODE bytedict
	,"rqst_2_3_star" VARCHAR(1)   ENCODE lzo
	,"rqst_edition" VARCHAR(2)   ENCODE lzo
	,"olf_edition" VARCHAR(2)   ENCODE bytedict
	,"star_2_3" VARCHAR(1)   ENCODE lzo
	,"zone" VARCHAR(1)   ENCODE lzo
	,"sbscr_type" VARCHAR(1)   ENCODE bytedict
	,"sbscr_sub_type" VARCHAR(1)   ENCODE bytedict
	,"last_sbscr_type" VARCHAR(1)   ENCODE bytedict
	,"last_sbscr_sub_type" VARCHAR(1)   ENCODE bytedict
	,"restr_access" VARCHAR(1)   ENCODE lzo
	,"supervisor_overide_ind" VARCHAR(1)   ENCODE lzo
	,"num_of_copy" SMALLINT   ENCODE runlength
	,"curr_srce_key" VARCHAR(6)   ENCODE bytedict
	,"expr_srce_key" VARCHAR(6)   ENCODE lzo
	,"lead_srce_key" VARCHAR(6)   ENCODE lzo
	,"origl_srce_key" VARCHAR(6)   ENCODE bytedict
	,"origl_sales_ccyymm" VARCHAR(6)   ENCODE lzo
	,"tel_mktg_rep_id" VARCHAR(2)   ENCODE lzo
	,"tel_mktg_office_cd" VARCHAR(1)   ENCODE lzo
	,"curr_term" SMALLINT   ENCODE bytedict
	,"sbscr_strt_dt" DATE   ENCODE delta32k
	,"payment_received" VARCHAR(1)   ENCODE lzo
	,"renwl_cnt" SMALLINT   ENCODE bytedict
	,"bill_renwl_cnt" SMALLINT   ENCODE bytedict
	,"renwl_dt" DATE   ENCODE delta32k
	,"expr_dt" DATE   ENCODE delta32k
	,"stop_dt" DATE   ENCODE bytedict
	,"stop_rsn" VARCHAR(1)   ENCODE bytedict
	,"stop_sub_rsn" VARCHAR(1)   ENCODE lzo
	,"bus_phone" VARCHAR(10)   ENCODE lzo
	,"home_phone" VARCHAR(10)   ENCODE lzo
	,"home_unlist" VARCHAR(1)   ENCODE lzo
	,"fax_num" VARCHAR(10)   ENCODE lzo
	,"grp_sale_cd" VARCHAR(1)   ENCODE lzo
	,"abc_rpt_type" VARCHAR(1)   ENCODE lzo
	,"refund_rsn" VARCHAR(2)   ENCODE lzo
	,"bill_cycle_ind" VARCHAR(1)   ENCODE bytedict
	,"bill_efrt_num" VARCHAR(2)   ENCODE bytedict
	,"rate_delvy_type" VARCHAR(1)   ENCODE lzo
	,"mkt_seg_id" VARCHAR(10)   ENCODE bytedict
	,"series_1" VARCHAR(1)   ENCODE lzo
	,"series_9" VARCHAR(9)   ENCODE bytedict
	,"mkt_seg_id_status" VARCHAR(1)   ENCODE lzo
	,"tran_type_adj_id" VARCHAR(1)   ENCODE lzo
	,"bill_plan_cd" VARCHAR(1)   ENCODE lzo
	,"bill_sub_plan_cd" VARCHAR(1)   ENCODE lzo
	,"replace_inv_id" VARCHAR(1)   ENCODE lzo
	,"req_inv_id" VARCHAR(1)   ENCODE lzo
	,"specl_process_type" VARCHAR(1)   ENCODE lzo
	,"upon_pay_del_billto_ind" VARCHAR(1)   ENCODE lzo
	,"curr_prem_cd_1" VARCHAR(2)   ENCODE lzo
	,"curr_prem_cd_2" VARCHAR(2)   ENCODE lzo
	,"curr_prem_cd_3" VARCHAR(2)   ENCODE lzo
	,"qual_prem1" VARCHAR(1)   ENCODE lzo
	,"qual_prem2" VARCHAR(1)   ENCODE lzo
	,"qual_prem3" VARCHAR(1)   ENCODE lzo
	,"date_paid" DATE   ENCODE bytedict
	,"sbscr_amt_applied" NUMERIC(8,2)   ENCODE bytedict
	,"specl_pstge_applied" NUMERIC(6,2)   ENCODE runlength
	,"sales_tax_applied" NUMERIC(5,2)   ENCODE bytedict
	,"cumul_amt_paid" NUMERIC(8,2)   ENCODE bytedict
	,"cumul_specl_pstge_paid" NUMERIC(6,2)   ENCODE runlength
	,"cumul_sales_tax_paid" NUMERIC(5,2)   ENCODE bytedict
	,"sales_tax_exmpt_expr_ccyymm" VARCHAR(6)   ENCODE lzo
	,"cash_type_cd" VARCHAR(1)   ENCODE lzo
	,"efrt_paid_on" VARCHAR(2)   ENCODE bytedict
	,"paid_by" VARCHAR(1)   ENCODE lzo
	,"curr_pay_status" VARCHAR(1)   ENCODE bytedict
	,"cc_type_cd" VARCHAR(1)   ENCODE bytedict
	,"cc_4digits" VARCHAR(16)   ENCODE lzo
	,"cc_expr_ccyymm" VARCHAR(6)   ENCODE bytedict
	,"sbscr_amt_bill" NUMERIC(8,2)   ENCODE bytedict
	,"specl_pstge_amt_bill" NUMERIC(6,2)   ENCODE runlength
	,"sales_tax_bill" NUMERIC(5,2)   ENCODE bytedict
	,"transfer_prod_cd" VARCHAR(1)   ENCODE lzo
	,"transfer_acct_num" VARCHAR(12)   ENCODE lzo
	,"rev_acct_dt" DATE   ENCODE bytedict
	,"rev_sum_dt" DATE   ENCODE bytedict
	,"icu_ind" VARCHAR(1)   ENCODE bytedict
	,"chng_addr_rsn" VARCHAR(1)   ENCODE lzo
	,"addr_sub_verf_ind" VARCHAR(1)   ENCODE lzo
	,"valid_addr_cd_mmyy" VARCHAR(4)   ENCODE lzo
	,"bill_co_name" VARCHAR(128)   ENCODE lzo
	,"bill_prefix" VARCHAR(100)   ENCODE lzo
	,"bill_first_name" VARCHAR(200)   ENCODE lzo
	,"bill_last_name" VARCHAR(200)   ENCODE lzo
	,"bill_suffix" VARCHAR(100)   ENCODE lzo
	,"bill_street_num" VARCHAR(100)   ENCODE lzo
	,"bill_addr" VARCHAR(100)   ENCODE lzo
	,"bill_addr2" VARCHAR(100)   ENCODE lzo
	,"bill_city" VARCHAR(100)   ENCODE lzo
	,"bill_state" VARCHAR(100)   ENCODE lzo
	,"bill_zip_5" VARCHAR(5)   ENCODE lzo
	,"bill_zip_plus_4" VARCHAR(4)   ENCODE lzo
	,"bill_zip_plus_3" VARCHAR(3)   ENCODE lzo
	,"bill_postal_cd" VARCHAR(12)   ENCODE lzo
	,"bill_cntry_cd" VARCHAR(3)   ENCODE lzo
	,"donor_prod_cd" VARCHAR(1)   ENCODE lzo
	,"donor_acct_num" VARCHAR(12)   ENCODE lzo
	,"billto_renwl_cnt" SMALLINT   ENCODE runlength
	,"cre_num_adopt" INTEGER   ENCODE runlength
	,"sponsor_info_ind" VARCHAR(1)   ENCODE lzo
	,"bill_chng_addr_rsn" VARCHAR(1)   ENCODE lzo
	,"bill_addr_sub_verf_ind" VARCHAR(1)   ENCODE lzo
	,"bill_valid_addr_cd_mmyy" VARCHAR(4)   ENCODE lzo
	,"corp_acct_num" VARCHAR(5)   ENCODE lzo
	,"purch_num" VARCHAR(20)   ENCODE lzo
	,"purch_name" VARCHAR(23)   ENCODE lzo
	,"purch_dt" DATE   ENCODE runlength
	,"school_num" VARCHAR(5)   ENCODE lzo
	,"course_type" VARCHAR(2)   ENCODE lzo
	,"class_level" VARCHAR(1)   ENCODE lzo
	,"grad_dt" DATE   ENCODE runlength
	,"profs_acct_num" VARCHAR(12)   ENCODE lzo
	,"major_type" VARCHAR(1)   ENCODE lzo
	,"prof_last_ord_dt" DATE   ENCODE runlength
	,"prof_num_of_copy" SMALLINT   ENCODE runlength
	,"prof_qual_num_of_copy" SMALLINT   ENCODE runlength
	,"prof_type" VARCHAR(1)   ENCODE lzo
	,"online_acct_num" VARCHAR(12)   ENCODE bytedict
	,"cult_upsell_term" VARCHAR(4)   ENCODE lzo
	,"cult_stop_ind" VARCHAR(1)   ENCODE lzo
	,"cult_callback_ind" VARCHAR(1)   ENCODE lzo
	,"rqst_name" VARCHAR(23)   ENCODE lzo
	,"authorization_cd" VARCHAR(6)   ENCODE lzo
	,"instlmt_cd" VARCHAR(2)   ENCODE bytedict
	,"commit_expr_dt" DATE   ENCODE bytedict
	,"instlmt_paid" SMALLINT   ENCODE runlength
	,"total_instlmt" SMALLINT   ENCODE runlength
	,"term_commmit" SMALLINT   ENCODE runlength
	,"rate_commmit" NUMERIC(6,2)   ENCODE bytedict
	,"line_of_travel" VARCHAR(5)   ENCODE lzo
	,"lot_seq_num" VARCHAR(4)   ENCODE bytedict
	,"lot_asc_desc_cd" VARCHAR(1)   ENCODE lzo
	,"walk_seq_num" VARCHAR(2)   ENCODE bytedict
	,"we_co_name" VARCHAR(128)   ENCODE lzo
	,"we_prefix" VARCHAR(100)   ENCODE lzo
	,"we_first_name" VARCHAR(200)   ENCODE lzo
	,"we_last_name" VARCHAR(200)   ENCODE lzo
	,"we_suffix" VARCHAR(100)   ENCODE lzo
	,"we_street_num" VARCHAR(100)   ENCODE lzo
	,"we_addr" VARCHAR(100)   ENCODE lzo
	,"we_addr2" VARCHAR(100)   ENCODE lzo
	,"we_city" VARCHAR(100)   ENCODE lzo
	,"we_state" VARCHAR(100)   ENCODE lzo
	,"we_zip_5" VARCHAR(5)   ENCODE lzo
	,"we_zip_plus_4" VARCHAR(4)   ENCODE lzo
	,"we_zip_plus_3" VARCHAR(3)   ENCODE lzo
	,"we_postal_cd" VARCHAR(12)   ENCODE lzo
	,"we_cntry_cd" VARCHAR(3)   ENCODE lzo
	,"we_delvy_ind" VARCHAR(1)   ENCODE lzo
	,"we_carrier_type" VARCHAR(1)   ENCODE bytedict
	,"we_carrier_rte" VARCHAR(4)   ENCODE bytedict
	,"we_carrier_city_cd" VARCHAR(2)   ENCODE bytedict
	,"we_carrier_rte_rem" VARCHAR(3)   ENCODE bytedict
	,"we_carrier_sequence_num" VARCHAR(5)   ENCODE lzo
	,"we_carrier_floor_num" VARCHAR(8)   ENCODE lzo
	,"we_carrier_num" VARCHAR(18)   ENCODE lzo
	,"we_carrier_inst" VARCHAR(23)   ENCODE lzo
	,"we_mail_rsn" VARCHAR(2)   ENCODE lzo
	,"we_print_cntr" VARCHAR(2)   ENCODE bytedict
	,"we_rqst_ed" VARCHAR(2)   ENCODE lzo
	,"we_olf_edition" VARCHAR(2)   ENCODE bytedict
	,"we_chng_addr_rsn" VARCHAR(1)   ENCODE lzo
	,"we_valid_addr_cd_mmyy" VARCHAR(4)   ENCODE lzo
	,"we_postal_rte" VARCHAR(5)   ENCODE bytedict
	,"we_line_of_travel" VARCHAR(5)   ENCODE lzo
	,"we_lot_seq_num" VARCHAR(4)   ENCODE bytedict
	,"we_lot_asc_desc_cd" VARCHAR(1)   ENCODE lzo
	,"we_walk_seq_num" VARCHAR(2)   ENCODE bytedict
	,"we_opt_out_cd" VARCHAR(2)   ENCODE lzo
	,"we_opt_out_dt" DATE   ENCODE runlength
	,"we_addr_type" VARCHAR(1)   ENCODE bytedict
	,"we_addr_srce" VARCHAR(1)   ENCODE bytedict
	,"we_addr_srce_type" VARCHAR(1)   ENCODE bytedict
	,"we_promo_id" VARCHAR(15)   ENCODE lzo
	,"we_promo_type" VARCHAR(1)   ENCODE bytedict
	,"multi_buyer_ind" VARCHAR(1)   ENCODE lzo
	,"iss_choice" VARCHAR(2)   ENCODE lzo
	,"last_renew_iss_choice" VARCHAR(2)   ENCODE lzo
	,"corp_opt_out_cd" VARCHAR(2)   ENCODE lzo
	,"we_stop_dt" DATE   ENCODE runlength
	,"addr_compressed" VARCHAR(23)   ENCODE lzo
	,"indiv_prefix" VARCHAR(9)   ENCODE lzo
	,"indiv_first_name" VARCHAR(200)   ENCODE lzo
	,"indiv_last_name" VARCHAR(200)   ENCODE lzo
	,"indiv_suffix" VARCHAR(9)   ENCODE lzo
	,"indiv_title" VARCHAR(16)   ENCODE lzo
	,"we_addr_compressed" VARCHAR(200)   ENCODE lzo
	,"trmnt_dt" DATE   ENCODE bytedict
	,"cycle_dt" DATE   ENCODE runlength
	,"iss_dt" DATE   ENCODE delta
	,"first_paid_dt" DATE   ENCODE bytedict
	,"first_paid_amt" NUMERIC(8,2)   ENCODE bytedict
	,"first_committed_paid_amt" NUMERIC(8,2)   ENCODE bytedict
	,"renwl_paid_amt" NUMERIC(8,2)   ENCODE bytedict
	,"renwl_committed_paid_amt" NUMERIC(8,2)   ENCODE bytedict
	,"new_ord_in_icap" INTEGER   ENCODE runlength
	,"new_ord_amt_in_icap" NUMERIC(11,2)   ENCODE bytedict
	,"uu_id&hash" VARCHAR(128)   ENCODE lzo
	,"acct_num&hash" VARCHAR(128)   ENCODE lzo
	,"first_name&hash" VARCHAR(128)   ENCODE lzo
	,"last_name&hash" VARCHAR(128)   ENCODE lzo
	,"email_addr&hash" VARCHAR(128)   ENCODE lzo
	,"street_num&hash" VARCHAR(128)   ENCODE lzo
	,"addr&hash" VARCHAR(128)   ENCODE lzo
	,"addr2&hash" VARCHAR(128)   ENCODE lzo
	,"city&hash" VARCHAR(128)   ENCODE lzo
	,"state&hash" VARCHAR(128)   ENCODE lzo
	,"bill_first_name&hash" VARCHAR(128)   ENCODE lzo
	,"bill_last_name&hash" VARCHAR(128)   ENCODE lzo
	,"bill_street_num&hash" VARCHAR(128)   ENCODE lzo
	,"bill_addr&hash" VARCHAR(128)   ENCODE lzo
	,"bill_addr2&hash" VARCHAR(128)   ENCODE lzo
	,"bill_city&hash" VARCHAR(128)   ENCODE lzo
	,"bill_state&hash" VARCHAR(128)   ENCODE lzo
	,"we_first_name&hash" VARCHAR(128)   ENCODE lzo
	,"we_last_name&hash" VARCHAR(128)   ENCODE lzo
	,"we_street_num&hash" VARCHAR(128)   ENCODE lzo
	,"we_addr&hash" VARCHAR(128)   ENCODE lzo
	,"we_addr2&hash" VARCHAR(128)   ENCODE lzo
	,"we_city&hash" VARCHAR(128)   ENCODE lzo
	,"we_state&hash" VARCHAR(128)   ENCODE lzo
	,"indiv_first_name&hash" VARCHAR(128)   ENCODE lzo
	,"indiv_last_name&hash" VARCHAR(128)   ENCODE lzo
	,"we_addr_compressed&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("acct_num&hash")
SORTKEY ("acct_num&hash")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.prov_customer_login_vw";
	) by &connection;

execute (
CREATE TABLE IF NOT EXISTS "&redshift_schema"."&temp_string.prov_customer_login_vw"
(
	"identity_uuid" VARCHAR(150)   ENCODE lzo
	,"client_org_id" VARCHAR(100)   ENCODE lzo
	,"vxid" VARCHAR(300)   ENCODE lzo
	,"identity_uuid&hash" VARCHAR(128)   ENCODE lzo
)
DISTSTYLE KEY
DISTKEY ("identity_uuid&hash")
SORTKEY ("identity_uuid&hash")
;
) by &connection;

execute (
DROP TABLE IF EXISTS "&redshift_schema"."&temp_string.PROV_ENTITLEMENT_FEATURE_REF";
	) by &connection;


execute (
CREATE TABLE IF NOT EXISTS &redshift_schema..&temp_string.PROV_ENTITLEMENT_FEATURE_REF
(
  ENT_NAME varchar(100)
, FEATURE_NAME varchar(100) distkey sortkey
, APP_NAME varchar(100)
, DEFAULT_ENT_NAME varchar(50)
, ENT_ACTIVE_IND varchar(1)
) ;
) by &connection;


execute (
DROP TABLE IF EXISTS &redshift_schema..staging_load_control;
	) by &connection;
 
execute ( 
create table if not exists &redshift_schema..staging_load_control 
(
	control_line varchar(1000)
);

	) by &connection;
			disconnect from &connection;
		quit;

%mend create_tmp_tables;

