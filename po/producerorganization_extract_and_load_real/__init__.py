import logging
import math
import pysurveycto
import pandas as pd
from io import StringIO
import io as IO
from datetime import datetime
from time import gmtime, strftime
from datetime import datetime, date
from statistics import variance
import pytz
import numpy as np
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient


def download_file_from_directory(datalake_service_client, filesystem_name, pre_path, suf_path):

    file_system_client = datalake_service_client.get_file_system_client(
        file_system=filesystem_name)

    directory_client = file_system_client.get_directory_client(pre_path)

    local_file_path = "/tmp/{}".format(suf_path)

    local_file = open(local_file_path, 'wb')

    file_client = directory_client.get_file_client(suf_path)

    download = file_client.download_file()

    downloaded_bytes = download.readall()

    local_file.write(downloaded_bytes)

    local_file.close()

    ALP_labels = pd.read_excel(local_file_path,
                               sheet_name="Selective", dtype={'variable': float},
                               index_col=None)
    return ALP_labels


def count_size(x):

    shed_size_template = """shed_size_{}"""
    if math.isnan(x['shed_num_count']):
        return 0
    else:
        count = 0
        for i in range(1, int(x['shed_num_count'])+1):
            shed_size_template.format(i)
            count += x[shed_size_template.format(i)]
        return count


def extract(server_name, username, password, form_id, project, phase):

    scto = pysurveycto.SurveyCTOObject(server_name, username, password)
    data = scto.get_form_data(form_id, format='csv')
    df = pd.read_csv(StringIO(data))
    df = df[(df['project'] == project) & (df['phase_pl'] == phase)]
    if df.empty:
        return pd.read_csv('producerorganization_extract_and_load_real/tmp_raw/surveycto_data.csv')
    return df


def init_datalake_service_client(account_name, account_key):

    datalake_service_client = DataLakeServiceClient(
        "https://{}.dfs.core.windows.net".format(account_name), credential=account_key)

    return datalake_service_client


def load_xlsx(datalake_service_client, filesystem_name, pre_path, file_name, sheets_name, *datas):
    tmp_path = '/tmp/' + file_name
    writer = pd.ExcelWriter(tmp_path, engine='xlsxwriter')
    io = IO.BytesIO()
    if(len(datas) == 1):
        datas[0].to_excel(writer, index=False)
    else:
        i = 0
        for data in datas:
            data.to_excel(writer, sheet_name=sheets_name[i], index=False)
            i += 1
    writer.book.filename = io
    writer.save()
    io_value = io.getvalue()

    file_system_client = datalake_service_client.get_file_system_client(
        file_system=filesystem_name)
    directory_client = file_system_client.get_directory_client(pre_path)
    file_client = directory_client.create_file(file_name)
    file_client.append_data(data=io_value, offset=0)
    file_client.flush_data(len(io_value))


def transform(root_dir, project, client, df, path):
# authentication
    filesystem_name = "data"
    realtime_path = f"{root_dir}/realtime/"
    validate_path = path.replace("processed", "validate")

    #
    # ### Tab 1: Open notes
    #


    open_notes_qc = df.loc[:, ["enumerator", "poid_key", "consent",
                            "primary_resp_name_final", "businessname_final", "open_notes"]]


    #
    # ### Tab 2: Duration
    #


    duration_qc = df.loc[:, ["enumerator", "poid_key", "consent",
                            "primary_resp_name_final", "businessname_final", "duration", "duration_min"]]
    # average duration for all surveys
    duration_qc['duration_survey_avg'] = strftime(
        "%H:%M:%S", gmtime(duration_qc['duration'].mean())
    )
    # average duration for each enumerator
    # calculate average each enumerator
    duration_e_enum = df.groupby(df.enumerator).agg(
        duration_enum_avg=('duration', 'mean')).reset_index()
    # convert to hh:mm:ss format
    duration_e_enum['duration_enum_avg'] = duration_e_enum['duration_enum_avg'].apply(lambda x: strftime(
        "%H:%M:%S", gmtime(x)
    ))
    # Merge with tab 2 dataframe
    duration_qc = duration_qc.merge(
        duration_e_enum, on='enumerator', how='left')


    #
    # ### Tab 3: Consent
    #


    consent_qc = df.loc[:, ["enumerator", "poid_key", "consent",
                            "primary_resp_name_final", "businessname_final"]]


    #
    # ### Tab 4: Survey and Respondent review
    #


    survey_res_review_qc = df.loc[:, ["poid_key", "primary_resp_name_final", "businessname_final", "rate_resp_ee",
                                    "rate_resp_o", "rate_resp_p", "rate_resp_oh", "rate_resp_a", "rate_resp_k", "enum_info",
                                    "enum_respanswer", "enum_rightresp"]]

    ### To Excel
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    review_file_name = ("Review notes for {}.xlsx").format(project)
    validate_sheets = ['Open notes', 'Duration',
                       'Consent', 'Survey and Respondent review']
    load_xlsx(client, filesystem_name, validate_path, review_file_name, validate_sheets,
              open_notes_qc,
              duration_qc,
              consent_qc,
              survey_res_review_qc)


    #
    # ## Change text format - All first letter is upper case
    #


    #
    # ### column for upper case
    #


    col_capitalize = "project,enumerator,resp_sex_pl,assessor_pl,phase_pl,primary_resp_name_final,businessname_final,admin1_final,admin2_final,admin3_final,admin4_final"
    upper_str = col_capitalize.split(",")


    #
    # ### capitalize data
    #


    for i in upper_str:
        if df[i].isna().all():
            df[i] == df[i].fillna('N/A')
        else:
            df[i] = df[i].fillna("")
            df[i] = df[i].apply(lambda x: " ".join(x.title().split()))


    # ## Identify and drop
    #


    # Non-consent data (consent = 0)
    df = df[df['consent'] != 0]
    # Rearrange to start index at 1, not 0
    df.index = np.arange(1, len(df)+1)


    df_meta_df = ['SubmissionDate',
                'starttime',
                'endtime',
                'deviceid',
                'subscriberid',
                'simid',
                'devicephonenum',
                'location-Latitude',
                'location-Longitude',
                'location-Altitude',
                'location-Accuracy',
                'duration',
                'duration_min',
                'project',
                'enumerator',
                'poid_key',
                'consent',
                'resp_label_pl',
                'resp_label_eng_pl',
                'resp_name_pl',
                'resp_sex_pl',
                'resp_mobilenum_pl',
                'resp_mobilenum_2_pl',
                'resp_position_pl',
                'businessname_pl',
                'countrycurrency_pl',
                'usd_exchange_rate_pl',
                'country_pl',
                'admin1_pl',
                'admin1_label_pl',
                'admin1_label_eng_pl',
                'admin2_pl',
                'admin2_label_pl',
                'admin2_label_eng_pl',
                'admin3_pl',
                'admin3_label_pl',
                'admin3_label_eng_pl',
                'admin4_pl',
                'admin4_label_pl',
                'admin4_label_eng_pl',
                'assessor_pl',
                'assessor_email_pl',
                'assessor_mobilenum_pl',
                'ifcproject_pl',
                'projectdescription_pl',
                'client_pl',
                'phase_pl',
                'a2f_pl',
                'ohada_pl',
                'currentyear',
                'primary_resp_listed',
                'primary_resp_confirm',
                'primary_resp_correct1',
                'primary_resp_correct2',
                'primary_resp_name_final',
                'primary_resp_gender_final',
                'primary_resp_literacy',
                'primary_resp_literacy_primaryyears',
                'primary_resp_position_final',
                'primary_resp_phone_final',
                'businessname_final',
                'admin1_text',
                'admin2_text',
                'admin3_text',
                'admin4_text',
                'admin1_final',
                'admin2_final',
                'admin3_final',
                'admin4_final',
                'pub_to_users',
                'rate_resp_ee',
                'rate_resp_o',
                'rate_resp_p',
                'rate_resp_oh',
                'rate_resp_a',
                'rate_resp_k',
                'enum_info',
                'enum_respanswer',
                'enum_rightresp',
                'open_notes',
                'instanceID',
                'instanceName',
                'formdef_version',
                'KEY'].copy()

    # Mapping meta data
    df_result = df.loc[:, df_meta_df]


    #
    # # Prep step
    # # Fill null values with 0 so that score calculations produce final number
    pts_cols_fillna = ['pts_records', 'pts_records1', 'pts_records2', 'pts_records3',
                       'pts_records4', 'pts_records5', 'pts_ledger',
                       'pts_ledger_cash', 'pts_ledger_sales', 'pts_ledger_expense', 'pts_ledger_asset',
                       'pts_ledger_inv', 'pts_ledger_loancredit', 'pts_ledger_bus_ledger', 'app_acct', 'app_tracing',
                       'app_inv', 'app_cpayment', 'app_fpayment', 'app_gps', 'app_onlineaccess', 'app_ict_s', 'app_ict_c',
                       'app97']

    for i in df.columns:
        if i in pts_cols_fillna:
            df[i].fillna(0, inplace=True)

    rmee_cols_fillna = ['rm_insurance', 'rm_question', 'rm_p_insurance', 'rm_v_insurance',
                        'rm_l_insurance', 'rm_h_insurance', 'rm_storage', 'rm_97_insurance',
                        'rm_writtencash', 'rm_writteninvent', 'rm_locked', 'rm_security',
                        'rm_safe', 'rm_budget', 'rm_inventory', 'rm_cash', 'rm_reserves',
                        'rm_succession', 'rm_insurance97']
    for i in df.columns:
        if i in rmee_cols_fillna:
            df[i].fillna(0, inplace=True)


    ## Convert starttime to format dd/mm/yyyy, new column “startdate”
    df['startdate'] = df["starttime"].apply(lambda x: datetime.strptime(
        x, "%b %d, %Y %I:%M:%S %p").strftime('%d/%m/%Y'))
    ## Convert endtime to format dd/mm/yyyy, new column name “enddate”
    df['enddate'] = df["endtime"].apply(lambda x: datetime.strptime(
        x, "%b %d, %Y %I:%M:%S %p").strftime('%d/%m/%Y'))
    ## Create new column “location_combine” by combining “admin3_final, admin4_final”
    df['location_combine'] = df['admin3_final'].astype(
        str) + ', ' + df['admin4_final'].astype(str)


    #
    # # Score calculation
    #


    # Score calculation
    # Member Services and Governance msg_
    # Answer Scores (unweighted)

    # Does the ${resp_label_pl} hold <i>at least one</i> meeting of members (or General Assembly meeting) per year?
    df['msg_meeting_sc'] = np.where(df['msg_meeting'] == 1, 100, 0)

    # Does the ${resp_label_pl} elect its leaders from amongst its members?
    df['msg_elect_leader_sc'] = np.where(df['msg_elect_leader'] == 1, 100, 0)

    # Does the ${resp_label_pl} have a <b>hired manager</b> who manages the ${resp_label_pl}’s activities on a day-to-day basis?
    df['msg_manager_has_sc'] = np.where(df['msg_manager_has'] == 1, 100, 0)

    # <p>How many people does the ${resp_label_pl} currently <b>employ as regular, paid workers</b>?  Please do not include seasonal workers.</p>
    df['msg_employ_total_sc'] = np.where(df['msg_employ_total'] > 3, 100,
                                        np.where((df['msg_employ_total'] > 0) & (df['msg_employ_total'] <= 3), 50, 0))

    # % of members who sell at least 75% of their product thru PO OR % of members that receive at least 75% of inputs from PO OR AVERAGE of both

    df['msg_loyal_member_sc'] = np.where((df['msg_marketprod'] == 1) & (df['msg_sellinput'] == 1), (((df['msg_loyal_product'] + df['msg_loyal_inputs'])/(2*df['msg_member'])))*100,
                                        np.where(df['msg_marketprod'] == 1, (df['msg_loyal_product']/df['msg_member'])*100,
                                                np.where(df['msg_sellinput'] == 1, (df['msg_loyal_inputs']/df['msg_member'])*100,
                                                        0)))

    # <p><b>Which, <i>if any</i>, of the following services does the ${resp_label_pl} provide to members?</b></p>
    df['msg_service_sc'] = (df['msg_service_edu_prod']*10
                            + df['msg_service_edu_org']*5
                            + df['msg_service_edu_business']*10
                            + df['msg_service_loans']*15
                            + df['msg_service_comm_invest']*10
                            + df['msg_service_equip']*10
                            + df['msg_service_farm_services']*10
                            + df['msg_service_farm_advice']*15
                            + df['msg_service_childcare']*10
                            + df['msg_service_transport_train']*5)

    # <p><b>When the ${resp_label_pl} markets products on behalf of members, does the ${resp_label_pl} do any of the following?</b></p>
    df['msg_marketprodx_sc'] = (df['msg_marketprodx_transport_org']*10
                                + df['msg_marketprodx_transport_buyer']*15
                                + df['msg_marketprodx_store']*10
                                + df['msg_marketprodx_process']*20
                                + df['msg_marketprodx_not_own']*5
                                + df['msg_marketprodx_own']*10
                                + df['msg_marketprodx_pay_org']*10
                                + df['msg_marketprodx_pay_buyer']*0
                                + df['msg_marketprodx_sell_onebuyer']*0
                                + df['msg_marketprodx_sell_multibuyer']*5
                                + df['msg_marketprodx_contract_buyer']*15
                                + df['msg_marketprodx97']*0)

    # <p><b>When the ${resp_label_pl} provides inputs to members, does the ${resp_label_pl} do any of the following?</b></p>
    df['msg_input_sc'] = (df['msg_input_own']*20
                        + df['msg_input_distrib']*0
                        + df['msg_input_credit']*15
                        + df['msg_input_training']*25
                        + df['msg_input_transport_farm']*20
                        + df['msg_input_transport_org']*20
                        + df['msg_input_at_cost']*0
                        + df['msg_input97']*0)

    # Weighted Question/Variables Score
    # Apply the question weights to each score answer variable
    df['msg_meeting_scw'] = df['msg_meeting_sc'] * 0.115
    df['msg_elect_leader_scw'] = df['msg_elect_leader_sc'] * 0.115
    df['msg_manager_has_scw'] = df['msg_manager_has_sc'] * 0.15
    df['msg_employ_total_scw'] = df['msg_employ_total_sc'] * 0.05
    df['msg_loyal_member_scw'] = df['msg_loyal_member_sc'] * 0.30
    df['msg_service_scw'] = df['msg_service_sc'] * 0.05
    df['msg_marketprodx_scw'] = df['msg_marketprodx_sc'] * 0.11
    df['msg_input_scw'] = df['msg_input_sc'] * 0.11

    msg_scw_vars = ['msg_meeting_scw', 'msg_elect_leader_scw', 'msg_manager_has_scw', 'msg_employ_total_scw',
                    'msg_loyal_member_scw', 'msg_service_scw', 'msg_marketprodx_scw', 'msg_input_scw']

    # Performance Tracking Systems and Technology/Digital Integrations `pts_` and 'tdiapps_'
    # <p><b>Which, if any, of the following <i>records</i> does the ${resp_label_pl} maintain?</b></p>
    df['pts_records_sc'] = (df['pts_records_reg']*7.5
                            + df['pts_records_tax']*15
                            + df['pts_records_bank']*5
                            + df['pts_records_contract_supplier']*10
                            + df['pts_records_member_register']*15
                            + df['pts_records_contract_buyer']*10
                            + df['pts_records_prod_license']*0
                            + df['pts_records_sale_invoice']*12.5
                            + df['pts_records_purch_invoice']*12.5
                            + df['pts_records_bills']*12.5)

    df['pts_records'] = np.where(df['pts_records_sc'] >= 1, 'Yes', 'No')

    # Does the ${resp_label_pl} do bookkeeping?
    df['pts_bk_sc'] = np.where(df['pts_bk'] == 1, 100, 0)

    # Which of the following methods best describes the bookkeeping system?
    df['pts_bk_how_sc'] = np.where(df['pts_bk_how'] == 2, 100,
                                np.where(df['pts_bk_how'] == 1, 25,
                                            np.where(df['pts_bk_how'] == 3, 50,
                                                    0)))

    # <p><b>Which, if any, of the following <i>business activities</i> does the ${resp_label_pl} document?</b></p>
    df['pts_ledger_sc'] = (df['pts_ledger_cash']*20
                        + df['pts_ledger_sales']*15
                        + df['pts_ledger_expense']*15
                        + df['pts_ledger_asset']*10
                        + df['pts_ledger_inv']*10
                        + df['pts_ledger_loancredit']*15
                        + df['pts_ledger_bus_ledger']*15)

    # <p><b>Which, if any, of the following <i>financial statements</i> does the ${resp_label_pl} prepare?</b></p>
    df['pts_fs_sc'] = (df['pts_fs_cash']*25
                    + df['pts_fs_pl']*50
                    + df['pts_fs_bs']*25)

    # <p>Are financial statements <b>audited by a qualified accountant</b>?</p>
    df['pts_fs_audit_sc'] = np.where(df['pts_fs_audit'] == 1, 100, 0)

    # <p><b>Which, if any, of the following <i>technology applications</i> does the ${resp_label_pl} use?</b></p>
    df['pts_tdiapps_sc'] = (df['app_acct']*5
                            + df['app_tracing']*5
                            + df['app_inv']*20
                            + df['app_cpayment']*5
                            + df['app_fpayment']*20
                            + df['app_gps']*5
                            + df['app_onlineaccess']*10
                            + df['app_ict_s']*15
                            + df['app_ict_c']*15
                            + df['app97']*0)

    # Weighted Question/Variables Score
    # Apply the question weights to each scored answer variable
    df['pts_records_scw'] = df['pts_records_sc']*0.20
    df['pts_bk_scw'] = df['pts_bk_sc']*0.20
    df['pts_bk_how_scw'] = df['pts_bk_how_sc']*0.025
    df['pts_ledger_scw'] = df['pts_ledger_sc']*0.25
    df['pts_fs_scw'] = df['pts_fs_sc']*0.125
    df['pts_fs_audit_scw'] = df['pts_fs_audit_sc']*0.05
    df['pts_tdiapps_scw'] = df['pts_tdiapps_sc']*0.15

    pts_scw_vars = ['pts_records_scw', 'pts_bk_scw', 'pts_bk_how_scw',
                    'pts_ledger_scw', 'pts_fs_scw', 'pts_fs_audit_scw', 'pts_tdiapps_scw']

    # Planning Pratices pp_

    # Does the ${resp_label_pl} prepare an <b>annual operating plan</b>?
    df['pp_aplan_sc'] = np.where(df['pp_aplan'] == 1, 100, 0)

    # Is the annual operating plan written down?
    df['pp_written_sc'] = np.where(df['pp_written'] == 1, 100, 0)

    # Does the annual operating plan include a <b>budget</b>?
    df['pp_aplanbudget_sc'] = np.where(df['pp_aplanbudget'] == 1, 100, 0)

    # Weighted Question/Variables Score
    # Apply the question weights to each scored answer variable
    df['pp_aplan_scw'] = df['pp_aplan_sc']*0.33
    df['pp_written_scw'] = df['pp_written_sc']*0.33
    df['pp_aplanbudget_scw'] = df['pp_aplanbudget_sc']*0.34
    pp_scw_vars = ['pp_aplan_scw', 'pp_written_scw', 'pp_aplanbudget_scw']

    # Risk Management & External Engagement rmee_
    # <p><b>Which, if any, of the following <i>risk management practices</i> does the ${resp_label_pl} use?</b></p>
    df['rm_sc'] = (df['rm_locked']*5
                + df['rm_security']*5
                + df['rm_safe']*5
                + df['rm_reserves']*15
                + df['rm_succession']*15
                + df['rm_cash']*15
                + df['rm_inventory']*10
                + df['rm_budget']*10
                + df['rm_writtencash']*10
                + df['rm_writteninvent']*10
                + df['rm_v_insurance']*0
                + df['rm_h_insurance']*0
                + df['rm_p_insurance']*0
                + df['rm_l_insurance']*0
                + df['rm_storage']*0
                + df['rm_97_insurance']*0)

    # <p>How <i>frequently</i> does the ${resp_label_pl} <b>update the cash record</b>?</p>
    df['rm_ledger_update_sc'] = np.where(df['pts_ledger_update'] == 1, 100,
                                        np.where(df['pts_ledger_update'] == 2, 75,
                                                np.where(df['pts_ledger_update'] == 3, 25,
                                                        np.where(df['pts_ledger_update'] == 4, 0,
                                                                    np.where(df['pts_ledger_update'] == 5, 0,
                                                                            np.where(df['pts_ledger_update'] == 6, 0,
                                                                                    np.where(df['pts_ledger_update'] == 7, 0,
                                                                                            np.where(df['pts_ledger_update'] == 8, 0,
                                                                                                        np.where(df['pts_ledger_update'] == 9, 0,
                                                                                                                0)))))))))

    # <p>How <i>frequently</i> does the ${resp_label_pl} <b>reconcile inventory</b>?</p>
    df['rm_inventory_sc'] = np.where(df['pts_inventory'] == 2, 100,
                                    np.where(df['pts_inventory'] == 3, 50,
                                            np.where(df['pts_inventory'] == 1, 25,
                                                    np.where(df['pts_inventory'] == 4, 25,
                                                                np.where(df['pts_inventory'] == 5, 0,
                                                                        np.where(df['pts_inventory'] == 6, 0,
                                                                                np.where(df['pts_inventory'] == 7, 0,
                                                                                        np.where(df['pts_inventory'] == 8, 0,
                                                                                                    np.where(df['pts_inventory'] == 9, 0,
                                                                                                            0)))))))))

    # <p><b>Which, if any, of the following <i>written polices</i> does the ${resp_label_pl} have?</b></p>
    df['rm_wp_sc'] = (df['wp_boardpolicy']*15
                    + df['wp_emppolicy']*15
                    + df['wp_cashmpolicy']*25
                    + df['wp_fmpolicy']*25
                    + df['wp_rmpolicy']*20
                    + df['wp_other']*0)

    # Is the ${resp_label_pl} <b>officially registered</b>?
    df['rm_reg_sc'] = np.where(df['ee_reg'] == 1, 100, 0)

    # Have any <b>leaders of the ${resp_label_pl} participated in training programs</b> related to the business in the <b>past 3 years</b>?
    df['rm_training_sc'] = np.where(df['ee_training'] == 1, 100, 0)

    # Does the ${resp_label_pl} belong to any <b>professional ${resp_label_pl}s or groups</b>, including cooperative federations?
    df['rm_group_sc'] = np.where(df['ee_group'] == 1, 100, 0)

    # Has the ${resp_label_pl} previously or does it currently <b>own another business</b> or <b>have investment(s) in another business</b>?
    df['rm_otherbusiness_sc'] = np.where(df['ee_otherbusiness'] == 1, 100, 0)

    # Weighted Question/Variable Score
    # Apply the question weights to each scored answer variable
    df['rm_scw'] = df['rm_sc']*0.30
    df['rm_ledger_update_scw'] = df['rm_ledger_update_sc']*0.15
    df['rm_inventory_scw'] = df['rm_inventory_sc']*0.15
    df['rm_wp_scw'] = df['rm_wp_sc']*0.15
    df['rm_reg_scw'] = df['rm_reg_sc']*0.10
    df['rm_training_scw'] = df['rm_training_sc']*0.05
    df['rm_groups_scw'] = df['rm_group_sc']*0.05
    df['rm_otherbusiness_scw'] = df['rm_otherbusiness_sc']*0.05

    rmee_scw_vars = ['rm_scw', 'rm_ledger_update_scw', 'rm_inventory_scw', 'rm_wp_scw',
                    'rm_reg_scw', 'rm_training_scw', 'rm_groups_scw', 'rm_otherbusiness_scw']

    # Operational & Financial Performance ofp_
    # Was the Organization profitable in the last operating year?
    df['ofp_profit_nearestyear_sc'] = np.where(
        (df['ofp_profit_nearestyear'].notna()) & (df['ofp_profit_nearestyear'] > 0), 100, 0)

    # <b><p>Which of the following <b>sources of income</b> does the ${resp_label_pl} have?</p></b>
    df['ofp_income_sc'] = (df['ofp_income_margins_prod']*25
                        + df['ofp_income_margins_inputs']*25
                        + df['ofp_income_salary']*15
                        + df['ofp_income_fees']*15
                        + df['ofp_income_trainingfees']*15
                        + df['ofp_income_custom_fees']*5
                        + df['ofp_income_govt']*0
                        + df['ofp_income_97']*0)

    # Does the ${resp_label_pl} have a <b>bank account</b>?
    df['ofp_bankacct_sc'] = np.where(df['ofp_bankacct'] == 1, 100, 0)

    # Has the Organization <b>obtained a loan or credit</b> within the <b>last 3 years</b>?  This includes loans from financial institutions,
    # inputs on credit or supplier credit, as well as advances to purchase farmer products.
    df['ofp_borrowed_sc'] = np.where(df['ofp_borrowed'] == 1, 100, 0)

    # <p>How does the ${resp_label_pl} <i>currently</i> finance the business?</p>
    df['ofp_current_fin_sc'] = np.where(df['ofp_current_fin'].str.contains('2', regex=False), 100,
                                        np.where(df['ofp_current_fin'].str.contains('3', regex=False), 80,
                                                np.where(df['ofp_current_fin'].str.contains('5', regex=False), 70,
                                                        np.where(df['ofp_current_fin'].str.contains('6', regex=False), 70,
                                                                np.where(df['ofp_current_fin'].str.contains('1', regex=False), 0,
                                                                            np.where(df['ofp_current_fin'].str.contains('4', regex=False), 0,
                                                                                    0))))))

    # Weighted Question/Variable Score
    # Apply the question weights to each score answer variable
    df['ofp_profit_nearestyear_scw'] = df['ofp_profit_nearestyear_sc']*0.20
    df['ofp_current_fin_scw'] = df['ofp_current_fin_sc']*0.20
    df['ofp_bankacct_scw'] = df['ofp_bankacct_sc']*0.20
    df['ofp_borrowed_scw'] = df['ofp_borrowed_sc']*0.20
    df['ofp_income_scw'] = df['ofp_income_sc']*0.20

    ofp_scw_vars = ['ofp_profit_nearestyear_scw', 'ofp_current_fin_scw'
                    'ofp_bankacct_scw', 'ofp_borrowed_scw', 'ofp_income_scw']

    # Category Scores, Benchmarks, Total Score
    # Additional calculations and new variable construction

    # Total Member loyalty ratio and average – Inputs
    df['loyal_ratio_inputs_members'] = (
        (df['msg_loyal_inputs']/df['msg_member'])*100).round(1)
    # Female member loyalty ratio and average – Inputs
    df['loyal_ratio_inputs_women'] = (
        (df['msg_loyal_inputs_women']/df['msg_loyal_inputs'])*100).round(1)
    # Male member loyalty ratio and average – Inputs
    df['loyal_ratio_inputs_men'] = (
        (df['msg_loyal_inputs_men']/df['msg_loyal_inputs'])*100).round(1)

    # Total member loyalty ratio and average – Product
    df['loyal_ratio_product_members'] = (
        (df['msg_loyal_product']/df['msg_member'])*100).round(1)
    # Female member loyalty ratio and average – Product# Score calculation
    # Member Services and Governance msg_
    # Answer Scores (unweighted)

    # Does the ${resp_label_pl} hold <i>at least one</i> meeting of members (or General Assembly meeting) per year?
    df['msg_meeting_sc'] = np.where(df['msg_meeting'] == 1, 100, 0)

    # Does the ${resp_label_pl} elect its leaders from amongst its members?
    df['msg_elect_leader_sc'] = np.where(df['msg_elect_leader'] == 1, 100, 0)

    # Does the ${resp_label_pl} have a <b>hired manager</b> who manages the ${resp_label_pl}’s activities on a day-to-day basis?
    df['msg_manager_has_sc'] = np.where(df['msg_manager_has'] == 1, 100, 0)

    # <p>How many people does the ${resp_label_pl} currently <b>employ as regular, paid workers</b>?  Please do not include seasonal workers.</p>
    df['msg_employ_total_sc'] = np.where(df['msg_employ_total'] > 3, 100,
                                        np.where((df['msg_employ_total'] > 0) & (df['msg_employ_total'] <= 3), 50, 0))

    # % of members who sell at least 75% of their product thru PO OR % of members that receive at least 75% of inputs from PO OR AVERAGE of both

    df['msg_loyal_member_sc'] = np.where((df['msg_marketprod'] == 1) & (df['msg_sellinput'] == 1), (((df['msg_loyal_product'] + df['msg_loyal_inputs'])/(2*df['msg_member'])))*100,
                                        np.where(df['msg_marketprod'] == 1, (df['msg_loyal_product']/df['msg_member'])*100,
                                                np.where(df['msg_sellinput'] == 1, (df['msg_loyal_inputs']/df['msg_member'])*100,
                                                        0)))

    # <p><b>Which, <i>if any</i>, of the following services does the ${resp_label_pl} provide to members?</b></p>
    df['msg_service_sc'] = (df['msg_service_edu_prod']*10
                            + df['msg_service_edu_org']*5
                            + df['msg_service_edu_business']*10
                            + df['msg_service_loans']*15
                            + df['msg_service_comm_invest']*10
                            + df['msg_service_equip']*10
                            + df['msg_service_farm_services']*10
                            + df['msg_service_farm_advice']*15
                            + df['msg_service_childcare']*10
                            + df['msg_service_transport_train']*5)

    # <p><b>When the ${resp_label_pl} markets products on behalf of members, does the ${resp_label_pl} do any of the following?</b></p>
    df['msg_marketprodx_sc'] = (df['msg_marketprodx_transport_org']*10
                                + df['msg_marketprodx_transport_buyer']*15
                                + df['msg_marketprodx_store']*10
                                + df['msg_marketprodx_process']*20
                                + df['msg_marketprodx_not_own']*5
                                + df['msg_marketprodx_own']*10
                                + df['msg_marketprodx_pay_org']*10
                                + df['msg_marketprodx_pay_buyer']*0
                                + df['msg_marketprodx_sell_onebuyer']*0
                                + df['msg_marketprodx_sell_multibuyer']*5
                                + df['msg_marketprodx_contract_buyer']*15
                                + df['msg_marketprodx97']*0)

    # <p><b>When the ${resp_label_pl} provides inputs to members, does the ${resp_label_pl} do any of the following?</b></p>
    df['msg_input_sc'] = (df['msg_input_own']*20
                        + df['msg_input_distrib']*0
                        + df['msg_input_credit']*15
                        + df['msg_input_training']*25
                        + df['msg_input_transport_farm']*20
                        + df['msg_input_transport_org']*20
                        + df['msg_input_at_cost']*0
                        + df['msg_input97']*0)

    # Weighted Question/Variables Score
    # Apply the question weights to each score answer variable
    df['msg_meeting_scw'] = df['msg_meeting_sc'] * 0.115
    df['msg_elect_leader_scw'] = df['msg_elect_leader_sc'] * 0.115
    df['msg_manager_has_scw'] = df['msg_manager_has_sc'] * 0.15
    df['msg_employ_total_scw'] = df['msg_employ_total_sc'] * 0.05
    df['msg_loyal_member_scw'] = df['msg_loyal_member_sc'] * 0.30
    df['msg_service_scw'] = df['msg_service_sc'] * 0.05
    df['msg_marketprodx_scw'] = df['msg_marketprodx_sc'] * 0.11
    df['msg_input_scw'] = df['msg_input_sc'] * 0.11

    msg_scw_vars = ['msg_meeting_scw', 'msg_elect_leader_scw', 'msg_manager_has_scw', 'msg_employ_total_scw',
                    'msg_loyal_member_scw', 'msg_service_scw', 'msg_marketprodx_scw', 'msg_input_scw']

    # Performance Tracking Systems and Technology/Digital Integrations `pts_` and 'tdiapps_'
    # <p><b>Which, if any, of the following <i>records</i> does the ${resp_label_pl} maintain?</b></p>
    df['pts_records_sc'] = (df['pts_records_reg']*7.5
                            + df['pts_records_tax']*15
                            + df['pts_records_bank']*5
                            + df['pts_records_contract_supplier']*10
                            + df['pts_records_member_register']*15
                            + df['pts_records_contract_buyer']*10
                            + df['pts_records_prod_license']*0
                            + df['pts_records_sale_invoice']*12.5
                            + df['pts_records_purch_invoice']*12.5
                            + df['pts_records_bills']*12.5)

    df['pts_records'] = np.where(df['pts_records_sc'] >= 1, 'Yes', 'No')

    # Does the ${resp_label_pl} do bookkeeping?
    df['pts_bk_sc'] = np.where(df['pts_bk'] == 1, 100, 0)

    # Which of the following methods best describes the bookkeeping system?
    df['pts_bk_how_sc'] = np.where(df['pts_bk_how'] == 2, 100,
                                np.where(df['pts_bk_how'] == 1, 25,
                                            np.where(df['pts_bk_how'] == 3, 50,
                                                    0)))

    # <p><b>Which, if any, of the following <i>business activities</i> does the ${resp_label_pl} document?</b></p>
    df['pts_ledger_sc'] = (df['pts_ledger_cash']*20
                        + df['pts_ledger_sales']*15
                        + df['pts_ledger_expense']*15
                        + df['pts_ledger_asset']*10
                        + df['pts_ledger_inv']*10
                        + df['pts_ledger_loancredit']*15
                        + df['pts_ledger_bus_ledger']*15)

    # <p><b>Which, if any, of the following <i>financial statements</i> does the ${resp_label_pl} prepare?</b></p>
    df['pts_fs_sc'] = (df['pts_fs_cash']*25
                    + df['pts_fs_pl']*50
                    + df['pts_fs_bs']*25)

    # <p>Are financial statements <b>audited by a qualified accountant</b>?</p>
    df['pts_fs_audit_sc'] = np.where(df['pts_fs_audit'] == 1, 100, 0)

    # <p><b>Which, if any, of the following <i>technology applications</i> does the ${resp_label_pl} use?</b></p>
    df['pts_tdiapps_sc'] = (df['app_acct']*5
                            + df['app_tracing']*5
                            + df['app_inv']*20
                            + df['app_cpayment']*5
                            + df['app_fpayment']*20
                            + df['app_gps']*5
                            + df['app_onlineaccess']*10
                            + df['app_ict_s']*15
                            + df['app_ict_c']*15
                            + df['app97']*0)

    # Weighted Question/Variables Score
    # Apply the question weights to each scored answer variable
    df['pts_records_scw'] = df['pts_records_sc']*0.20
    df['pts_bk_scw'] = df['pts_bk_sc']*0.20
    df['pts_bk_how_scw'] = df['pts_bk_how_sc']*0.025
    df['pts_ledger_scw'] = df['pts_ledger_sc']*0.25
    df['pts_fs_scw'] = df['pts_fs_sc']*0.125
    df['pts_fs_audit_scw'] = df['pts_fs_audit_sc']*0.05
    df['pts_tdiapps_scw'] = df['pts_tdiapps_sc']*0.15

    pts_scw_vars = ['pts_records_scw', 'pts_bk_scw', 'pts_bk_how_scw',
                    'pts_ledger_scw', 'pts_fs_scw', 'pts_fs_audit_scw', 'pts_tdiapps_scw']

    # Planning Pratices pp_

    # Does the ${resp_label_pl} prepare an <b>annual operating plan</b>?
    df['pp_aplan_sc'] = np.where(df['pp_aplan'] == 1, 100, 0)

    # Is the annual operating plan written down?
    df['pp_written_sc'] = np.where(df['pp_written'] == 1, 100, 0)

    # Does the annual operating plan include a <b>budget</b>?
    df['pp_aplanbudget_sc'] = np.where(df['pp_aplanbudget'] == 1, 100, 0)

    # Weighted Question/Variables Score
    # Apply the question weights to each scored answer variable
    df['pp_aplan_scw'] = df['pp_aplan_sc']*0.33
    df['pp_written_scw'] = df['pp_written_sc']*0.33
    df['pp_aplanbudget_scw'] = df['pp_aplanbudget_sc']*0.34
    pp_scw_vars = ['pp_aplan_scw', 'pp_written_scw', 'pp_aplanbudget_scw']

    # Risk Management & External Engagement rmee_
    # <p><b>Which, if any, of the following <i>risk management practices</i> does the ${resp_label_pl} use?</b></p>
    df['rm_sc'] = (df['rm_locked']*5
                + df['rm_security']*5
                + df['rm_safe']*5
                + df['rm_reserves']*15
                + df['rm_succession']*15
                + df['rm_cash']*15
                + df['rm_inventory']*10
                + df['rm_budget']*10
                + df['rm_writtencash']*10
                + df['rm_writteninvent']*10
                + df['rm_v_insurance']*0
                + df['rm_h_insurance']*0
                + df['rm_p_insurance']*0
                + df['rm_l_insurance']*0
                + df['rm_storage']*0
                + df['rm_97_insurance']*0)

    # <p>How <i>frequently</i> does the ${resp_label_pl} <b>update the cash record</b>?</p>
    df['rm_ledger_update_sc'] = np.where(df['pts_ledger_update'] == 1, 100,
                                        np.where(df['pts_ledger_update'] == 2, 75,
                                                np.where(df['pts_ledger_update'] == 3, 25,
                                                        np.where(df['pts_ledger_update'] == 4, 0,
                                                                    np.where(df['pts_ledger_update'] == 5, 0,
                                                                            np.where(df['pts_ledger_update'] == 6, 0,
                                                                                    np.where(df['pts_ledger_update'] == 7, 0,
                                                                                            np.where(df['pts_ledger_update'] == 8, 0,
                                                                                                        np.where(df['pts_ledger_update'] == 9, 0,
                                                                                                                0)))))))))

    # <p>How <i>frequently</i> does the ${resp_label_pl} <b>reconcile inventory</b>?</p>
    df['rm_inventory_sc'] = np.where(df['pts_inventory'] == 2, 100,
                                    np.where(df['pts_inventory'] == 3, 50,
                                            np.where(df['pts_inventory'] == 1, 25,
                                                    np.where(df['pts_inventory'] == 4, 25,
                                                                np.where(df['pts_inventory'] == 5, 0,
                                                                        np.where(df['pts_inventory'] == 6, 0,
                                                                                np.where(df['pts_inventory'] == 7, 0,
                                                                                        np.where(df['pts_inventory'] == 8, 0,
                                                                                                    np.where(df['pts_inventory'] == 9, 0,
                                                                                                            0)))))))))

    # <p><b>Which, if any, of the following <i>written polices</i> does the ${resp_label_pl} have?</b></p>
    df['rm_wp_sc'] = (df['wp_boardpolicy']*15
                    + df['wp_emppolicy']*15
                    + df['wp_cashmpolicy']*25
                    + df['wp_fmpolicy']*25
                    + df['wp_rmpolicy']*20
                    + df['wp_other']*0)

    # Is the ${resp_label_pl} <b>officially registered</b>?
    df['rm_reg_sc'] = np.where(df['ee_reg'] == 1, 100, 0)

    # Have any <b>leaders of the ${resp_label_pl} participated in training programs</b> related to the business in the <b>past 3 years</b>?
    df['rm_training_sc'] = np.where(df['ee_training'] == 1, 100, 0)

    # Does the ${resp_label_pl} belong to any <b>professional ${resp_label_pl}s or groups</b>, including cooperative federations?
    df['rm_group_sc'] = np.where(df['ee_group'] == 1, 100, 0)

    # Has the ${resp_label_pl} previously or does it currently <b>own another business</b> or <b>have investment(s) in another business</b>?
    df['rm_otherbusiness_sc'] = np.where(df['ee_otherbusiness'] == 1, 100, 0)

    # Weighted Question/Variable Score
    # Apply the question weights to each scored answer variable
    df['rm_scw'] = df['rm_sc']*0.30
    df['rm_ledger_update_scw'] = df['rm_ledger_update_sc']*0.15
    df['rm_inventory_scw'] = df['rm_inventory_sc']*0.15
    df['rm_wp_scw'] = df['rm_wp_sc']*0.15
    df['rm_reg_scw'] = df['rm_reg_sc']*0.10
    df['rm_training_scw'] = df['rm_training_sc']*0.05
    df['rm_groups_scw'] = df['rm_group_sc']*0.05
    df['rm_otherbusiness_scw'] = df['rm_otherbusiness_sc']*0.05

    rmee_scw_vars = ['rm_scw', 'rm_ledger_update_scw', 'rm_inventory_scw', 'rm_wp_scw',
                    'rm_reg_scw', 'rm_training_scw', 'rm_groups_scw', 'rm_otherbusiness_scw']

    # Operational & Financial Performance ofp_
    # Was the Organization profitable in the last operating year?
    df['ofp_profit_nearestyear_sc'] = np.where(
        (df['ofp_profit_nearestyear'].notna()) & (df['ofp_profit_nearestyear'] > 0), 100, 0)

    # <b><p>Which of the following <b>sources of income</b> does the ${resp_label_pl} have?</p></b>
    df['ofp_income_sc'] = (df['ofp_income_margins_prod']*25
                        + df['ofp_income_margins_inputs']*25
                        + df['ofp_income_salary']*15
                        + df['ofp_income_fees']*15
                        + df['ofp_income_trainingfees']*15
                        + df['ofp_income_custom_fees']*5
                        + df['ofp_income_govt']*0
                        + df['ofp_income_97']*0)

    # Does the ${resp_label_pl} have a <b>bank account</b>?
    df['ofp_bankacct_sc'] = np.where(df['ofp_bankacct'] == 1, 100, 0)

    # Has the Organization <b>obtained a loan or credit</b> within the <b>last 3 years</b>?  This includes loans from financial institutions,
    # inputs on credit or supplier credit, as well as advances to purchase farmer products.
    df['ofp_borrowed_sc'] = np.where(df['ofp_borrowed'] == 1, 100, 0)

    # <p>How does the ${resp_label_pl} <i>currently</i> finance the business?</p>
    df['ofp_current_fin_sc'] = np.where(df['ofp_current_fin'].str.contains('2', regex=False), 100,
                                        np.where(df['ofp_current_fin'].str.contains('3', regex=False), 80,
                                                np.where(df['ofp_current_fin'].str.contains('5', regex=False), 70,
                                                        np.where(df['ofp_current_fin'].str.contains('6', regex=False), 70,
                                                                np.where(df['ofp_current_fin'].str.contains('1', regex=False), 0,
                                                                            np.where(df['ofp_current_fin'].str.contains('4', regex=False), 0,
                                                                                    0))))))

    # Weighted Question/Variable Score
    # Apply the question weights to each score answer variable
    df['ofp_profit_nearestyear_scw'] = df['ofp_profit_nearestyear_sc']*0.20
    df['ofp_current_fin_scw'] = df['ofp_current_fin_sc']*0.20
    df['ofp_bankacct_scw'] = df['ofp_bankacct_sc']*0.20
    df['ofp_borrowed_scw'] = df['ofp_borrowed_sc']*0.20
    df['ofp_income_scw'] = df['ofp_income_sc']*0.20

    ofp_scw_vars = ['ofp_profit_nearestyear_scw', 'ofp_current_fin_scw'
                    'ofp_bankacct_scw', 'ofp_borrowed_scw', 'ofp_income_scw']

    # Category Scores, Benchmarks, Total Score
    # Additional calculations and new variable construction

    # Total Member loyalty ratio and average – Inputs
    df['loyal_ratio_inputs_members'] = (
        (df['msg_loyal_inputs']/df['msg_member'])*100).round(1)
    # Female member loyalty ratio and average – Inputs
    df['loyal_ratio_inputs_women'] = (
        (df['msg_loyal_inputs_women']/df['msg_loyal_inputs'])*100).round(1)
    # Male member loyalty ratio and average – Inputs
    df['loyal_ratio_inputs_men'] = (
        (df['msg_loyal_inputs_men']/df['msg_loyal_inputs'])*100).round(1)

    # Total member loyalty ratio and average – Product
    df['loyal_ratio_product_members'] = (
        (df['msg_loyal_product']/df['msg_member'])*100).round(1)
    # Female member loyalty ratio and average – Product
    df['female_rtloyal_network_product'] = (
        (df['msg_loyal_product_women']/df['msg_loyal_product'])*100).round(1)
    # Male member loyalty ratio and average – Product
    df['male_rtloyal_network_product'] = (
        (df['msg_loyal_product_men']/df['msg_loyal_product'])*100).round(1)

    # Sales trends
    # convert 0 values to NaN
    cols = ['ofp_valuenearestyear',
            'ofp_valuemiddleyear', 'ofp_valuefurthestyear']
    df[cols] = df[cols].replace({0: np.nan})
    # Sales per member (most recent year only)
    df['sales_per_member'] = (
        df['ofp_valuenearestyear']/df['msg_member']).round(1)
    # Calculate average sales values
    df['sales_avg'] = round(
        (df['ofp_valuenearestyear']/df['msg_member'].sum()), 1)
    # Count number of years of available sales data
    df['sales_data_years'] = df[cols].count(axis=1)
    # Calculate percentage change trends across all possible combinations of available data
    df['sales_trend_mid_near'] = ((df['ofp_valuenearestyear'] - df['ofp_valuemiddleyear'])
                                / (df['ofp_valuemiddleyear'])).round(3)
    df['sales_trend_mid_near'] = df['sales_trend_mid_near'].replace(
        [np.inf, -np.inf], np.nan)
    df['sales_trend_far_mid'] = ((df['ofp_valuemiddleyear'] - df['ofp_valuefurthestyear'])
                                / (df['ofp_valuefurthestyear'])).round(3)
    df['sales_trend_far_mid'] = df['sales_trend_far_mid'].replace(
        [np.inf, -np.inf], np.nan)
    df['sales_trend_far_near'] = ((df['ofp_valuenearestyear'] - df['ofp_valuefurthestyear'])
                                / (df['ofp_valuefurthestyear'])).round(3)
    df['sales_trend_far_near'] = df['sales_trend_far_near'].replace(
        [np.inf, -np.inf], np.nan)
    # Calculate the average percentage change trend
    df['sales_trend_avg'] = df[['sales_trend_far_near', 'sales_trend_far_mid',
                                'sales_trend_mid_near']].mean(axis=1).round(3)
    df['total_sales_trend_avg'] = ((df['sales_trend_far_near'].sum() + df['sales_trend_far_mid'].sum() +
                                    df['sales_trend_mid_near'])/3).round(3)
    df['total_sales_trend_desc'] = np.where(df['total_sales_trend_avg'] > 0.0, 'Increase',
                                            np.where(df['total_sales_trend_avg'] == 0.0, 'No Change',
                                                     np.where(df['total_sales_trend_avg'] < 0.0, 'Decrease', 'Insufficient sales financial data'
                                                              )))
    # Add description for available trend
    df['sales_trend_desc'] = np.where(df['ofp_valuenearestyear_refused'] == 99, 'Refused to answer', np.where(df['sales_trend_avg'] > 0.0, 'Increase', np.where(df['sales_trend_avg'] == 0.0, 'No Change',
                                                                                                                                                                np.where(df['sales_trend_avg'] < 0.0,
                                                                                                                                                                        'Decrease',
                                                                                                                                                                        'Insufficient sales financial data'))))
    # profit trends
    # convert 0 values to NaN
    cols = ['ofp_profit_nearestyear',
            'ofp_profitmiddleyear', 'ofp_profitfurthestyear']
    df[cols] = df[cols].replace({0: np.nan})
    # profit per customer member (most recent year only)
    df['profit_per_nfarmer'] = (
        df['ofp_profit_nearestyear']/df['msg_member']).round(1)
    # Calculate average profit values
    df['profit_avg'] = df[cols].mean(axis=1).round(1)
    # Count number of years of available profit data
    df['profit_data_years'] = df[cols].count(axis=1)
    # Calculate percentage change trends across all possible combinations of available data
    df['profit_trend_mid_near'] = ((df['ofp_profit_nearestyear'] - df['ofp_profitmiddleyear'])
                                / (df['ofp_profitmiddleyear'])).round(3)
    df['profit_trend_mid_near'] = df['profit_trend_mid_near'].replace(
        [np.inf, -np.inf], np.nan)
    df['profit_trend_far_mid'] = ((df['ofp_profitmiddleyear'] - df['ofp_profitfurthestyear'])
                                / (df['ofp_profitfurthestyear'])).round(3)
    df['profit_trend_far_mid'] = df['profit_trend_far_mid'].replace(
        [np.inf, -np.inf], np.nan)
    df['profit_trend_far_near'] = ((df['ofp_profit_nearestyear'] - df['ofp_profitfurthestyear'])
                                / (df['ofp_profitfurthestyear'])).round(3)
    df['profit_trend_far_near'] = df['profit_trend_far_near'].replace(
        [np.inf, -np.inf], np.nan)
    # Calculate the average percentage change trend
    df['profit_trend_avg'] = df[['profit_trend_far_near', 'profit_trend_far_mid',
                                'profit_trend_mid_near']].mean(axis=1).round(3)
    # Add description for available trend
    df['profit_trend_desc'] = np.where(df['ofp_profit_nearestyear_refused'] == 99, 'Refused to answer', np.where(df['profit_trend_avg'] > 0.0, 'Increase', np.where(df['profit_trend_avg'] == 0.0, 'No Change',
                                                                                                                                                                    np.where(df['profit_trend_avg'] < 0.0,
                                                                                                                                                                            'Decrease',
                                                                                                                                                                            'Insufficient profit financial data'))))  # loss trends
    # convert 0 values to NaN
    cols = ['ofp_loss_nearestyear',
            'ofp_loss_middleyear', 'ofp_loss_furthestyear']
    df[cols] = df[cols].replace({0: np.nan})

    # loss per customer member (most recent year only)
    df['loss_per_nfarmer'] = (
        df['ofp_loss_nearestyear']/df['msg_member']).round(1)

    # Calculate average loss values
    df['loss_avg'] = df[cols].mean(axis=1).round(1)

    # Count number of years of available loss data
    df['loss_data_years'] = df[cols].count(axis=1)
    # Calculate percentage change trends across all possible combinations of available data
    df['loss_trend_mid_near'] = ((df['ofp_loss_nearestyear'] - df['ofp_loss_middleyear'])
                                / (df['ofp_loss_middleyear'])).round(3)
    df['loss_trend_mid_near'] = df['loss_trend_mid_near'].replace(
        [np.inf, -np.inf], np.nan)
    df['loss_trend_far_mid'] = ((df['ofp_loss_middleyear'] - df['ofp_loss_furthestyear'])
                                / (df['ofp_loss_furthestyear'])).round(3)
    df['loss_trend_far_mid'] = df['loss_trend_far_mid'].replace(
        [np.inf, -np.inf], np.nan)
    df['loss_trend_far_near'] = ((df['ofp_loss_nearestyear'] - df['ofp_loss_furthestyear'])
                                / (df['ofp_loss_furthestyear'])).round(3)
    df['loss_trend_far_near'] = df['loss_trend_far_near'].replace(
        [np.inf, -np.inf], np.nan)
    # Calculate the average percentage change trend
    df['loss_trend_avg'] = df[['loss_trend_far_near', 'loss_trend_far_mid',
                            'loss_trend_mid_near']].mean(axis=1).round(3)
    # Add description for available trend
    df['loss_trend_desc'] = np.where(df['ofp_loss_nearestyear_refused'] == 99, 'Refused to answer',
                                    np.where(df['loss_trend_avg'] > 0.0, 'Increase',
                                            np.where(df['loss_trend_avg'] == 0.0, 'No Change',
                                                    np.where(df['loss_trend_avg'] < 0.0, 'Decrease',
                                                                'Insufficient loss financial data'))))
    # Months of cash reserves
    df['ofp_cash_amnt'] = df['ofp_cash_amnt'].fillna(0)
    df['monthscashreserve'] = round(
        df['ofp_cash_amnt']/df['ofp_monthlyexp'], 1)
    df['monthscashreserve_avg'] = round(df['monthscashreserve'].mean(), 1)
    df['monthscashreserve_topq'] = round(
        df['monthscashreserve'].quantile(0.75), 0)
    # Awaits report template for more calculations

    # The weighted category score (sum of weighted question scores times categ weight

    df['msg_categ_scw'] = ((df['msg_meeting_scw'].fillna(0)
                            + df['msg_elect_leader_scw'].fillna(0)
                            + df['msg_manager_has_scw'].fillna(0)
                            + df['msg_employ_total_scw'].fillna(0)
                            + df['msg_loyal_member_scw'].fillna(0)
                            + df['msg_service_scw'].fillna(0)
                            + df['msg_marketprodx_scw'].fillna(0)
                            + df['msg_input_scw'].fillna(0))*0.20).round(1)
    df['pts_categ_scw'] = ((df['pts_records_scw'].fillna(0)
                            + df['pts_bk_scw'].fillna(0)
                            + df['pts_bk_how_scw'].fillna(0)
                            + df['pts_ledger_scw'].fillna(0)
                            + df['pts_fs_scw'].fillna(0)
                            + df['pts_fs_audit_scw'].fillna(0)
                            + df['pts_tdiapps_scw'].fillna(0))*0.20).round(1)
    df['pp_categ_scw'] = ((df['pp_aplan_scw'].fillna(0)
                        + df['pp_written_scw'].fillna(0)
                        + df['pp_aplanbudget_scw'].fillna(0))*0.20).round(1)
    df['rm_categ_scw'] = ((df['rm_scw'].fillna(0)
                        + df['rm_ledger_update_scw'].fillna(0)
                        + df['rm_inventory_scw'].fillna(0)
                        + df['rm_wp_scw'].fillna(0)
                        + df['rm_reg_scw'].fillna(0)
                        + df['rm_training_scw'].fillna(0)
                        + df['rm_groups_scw'].fillna(0)
                        + df['rm_otherbusiness_scw'].fillna(0))*0.20).round(1)
    df['ofp_categ_scw'] = ((df['ofp_profit_nearestyear_scw'].fillna(0)
                            + df['ofp_current_fin_scw'].fillna(0)
                            + df['ofp_bankacct_scw'].fillna(0)
                            + df['ofp_borrowed_scw'].fillna(0)
                            + df['ofp_income_scw'].fillna(0))*0.20).round(1).fillna(0)

    # ALP Total Scores
    df['total_sc'] = (df['msg_categ_scw']
                    + df['pts_categ_scw']
                    + df['pp_categ_scw']
                    + df['rm_categ_scw']
                    + df['ofp_categ_scw']).round(1)
    # Conditionality: Cannot be in green category if:
    # - Does not do bookkeeping.
    # - Is not officially registered.
    # conditionality check - No bookkeeping
    conditions_bk = [(df['total_sc'] <= 66.0),
                    ((df['total_sc'] > 66.0) & (df['pts_bk'] == 1)),
                    ((df['total_sc'] > 66.0) & (df['pts_bk'] == 0))]
    values_conditions_bk = ['Conditionality check not required',
                            'Passes conditionality check',
                            'FAILS conditionality check - cannot score above 66 because does not do bookkeeping']
    df['cc_bk'] = np.select(conditions_bk, values_conditions_bk)

    # conditionality check - not officially registered
    conditions_reg = [(df['total_sc'] <= 66.0),
                    ((df['total_sc'] > 66.0) & (df['ee_reg'] == 1)),
                    ((df['total_sc'] > 66.0) & (df['ee_reg'] == 0))]
    values_conditions_reg = ['Conditionality check not required',
                            'Passes conditionality check',
                            'FAILS conditionality check - cannot score above 66 because not officially registred']
    df['cc_reg'] = np.select(conditions_reg, values_conditions_reg)

    # Benchmarks - Average & Top Quartiles
    # Generate final score based on adjustments from conditionality check -
    # if failed conditionalities, drop to 66 (yellow category)
    df['total_sc_final'] = np.where((df['cc_bk'] == 'FAILS conditionality check - cannot score above 66 because does not do bookkeeping') |
                                    (df['cc_reg'] == 'FAILS conditionality check - cannot score above 66 because not officially registred'),
                                    66.0,
                                    df['total_sc'])

    total_sc_categ_conditions = [(df['total_sc_final'] <= 33.0),
                                ((df['total_sc_final'] > 33.0) &
                                (df['total_sc_final'] <= 66.0)),
                                (df['total_sc_final'] > 66.0)]

    total_sc_categ_values = ['Red', 'Yellow', 'Green']

    df['total_sc_categ'] = np.select(
        total_sc_categ_conditions, total_sc_categ_values)

    df['total_sc_desc'] = np.where(df['total_sc_categ'] == 'Red',
                                'Very immature, needs basic systems and mgmt practices',
                                np.where(df['total_sc_categ'] == 'Yellow',
                                            'Average application of mgmt systems and practices, can improve operational and financial performance',
                                            np.where(df['total_sc_categ'] == 'Green',
                                                    'Top performer, areas for improvement', 0)))
    max_total_score_final = 100
    df['total_sc_grouping'] = np.where(df['total_sc_final'] <= max_total_score_final/3, 'Basic Performance',
                                    np.where(df['total_sc_final'] >= max_total_score_final*2/3, 'Top Performance', 'Moderate Performance'))

    # calculate average score per category and total score
    df['msg_categ_avg'] = np.round(df['msg_categ_scw'].mean(), 0)
    df['pts_categ_avg'] = np.round(df['pts_categ_scw'].mean(), 0)
    df['pp_categ_avg'] = np.round(df['pp_categ_scw'].mean(), 0)
    df['rm_categ_avg'] = np.round(df['rm_categ_scw'].mean(), 0)
    df['ofp_categ_avg'] = np.round(df['ofp_categ_scw'].mean(), 0)
    df['total_sc_final_avg'] = np.round(df['total_sc_final'].mean(), 0)
    # calculate min score per category and total score
    df['msg_categ_min'] = np.round(df['msg_categ_scw'].min(), 0)
    df['pts_categ_min'] = np.round(df['pts_categ_scw'].min(), 0)
    df['pp_categ_min'] = np.round(df['pp_categ_scw'].min(), 0)
    df['rm_categ_min'] = np.round(df['rm_categ_scw'].min(), 0)
    df['ofp_categ_min'] = np.round(df['ofp_categ_scw'].min(), 0)
    df['total_sc_final_min'] = np.round(df['total_sc_final'].min(), 0)
    # calculate max score per category and total score
    df['msg_categ_max'] = np.round(df['msg_categ_scw'].max(), 0)
    df['pts_categ_max'] = np.round(df['pts_categ_scw'].max(), 0)
    df['pp_categ_max'] = np.round(df['pp_categ_scw'].max(), 0)
    df['rm_categ_max'] = np.round(df['rm_categ_scw'].max(), 0)
    df['ofp_categ_max'] = np.round(df['ofp_categ_scw'].max(), 0)
    df['total_sc_final_max'] = np.round(df['total_sc_final'].max(), 0)
    # calculate median score per category and total score
    df['msg_categ_median'] = np.round(df['msg_categ_scw'].median(), 0)
    df['pts_categ_median'] = np.round(df['pts_categ_scw'].median(), 0)
    df['pp_categ_median'] = np.round(df['pp_categ_scw'].median(), 0)
    df['rm_categ_median'] = np.round(df['rm_categ_scw'].median(), 0)
    df['ofp_categ_median'] = np.round(df['ofp_categ_scw'].median(), 0)
    df['total_sc_final_median'] = np.round(df['total_sc_final'].median(), 0)
    # calculate variance score per category and total score
    if(len(df)>1):
        df['msg_categ_variance'] = np.round(variance(df['msg_categ_scw']), 0)
        df['pts_categ_variance'] = np.round(variance(df['pts_categ_scw']), 0)
        df['pp_categ_variance'] = np.round(variance(df['pp_categ_scw']), 0)
        df['rm_categ_variance'] = np.round(variance(df['rm_categ_scw']), 0)
        df['ofp_categ_variance'] = np.round(variance(df['ofp_categ_scw']), 0)
        df['total_sc_final_variance'] = np.round(variance(df['total_sc_final']), 0)
    else:
        df[['msg_categ_variance', 'pts_categ_variance', 'pp_categ_variance', 'rm_categ_variance', 'ofp_categ_variance', 'total_sc_final_variance']] = 0
    # Calculate top quartile per category
    df['msg_categ_topq'] = df['msg_categ_scw'].quantile(0.75).round(1)
    df['pts_categ_topq'] = df['pts_categ_scw'].quantile(0.75).round(1)
    df['pp_categ_topq'] = df['pp_categ_scw'].quantile(0.75).round(1)
    df['rm_categ_topq'] = df['rm_categ_scw'].quantile(0.75).round(1)
    df['ofp_categ_topq'] = df['ofp_categ_scw'].quantile(0.75).round(1)
    df['total_sc_final_topq'] = df['total_sc_final'].quantile(0.75).round(1)

    # Breaking down all individual scores according to ALP score out of 100
    bd_ALP_score_conditions = [(df['total_sc'] <= 33.0),
                            ((df['total_sc'] > 33.0) &
                                (df['total_sc'] <= 66.0)),
                            (df['total_sc'] > 66.0)]
    bd_ALP_score_values = ['Basic Performance',
                        'Moderate Performance', 'Top Performance']
    df['bd_ALP_score'] = np.select(
        bd_ALP_score_conditions, bd_ALP_score_values)

    # Breaking down all individual scores according to project top scorer
    bd_project_top_score_conditions = [(df['bd_ALP_score'] == 'Basic Performance'),
                                    (df['bd_ALP_score'] ==
                                        'Moderate Performance'),
                                    (df['bd_ALP_score'] == 'Top Performance')]
    bd_project_top_score_values = ['Bottom 1/3',
                                'Middle 1/3', 'Top 1/3']
    df['bd_project_top_score'] = np.select(
        bd_project_top_score_conditions, bd_project_top_score_values)

    # Calculate years that farmer has bank account “bankyear” = (currentyear) – (ofp_bankacct_years) - BLOCKING OUT BC VAR ALREADY EXISTS IN SURVEY
    #df['bankyear'] = date.today().year - df['ofp_bankacct_years']

    # Caculate size of shed/warehouses
    df['land_comm_size_converted'] = np.where(df['land_comm_um'] == 2, df['land_comm_size'],
                                            np.where(df['land_comm_um'] == 3, df['land_comm_size']*10000,
                                                    np.where(df['land_comm_um'] == 4, df['land_comm_size']*4047,
                                                                np.where(df['land_comm_um'] == 1, df['land_comm_size']*0.093,
                                                                        np.where(df['land_comm_um'] == 5, df['land_comm_size']/1.196, df['land_comm_size']
                                                                                )))))

    df['land_ag_size_converted'] = np.where(df['land_ag_um'] == 2, df['land_ag_size'],
                                            np.where(df['land_ag_um'] == 3, df['land_ag_size']*10000,
                                                    np.where(df['land_ag_um'] == 4, df['land_ag_size']*4047,
                                                            np.where(df['land_ag_um'] == 1, df['land_ag_size']*0.093,
                                                                    np.where(df['land_ag_um'] == 5, df['land_ag_size']/1.196, df['land_ag_size']
                                                                                )))))

    final_score_cols = ['msg_categ_scw', 'pts_categ_scw', 'pp_categ_scw', 'rm_categ_scw',
                        'ofp_categ_scw', 'total_sc_final', 'primary_resp_name_final', 'poid_key']


    df['female_rtloyal_network_product'] = (
        (df['msg_loyal_product_women']/df['msg_loyal_product'])*100).round(1)
    # Male member loyalty ratio and average – Product
    df['male_rtloyal_network_product'] = (
        (df['msg_loyal_product_men']/df['msg_loyal_product'])*100).round(1)

    # Sales trends
    # convert 0 values to NaN
    cols = ['ofp_valuenearestyear',
            'ofp_valuemiddleyear', 'ofp_valuefurthestyear']
    df[cols] = df[cols].replace({0: np.nan})
    # Sales per member (most recent year only)
    df['sales_per_member'] = (
        df['ofp_valuenearestyear']/df['msg_member']).round(1)
    if(df['sales_per_member'].isnull().values.all()):
        df[['sales_avg', 'sales_topq']] = 0
    else:
        df['sales_avg'] = df['sales_per_member'].mean()
        df['sales_topq'] = df['sales_per_member'].quantile(
            0.75).round(1)

    # Create file with final scores and summary statistics on final scores.
    final_score_file_name = "ALP_PO_FinalScores.csv"

    df_categ_scores_df = df[final_score_cols]
    df_categ_scores_df.describe()
    df_categ_scores_df.index = np.arange(1, len(df_categ_scores_df)+1)

    load_csv(client, realtime_path, final_score_file_name, df_categ_scores_df)
    load_csv(client, path, final_score_file_name, df_categ_scores_df)

    #
    # ## Apply Label Columns
    #


    # Cleansing data
    # Fill null value of metrics
    col_null = ['msg_employ_manager_total', 'msg_employ_manager_men',
                'msg_employ_manager_women', 'msg_employ_total', 'msg_employ_women', 'msg_employ_men']
    df[col_null] = df[col_null].fillna(0)
    # Changes columns' dtypes from int to float
    columns_int = df.select_dtypes(include=[np.int64, np.int32]).columns
    df[columns_int] = df[columns_int].astype(float)

    # Adding new columns with new formulation - revised with Not applicable
    #df['location_combine'] = df['admin3_final'].astype(str) + ', ' + df['admin4_final']
    df['pts_inventory_yesno'] = np.where(
        df['pts_inventory'].isnull(), 'N/A', 'Yes')
    # df['msg_cust_rank'] = df['msg_cust'].rank(method = 'first')
    # df['msg_cust_band'] = pd.qcut(df['msg_cust_rank'], 3, labels = False) + 1
    df.loc[(df['app_tracing']+df['app_inv']+df['app_fpayment']+df['app_gps']+df['app_onlineaccess']+df['app_acct']+df['app_cpayment']+df['app_ict_s'] +
            df['app_ict_c']) >= 1, 'tdiapps_yesno'] = 1
    df.loc[df['tdiapps_yesno'].isnull(), 'tdiapps_yesno'] = 0
    df.loc[df['tdiapps_yesno'] == 0, 'tdiapps_yesno_label'] = 'No'
    df.loc[df['tdiapps_yesno'] == 1, 'tdiapps_yesno_label'] = 'Yes'

    df.loc[(df['pts_ledger_cash']+df['pts_ledger_sales']+df['pts_ledger_expense']+df['pts_ledger_asset']+df['pts_ledger_inv'] +
            df['pts_ledger_loancredit']+df['pts_ledger_bus_ledger']) >= 1, 'pts_ledger_yesno'] = 1
    df.loc[df['pts_ledger_yesno'].isnull(), 'pts_ledger_yesno'] = 0
    df.loc[df['pts_ledger_yesno'] == 0, 'pts_ledger_yesno_label'] = 'No'
    df.loc[df['pts_ledger_yesno'] == 1, 'pts_ledger_yesno_label'] = 'Yes'

    df['pts_ledger_update_yesno'] = np.where(df['pts_ledger_update'] <= 100, 'Yes',
                                            np.where(df['pts_ledger_update'].isnull(), 'N/A', 'N/A'))


    pre_azure_label_path = "/label"
    sub_azure_label_path = "ALP_LabelsForPython.xlsx"
    ALP_labels = download_file_from_directory(
        client, filesystem_name, pre_azure_label_path, sub_azure_label_path)
    list_old_columns_yes_no = ALP_labels[ALP_labels['choice_labels']
                                        == 'yesno']['name'].unique().tolist()
    list_old_columns_yes_no_99 = ALP_labels[ALP_labels['choice_labels']
                                            == 'yesno99']['name'].unique().tolist()
    list_old_columns = ALP_labels['name'].unique()
    list_old_columns = list_old_columns[~(np.isin(
        list_old_columns, list_old_columns_yes_no + list_old_columns_yes_no_99))]


    for i in list_old_columns_yes_no:
        new_values = i
        if df[i].dtype == 'object':
                continue
        df[new_values + '_label'] = np.where(df[new_values] == 1, 'Yes',
                                                np.where(df[new_values] == 0, 'No',
                                                'N/A'))
    for i in list_old_columns_yes_no_99:
        new_values = i
        if df[i].dtype == 'object':
            continue
        df[new_values + '_label'] = np.where(df[new_values] == 1, 'Yes',
                                        np.where(df[new_values] == 0, 'No',
                                        np.where(df[new_values] == 99, "I don't know",
                                                "N/A")))


    for i in list_old_columns:
        if i in ['gender_other_resp', 'literacy_other_resp', 'position_po_other_resp']:
            continue
        new_values = i

        if df[i].dtype == 'object':
            continue
        df_label = ALP_labels[ALP_labels['name'] == i]

        df = df.merge(df_label[['value', 'Label']], left_on=new_values,
                    right_on='value', how='left')
        if (new_values not in ['land_comm_um', 'land_ag_um', 'ofp_profit_loss_nearestyear', 'ofp_profit_loss_middleyear', 'ofp_profit_loss_furthestyear']):
            df[new_values] = df[new_values].fillna(10000)
        df.drop(columns={'value'}, inplace=True)
        df.rename(columns={'Label': new_values + '_label'}, inplace=True)
        df.index = np.arange(1, len(df)+1)


    #
    # ### Fill null value
    #


    # Fill null value
    object_col_not_change_list = ['pts_ledger_update_label',
                                'pts_bk_how_label', 'pts_inventory_label', 'pts_fs_prep_label']
    non_object_col_not_change_list = ["trucksize_s", "trucksize_m", "trucksize_l",
                                    "shed_num_count", "trucksize_vl", "trucksize_97", "monthscashreserve"]
    col_object = df.select_dtypes(include=[np.object]).columns
    col_other = df.columns.drop(col_object)
    df.loc[:, col_object.drop(object_col_not_change_list)
        ] = df.loc[:, col_object].fillna('N/A')
    df.loc[:, ['ofp_borrowed_issues', 'pts_fs_audit']] = df.loc[:,
                                                                ['ofp_borrowed_issues', 'pts_fs_audit']].fillna(10000)

    df['sales_per_member'] = df['sales_per_member'].fillna('N/A')
    df['businessname_final'] = df['businessname_final'].fillna('Not available')


    #
    # # Exceptional columns
    #


    df['monthscashreserve'] = df['monthscashreserve'].fillna(0)
    # Edit msg
    df['msg_employ_condition'] = np.where(
        (df['msg_employ_men'] == 0) & (df['msg_employ_women'] == 0), 1, 0)
    df['msg_employ_manager_condition'] = np.where(
        (df['msg_employ_manager_men'] == 0) & (df['msg_employ_manager_women'] == 0), 1, 0)
    df['msg_employ_condition_label'] = np.where(
        df['msg_employ_condition'] == 1, 'Not available', '')
    df['msg_employ_manager_condition_label'] = np.where(
        df['msg_employ_manager_condition'] == 1, 'Not available', '')


    #
    # # New columns
    #

    df['loyal_ratio_inputs_total_avg'] = np.round(
        df['loyal_ratio_inputs_members'].mean(), 0)
    df['loyal_ratio_product_total_avg'] = np.round(
        df['loyal_ratio_product_members'].mean(), 0)
    df['loyal_ratio_inputs_total_topq'] = df['loyal_ratio_inputs_members'].quantile(
        0.75).round(1)
    df['loyal_ratio_product_total_topq'] = df['loyal_ratio_product_members'].quantile(
        0.75).round(1)
    if len(df[df['ofp_asset_truck'] == 1]) != 0:
        df['total_trucksize_label'] = 'Yes'
    else:
        df['total_trucksize_label'] = 'No'
    df['trucksize_97_label'] = np.where(
        df['trucksize_97'].isna(), "N/A", df['trucksize_97'].round(0).astype('Int64'))
    df['trucksize_s_label'] = np.where(
        df['trucksize_s'].isna(), "N/A", df['trucksize_s'].round(0).astype('Int64'))
    df['trucksize_m_label'] = np.where(
        df['trucksize_m'].isna(), "N/A", df['trucksize_m'].round(0).astype('Int64'))
    df['trucksize_l_label'] = np.where(
        df['trucksize_l'].isna(), "N/A", df['trucksize_l'].round(0).astype('Int64'))
    df['trucksize_vl_label'] = np.where(
        df['trucksize_vl'].isna(), "N/A", df['trucksize_vl'].round(0).astype('Int64'))

    df['shed_size_total'] = df.apply(lambda x: count_size(x), axis=1)
    df['ofp_asset_shed_size_converted'] = np.where(df['ofp_asset_shed_um'] == 2, df['shed_size_total'],
                                                np.where(df['ofp_asset_shed_um'] == 1, df['shed_size_total'] * 0.093,
                                                            np.where(df['ofp_asset_shed_um'] == 3, df['shed_size_total'] * 10000,
                                                                    np.where(df['ofp_asset_shed_um'] == 4, df['shed_size_total'] * 4047, df['shed_size_total']
                                                                            ))))
    df['total_warehouses'] = np.where(
        df['ofp_asset_shed_size_know'] == 1, df['ofp_asset_shed_size_converted'], "")
    df['total_land_comm'] = np.where(
        df['land_comm_know'] == 1, df['land_comm_size_converted'], "")
    df['total_land_ag'] = np.where(
        df['land_ag_know'] == 1, df['land_ag_size_converted'], "")
    list_asset = ["ofp_asset_office",
                "ofp_asset_shed",
                "ofp_asset_land",
                "ofp_asset_truck",
                "ofp_asset_motorbike",
                "ofp_asset_tractor",
                "ofp_asset_harvester",
                "ofp_asset_scale",
                "ofp_asset97"]
    for i in list_asset:
        df[i] = np.where(df["ofp_asset"] == 0, 0, df[i])
    list_lasset = ["ofp_asset_office",
                "ofp_asset_shed",
                "ofp_asset_land",
                "ofp_asset_truck",
                "ofp_asset_motorbike",
                "ofp_asset_tractor",
                "ofp_asset_harvester",
                "ofp_asset_scale",
                "ofp_asset97"]
    for i in list_lasset:
        df[i] = np.where(df["ofp_lasset"] == 0, 0, df[i])

    if(1 in df['ofp_asset_truck']):
        df['total_truck_label'] = "Yes"
    else:
        df['total_truck_label'] = "No"

    df.index = np.arange(1, len(df) + 1)
    df.index = df.index.set_names(['ID'])



    full_process_filename = ("ALP_PO_2022_FullProcessedDataWithLabel.csv")
    load_csv(client, realtime_path, full_process_filename, df)
    load_csv(client, path, full_process_filename, df)

def load_csv(datalake_service_client, pre_path, suf_path, df):
 
    filesystem_name = "data"

    file_client = datalake_service_client.get_file_client(filesystem_name, pre_path + suf_path)

    raw_df = df.to_csv(index=False)

    file_client.upload_data(data=raw_df, overwrite=True)

    logging.info("Updated data")


def main(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.info('The timer is past due!')

    server_name = "ifcafrica"
    username = "squiroga@ifc.org"
    password = "IFCMAS2021!"
    account_name = 'sproducerorganization'
    account_key = 'kx3ymtKbVE8wTD8GjeymqOUCcSTA9wJQ3YdBdSEFHnbjyb+T5nYhwSfMqJwYPhv/R+zasPiyZk0A+AStpQM7Tw=='

    form_id = "alp_producer_organization_survey"
    survey_name = "ALP Producer Organization Survey"
    project = 'Project 1 (Test) (2022)'
    phase = 'Baseline' 
    root_dir = "{}/{}/{}".format(survey_name, project, phase)

    df = extract(server_name, username, password, form_id, project, phase)

    client = init_datalake_service_client(account_name, account_key)

    current_date_str = datetime.now(pytz.timezone(
        "Asia/Ho_Chi_Minh")).strftime("%Y/%m/%d/")

    raw_pre_path = f"{root_dir}/raw/{current_date_str}"

    processed_pre_path = f"{root_dir}/processed/{current_date_str}"

    load_csv(client, raw_pre_path, "surveycto_data.csv", df)

    transform(root_dir, project, client, df, processed_pre_path)


