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

    file_system_client = datalake_service_client.get_file_system_client(file_system=filesystem_name)

    directory_client = file_system_client.get_directory_client(pre_path)
    
    local_file_path = "/tmp/{}".format(suf_path)

    local_file = open(local_file_path,'wb')

    file_client = directory_client.get_file_client(suf_path)

    download = file_client.download_file()

    downloaded_bytes = download.readall()

    local_file.write(downloaded_bytes)

    local_file.close()

    ALP_labels = pd.read_excel(local_file_path, 
                                  sheet_name = "Selective", dtype = {'variable': float}, 
                                  index_col = None)
    return ALP_labels


def count_size(x):

    shed_size_template = """shed_size_{}"""
    if math.isnan(x['shed_num_count']):
        return 0
    else:
        count = 0
        for i in range(1 ,int(x['shed_num_count'])+1):
            shed_size_template.format(i)
            count += x[shed_size_template.format(i)]
        return count

    
def extract(server_name, username, password, form_id, project_pl, phase):

    scto = pysurveycto.SurveyCTOObject(server_name, username, password)
    data = scto.get_form_data(form_id, format = 'csv')
    df = pd.read_csv(StringIO(data))
    df = df[(df['project'] == project_pl) & (df['phase_pl'] == phase)]

    return df


def init_datalake_service_client(account_name, account_key):

    datalake_service_client = DataLakeServiceClient("https://{}.dfs.core.windows.net".format(account_name), credential=account_key)
    
    return datalake_service_client


def load_xlsx(datalake_service_client, filesystem_name, pre_path, file_name, sheets_name, *datas):
    tmp_path = '/tmp/' + file_name
    writer = pd.ExcelWriter(tmp_path, engine='xlsxwriter')
    io = IO.BytesIO()
    if(len(datas)==1):
        datas[0].to_excel(writer, index = False)
    else:
        i = 0
        for data in datas:
            data.to_excel(writer, sheet_name=sheets_name[i], index = False)
            i+=1
    writer.book.filename = io
    writer.save()
    io_value = io.getvalue()
    
    file_system_client = datalake_service_client.get_file_system_client(file_system=filesystem_name)
    directory_client = file_system_client.get_directory_client(pre_path)
    file_client = directory_client.create_file(file_name)
    file_client.append_data(data=io_value, offset=0)
    file_client.flush_data(len(io_value))


def transform(root_dir, client_pl, client, df, path):
    
    filesystem_name = "data"
    realtime_path = f"{root_dir}/realtime/"
    validate_path = path.replace("processed", "validate")
    
    # # Quality Check
    # ## 1.Export file for project to review, file name: “Review notes for Asili Farms Uganda (2022)”
    # 


    # 
    # ### Tab 1: Open notes
    # 

    # %%

    open_notes_qc = df.loc[:, ["enumerator", "rtid_key", "consent", "primary_resp_name_final", "businessname_final", "open_notes"]]


    # ### Tab 2: Duration
    # 

    # %%

    duration_qc = df.loc[:, ["enumerator", "rtid_key", "consent",
                            "primary_resp_name_final", "businessname_final", "duration"]]
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


    # ### Tab 3: Consent
    # 

    consent_qc = df.loc[:, ["enumerator", "rtid_key", "consent",
                            "primary_resp_name_final", "businessname_final"]]



    # ### Tab 4: Survey and Respondent review
    # 


    survey_res_review_qc = df.loc[:, ["rtid_key", "primary_resp_name_final", "businessname_final", "rate_resp_ee",
                                  "rate_resp_o", "rate_resp_p", "rate_resp_oh", "rate_resp_a", "rate_resp_k", "enum_info",
                                  "enum_respanswer", "enum_rightresp"]]
    
    ### To Excel
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    review_file_name = ("Review notes for {}.xlsx").format(client_pl)
    validate_sheets=['Open notes', 'Duration', 'Consent', 'Survey and Respondent review']
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
        df[i] = df[i].fillna("")
        df[i] = df[i].apply(lambda x: " ".join(x.title().split()))

    # ## Identify and drop
    # 



    ### Non-consent data (consent = 0)
    df = df[df['consent'] != 0]
    ### Rearrange to start index at 1, not 0
    df.index = np.arange(1, len(df)+1)




    ## Copy meta data to seperate dataframe called df_meta_df
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
                #   'location',
                'duration',
                'duration_min',
                'project',
                'enumerator',
                'rtid_key',
                'consent',
                'resp_name_pl',
                'resp_sex_pl',
                'resp_mobilenum_pl',
                'resp_mobilenum_2_pl',
                'resp_position_pl',
                'resp_label_pl',
                'businessname_pl',
                'proprietor_name_pl',
                'proprietor_sex_pl',
                'proprietor_mobilenum_pl',
                'countrycurrency_pl',
                'usd_exchange_rate_pl',
                'country_pl',
                'admin1_pl',
                'admin1_label_pl',
                'admin2_pl',
                'admin2_label_pl',
                'admin3_pl',
                'admin3_label_pl',
                'admin4_pl',
                'admin4_label_pl',
                'assessor_pl',
                'assessor_email_pl',
                'assessor_mobilenum_pl',
                'ifcproject_pl',
                'projectdescription_pl',
                'client_pl',
                'phase_pl',
                'a2f_pl',
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
                'businessyears',
                'businessyears_location',
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
    ### Mapping meta data
    df_result = df.loc[:, df_meta_df]



    # 
    # # Prep step
    # # Fill null values with 0 so that score calculations produce final number
    pts_cols_fillna = ['pts_records', 'pts_records1', 'pts_records2', 'pts_records3',
                       'pts_records4', 'pts_records5', 'pts_ledger',
                       'pts_ledger_cash', 'pts_ledger_sales', 'pts_ledger_expense', 'pts_ledger_asset',
                       'pts_ledger_inv', 'pts_ledger_credit', 'pts_ledger_payable', 'app_acct', 'app_tracing',
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


    ## Convert starttime to format dd/mm/yyyy, new column “startdate”
    df['startdate'] = df["starttime"].apply(lambda x: datetime.strptime(x, "%b %d, %Y %I:%M:%S %p").strftime('%d/%m/%Y'))
    ## Convert endtime to format dd/mm/yyyy, new column name “enddate”
    df['enddate'] = df["endtime"].apply(lambda x: datetime.strptime(x, "%b %d, %Y %I:%M:%S %p").strftime('%d/%m/%Y'))
    ## Create new column “location_combine” by combining “admin3_final, admin4_final”
    df['location_combine'] = df['admin3_final'].astype(
        str) + ', ' + df['admin4_final'].astype(str)
    ## Make sure null values are standardized
    df['pts_inventory_yesno'] = np.where(
        df['pts_inventory'].isnull(), 'N/A', 'Yes')

    df.loc[(df['app_tracing']+df['app_inv']+df['app_fpayment']+df['app_gps']+df['app_onlineaccess']+df['app_acct']+df['app_cpayment']+df['app_ict_s'] +
            df['app_ict_c']) >= 1, 'tdiapps_yesno'] = 1
    df.loc[df['tdiapps_yesno'].isnull(), 'tdiapps_yesno'] = 0
    df.loc[df['tdiapps_yesno'] == 0, 'tdiapps_yesno_label'] = 'No'
    df.loc[df['tdiapps_yesno'] == 1, 'tdiapps_yesno_label'] = 'Yes'
    df.loc[(df['pts_ledger_cash']+df['pts_ledger_sales']+df['pts_ledger_expense']+df['pts_ledger_asset']+df['pts_ledger_inv'] +
            df['pts_ledger_credit']+df['pts_ledger_payable']) >= 1, 'pts_ledger_yesno'] = 1
    df.loc[df['pts_ledger_yesno'].isnull(), 'pts_ledger_yesno'] = 0
    df.loc[df['pts_ledger_yesno'] == 0, 'pts_ledger_yesno_label'] = 'No'
    df.loc[df['pts_ledger_yesno'] == 1, 'pts_ledger_yesno_label'] = 'Yes'
    df['pts_ledger_update_yesno'] = np.where(df['pts_ledger_update'] <= 100, 'Yes',
                                            np.where(df['pts_ledger_update'] == 10000, 'N/A', 'N/A'))



    # 
    # # Score calculation
    # 


    # 
    # ## Customer Service cs_
    # 


    # 
    # ### Answer Scores (unweighted)
    # 



    #### % of farmers Inputs at least 75% thru LF OR % of farmers that purchase at least 75% of inputs from LF OR AVERAGE of both
    df['cs_loyal_customer'] = (df['cs_loyal']/df['cs_customer'])*100
                                                



    #### Does the model Farmer offer credit?
    df['cs_offer_credit_sc'] = np.where(df['cs_offer_credit'] == 1, 100, 0)



    # 
    # ### Weighted Question/Variables Score
    # 



    #### Apply the question weights to each score answer variable
    df['cs_loyal_customer_scw'] = df['cs_loyal_customer'] * 0.60
    df['cs_offer_credit_scw'] = df['cs_offer_credit_sc'] * 0.40
    cs_scw_vars = ['cs_loyal_customer_scw',
                    'cs_offer_credit_scw']



    # 
    # ## Performance Tracking Systems and Technology/Digital Integrations `pts_` and 'tdiapps_'
    # 



    ### Which records does LF have?
    df['pts_records_sc'] = (df['pts_records_reg']*10
                            + df['pts_records_tax']*12.5
                            + df['pts_records_bank']*5
                            + df['pts_records_contract_supplier']*15
                            + df['pts_records_contract_buyer']*2.5
                            + df['pts_records_prod_license']*10
                            + df['pts_records_sale_invoice']*15
                            + df['pts_records_purch_invoice']*15
                            + df['pts_records_bills']*15)
    df['pts_records'] = np.where(df['pts_records_sc'] >= 1, 'Yes', 'No')
    ### Does the LF do bookkeeping?
    df['pts_bk_sc'] = np.where(df['pts_bk'] == 1, 100, 0)
    ### How does LF do bookkeeping?
    df['pts_bk_how_sc'] = np.where(df['pts_bk_how'] == 2, 100,
                                np.where(df['pts_bk_how'] == 3, 50,
                                np.where(df['pts_bk_how'] == 1, 25,
                                            0)))
    ### What business activities tracked does the LF maintain?
    df['pts_ledger_sc'] = (df['pts_ledger_cash']*20
                        + df['pts_ledger_sales']*15
                        + df['pts_ledger_expense']*15
                        + df['pts_ledger_asset']*10
                        + df['pts_ledger_inv']*10
                        + df['pts_ledger_credit']*15
                        + df['pts_ledger_payable']*15)
    ### Which financial statements does the LF produce?
    df['pts_fs_sc'] = (df['pts_fs_cash']*25
                    + df['pts_fs_pl']*50
                    + df['pts_fs_bs']*25)

    df['pts_fs_audit_sc'] = np.where(df['pts_fs_audit'] == 1, 100, 0)
    ### Does the LF use any of the following technology applications?
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
                            



    # ### Weighted Question/Variables Score
    # 



    #### Apply the question weights to each scored answer variable
    df['pts_records_scw'] = df['pts_records_sc']*0.15
    df['pts_bk_scw'] = df['pts_bk_sc']*0.10
    df['pts_bk_how_scw'] = df['pts_bk_how_sc']*0.05
    df['pts_ledger_scw'] = df['pts_ledger_sc']*0.30
    df['pts_fs_scw'] = df['pts_fs_sc']*0.15
    df['pts_fs_audit_scw'] = df['pts_fs_audit_sc']*0.10
    df['pts_tdiapps_scw'] = df['pts_tdiapps_sc']*0.15
    pts_scw_vars = ['pts_records_scw', 'pts_bk_scw', 'pts_bk_how_scw',
                    'pts_ledger_scw', 'pts_fs_audit_scw'
                    'pts_fs_scw', 'pts_tdiapps_scw']
                    



    # ## Planning Pratices pp_
    # 



    ### Does the LF have goals for the business?
    df['pp_goals_sc'] = np.where(df['pp_goals'] == 1, 100, 0)
    df['pp_ap'] = np.where(df['pp_goals'] == 0, 0, df['pp_ap'])


    df['pp_written'] = np.where(df['pp_goals'] == 0, 0, df['pp_written'])
    df['pp_ap_budget'] = np.where(df['pp_ap_budget'] == 0, 0, df['pp_ap_budget'])
    ### Does the LF have a plan for achieving those goals?
    df['pp_ap_sc'] = np.where(df['pp_ap'] == 1, 100, 0)
    ### Is the plan a written plan?
    df['pp_written_sc'] = np.where(df['pp_written'] == 1, 100, 0)
    ### Does the plan include a budget?
    df['pp_ap_budget_sc'] = np.where(df['pp_ap_budget'] == 1, 100, 0)



    # 
    # ### Weighted Question/Variables Score
    # 



    #### Apply the question weights to each scored answer variable
    df['pp_goals_scw'] = df['pp_goals_sc']*0.25
    df['pp_ap_scw'] = df['pp_ap_sc']*0.25
    df['pp_written_scw'] = df['pp_written_sc']*0.25
    df['pp_ap_budget_scw'] = df['pp_ap_budget_sc']*0.25
    pp_scw_vars = ['pp_goals_scw', 'pp_ap_scw',
                'pp_written_scw', 'pp_ap_budget_scw']
                



    # ## Risk Management & External Engagement rmee_
    # 



    ### Which of the following risk mgmt practices does the LF use?
    df['rmee_sc'] = (df['rm_locked']*5
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
                + df['rm_97_insurance']*0
                )
    ### How frequently does the LF update the cash ledger?
    df['rmee_ledger_update_sc'] = np.where(df['pts_ledger_update'] == 1, 100,
                                        np.where(df['pts_ledger_update'] == 2, 75,
                                        np.where(df['pts_ledger_update'] == 3, 25,
                                                np.where(df['pts_ledger_update'] == 4, 0,
                                                np.where(df['pts_ledger_update'] == 5, 0,
                                                        np.where(df['pts_ledger_update'] == 6, 0,
                                                        np.where(df['pts_ledger_update'] == 7, 0,
                                                                    np.where(df['pts_ledger_update'] == 8, 0,
                                                                    np.where(df['pts_ledger_update'] == 9, 0,
                                                                            0)))))))))
    ### How frequently does the LF reconcile inventory?
    df['rmee_inventory_sc'] = np.where(df['pts_inventory'] == 2, 100,
                                    np.where(df['pts_inventory'] == 3, 100,
                                            np.where(df['pts_inventory'] == 4, 50,
                                                    np.where(df['pts_inventory'] == 1, 25,
                                                                np.where(df['pts_inventory'] == 5, 0,
                                                                        np.where(df['pts_inventory'] == 6, 0,
                                                                                np.where(df['pts_inventory'] == 7, 0,
                                                                                        np.where(df['pts_inventory'] == 8, 0,
                                                                                                    np.where(df['pts_inventory'] == 9, 0,
                                                                                                            0)))))))))
    ### Is the LF's business officially registered?
    df['rmee_reg_sc'] = np.where(df['ee_reg'] == 1, 100, 0)
    ### Has the LF participated in training programs related to the LF business in the past 3 years?
    df['rmee_training_sc'] = np.where(df['ee_training'] == 1, 100, 0)
    ### Does the LF belong to any professional organizations or groups related to his/her farming activity and/or to the LF farming business?
    df['rmee_group_sc'] = np.where(df['ee_group'] == 1, 100, 0)
    ### Does the LF own or operate another enterprise or business in additionl to his/her LF model farmer business?
    df['rmee_otherbusiness_sc'] = np.where(df['ee_otherbusiness'] == 1, 100, 0)



    # 
    # ### Weighted Question/Variable Score
    # 



    #### Apply the question weights to each scored answer variable
    df['rmee_scw'] = df['rmee_sc']*0.35
    df['rmee_ledger_update_scw'] = df['rmee_ledger_update_sc']*0.20
    df['rmee_inventory_scw'] = df['rmee_inventory_sc']*0.20
    df['rmee_reg_scw'] = df['rmee_reg_sc']*0.10
    df['rmee_training_scw'] = df['rmee_training_sc']*0.05
    df['rmee_groups_scw'] = df['rmee_group_sc']*0.05
    df['rmee_otherbusiness_scw'] = df['rmee_otherbusiness_sc']*0.05
    rmee_scw_vars = ['rmee_ledger_update_scw', 'rmee_scw', 'rmee_inventory_scw',
                    'rmee_reg_scw', 'rmee_training_scw', 'rmee_groups_scw', 'rmee_otherbusiness_scw']
                    



    # ## Operational & Financial Performance ofp_
    # 



    ### Was the LF profitable in the last operating year?
    df['ofp_profit_nearestyear_sc'] = np.where(
        df['ofp_profit_nearestyear'].notna(), 100, 0)
    ### Current financing sources
    df['ofp_income_sc'] = (df['ofp_income_margins_prod']*5
                        + df['ofp_income_margins_inputs']*55
                        + df['ofp_income_salary']*25
                        + df['ofp_income_custom_fees']*5
                        + df['ofp_income_interest']*10
                        + df['ofp_income_97']*0)
    ### Does the LF keep business accounts separate from other accounts?
    df['ofp_acct_sc'] = np.where(df['ofp_acct'] == 1, 100, 0)
    ### Does the LF have a bank account?
    df['ofp_bankacct_sc'] = np.where(df['ofp_bankacct'] == 1, 100, 0)
    ### Has the LF obtained a loan with the last 3 years from a financial institution?
    df['ofp_borrowed_sc'] = np.where(df['ofp_borrowed'] == 1, 100, 0)
    ### How does the business earn income?
    df['ofp_current_fin_sc'] = np.where(df['ofp_current_fin'].str.contains('2', regex=False), 100,
                                        np.where(df['ofp_current_fin'].str.contains('5', regex=False), 90,
                                        np.where(df['ofp_current_fin'].str.contains('3', regex=False), 80,
                                                np.where(df['ofp_current_fin'].str.contains('6', regex=False), 70,
                                                np.where(df['ofp_current_fin'].str.contains('1', regex=False), 0,
                                                        np.where(df['ofp_current_fin'].str.contains('4', regex=False), 0,
                                                        0))))))
                                                        



    # ### Weighted Question/Variable Score
    # 



    #### Apply the question weights to each score answer variable
    df['ofp_profit_nearestyear_scw'] = df['ofp_profit_nearestyear_sc']*0.15
    df['ofp_acct_scw'] = df['ofp_acct_sc']*0.15
    df['ofp_current_fin_scw'] = df['ofp_current_fin_sc']*0.15
    df['ofp_bankacct_scw'] = df['ofp_bankacct_sc']*0.15
    df['ofp_borrowed_scw'] = df['ofp_borrowed_sc']*0.25
    df['ofp_income_scw'] = df['ofp_income_sc']*0.15
    ofp_scw_vars = ['ofp_acct_scw', 'ofp_profit_nearestyear_scw', 'ofp_current_fin_scw'
                    'ofp_bankacct_scw', 'ofp_borrowed_scw', 'ofp_income_scw']



    # 
    # # Category Scores, Benchmarks, Total Score
    # 


    # 
    # ## Additional calculations and new variable construction
    # 


    # 
    # ### Total Farmer loyalty ratio and average – Inputs
    # 



    df['loyal_customer_ratio'] = (
        df['cs_loyal']/df['cs_customer']
        ).round(1)



    # 
    # ### Female farmer loyalty ratio and average – Inputs
    # 



    df['loyal_female_customer_ratio'] = (
        (df['cs_loyal_women']/df['cs_loyal'])*100).round(1)
        



    # ### Male farmer loyalty ratio and average – Inputs
    # 



    df['loyal_male_customer_ratio'] = (
        ((df['cs_loyal'] - df['cs_loyal_women'])/df['cs_loyal'])*100
        ).round(1)

        



    # ### Sales trends
    # 



    #### convert 0 values to NaN
    cols = ['ofp_valuenearestyear',
            'ofp_valuemiddleyear', 'ofp_valuefurthestyear']
    df[cols] = df[cols].replace({0: np.nan})
    
    #### Sales per network farmer (most recent year only)
    df['sale_per_customer'] = (df['ofp_valuenearestyear']/df['cs_customer']).round(1)
    if(df['sale_per_customer'].isnull().values.all()):
        df[['sale_per_customer_avg', 'sale_per_customer_topq']] = 0
    else:
        df['sale_per_customer_avg'] = round(df['sale_per_customer'].mean(), 1)
        df['sale_per_customer_topq'] = round(df['sale_per_customer'].quantile(0.75), 0)

    #### Calculate average sales values
    df['sales_avg'] = round(
        (df['ofp_valuenearestyear']/df['cs_customer'].sum()), 1)



    #### Count number of years of available sales data
    df['sales_data_years'] = df[cols].count(axis=1)


    #### Calculate percentage change trends across all possible combinations of available data
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
    #### Calculate the average percentage change trend
    df['profit_trend_avg'] = df[['profit_trend_far_near', 'profit_trend_far_mid',
                                 'profit_trend_mid_near']].mean(axis=1).round(3)


    df['total_profit_trend_avg'] = ((df['profit_trend_far_near'].sum() + df['profit_trend_far_mid'].sum() +
                                    df['profit_trend_mid_near'])/3).round(3)
    df['total_profit_trend_desc'] = np.where(df['total_profit_trend_avg'] > 0.0, 'Increase',
                                            np.where(df['total_profit_trend_avg'] == 0.0, 'No Change',
                                                    np.where(df['total_profit_trend_avg'] < 0.0, 'Decrease', 'Insufficient sales financial data'
                                                            )))
    #### Add description for available trend
    df['sales_trend_desc'] = np.where(df['ofp_valuenearestyear_refused'] == 99, 'Refused to answer', 
                            np.where(df['sales_trend_avg'] > 0.0, 'Increase', 
                            np.where(df['sales_trend_avg'] == 0.0, 'No Change',
                            np.where(df['sales_trend_avg'] < 0.0, 'Decrease',
                            'Insufficient financial data'))))
                            



    ### profit trends
    #### convert 0 values to NaN
    cols = ['ofp_profit_nearestyear',
            'ofp_profitmiddleyear', 'ofp_profitfurthestyear']
    df[cols] = df[cols].replace({0: np.nan})
    #### profit per customer network farmer (most recent year only)
    df['profit_per_nfarmer'] = (
        df['ofp_profit_nearestyear']/df['cs_customer']).round(1)
    #### Calculate average profit values
    df['profit_avg'] = df[cols].mean(axis=1).round(1)
    #### Count number of years of available profit data
    df['profit_data_years'] = df[cols].count(axis=1)
    #### Calculate percentage change trends across all possible combinations of available data
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
    #### Calculate the average percentage change trend
    df['profit_trend_avg'] = df[['profit_trend_far_near', 'profit_trend_far_mid',
                                'profit_trend_mid_near']].mean(axis=1).round(3)
    #### Add description for available trend
    df['profit_trend_desc'] = np.where(df['ofp_profit_nearestyear_refused'] == 99, 'Refused to answer',
                            np.where(df['profit_trend_avg'] > 0.0, 'Increase', 
                            np.where(df['profit_trend_avg'] == 0.0, 'No Change',
                            np.where(df['profit_trend_avg'] < 0.0, 'Decrease',
                                        'Insufficient financial data'))))  # loss trends
                                        



    #### convert 0 values to NaN
    cols = ['ofp_loss_nearestyear',
            'ofp_loss_middleyear', 'ofp_loss_furthestyear']
    df[cols] = df[cols].replace({0: np.nan})
    #### loss per customer network farmer (most recent year only)
    df['loss_per_nfarmer'] = (
        df['ofp_loss_nearestyear']/df['cs_customer']).round(1)
    #### Calculate average loss values
    df['loss_avg'] = df[cols].mean(axis=1).round(1)
    #### Count number of years of available losfillna(10000)s data
    df['loss_data_years'] = df[cols].count(axis=1)
    #### Calculate percentage change trends across all possible combinations of available data
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
    #### Calculate the average percentage change trend
    df['loss_trend_avg'] = df[['loss_trend_far_near', 'loss_trend_far_mid',
                            'loss_trend_mid_near']].mean(axis=1).round(3)
    #### Add description for available trend
    df['loss_trend_desc'] = np.where(df['ofp_loss_nearestyear_refused'] == 99, 'Refused to answer',
                            np.where(df['loss_trend_avg'] > 0.0, 'Increase',
                            np.where(df['loss_trend_avg'] == 0.0, 'No Change',
                            np.where(df['loss_trend_avg'] < 0.0, 'Decrease',
                                'Insufficient financial data'))))
                                                    



    ### Months of cash reserves
    df['ofp_cash_amnt'] = df['ofp_cash_amnt'].fillna(0)
    df['monthscashreserve'] = round(
        df['ofp_cash_amnt']/df['ofp_monthlyexp'], 1)
    df['monthscashreserve_avg'] = round(df['monthscashreserve'].mean(), 1)
    df['monthscashreserve_topq'] = round(
        df['monthscashreserve'].quantile(0.75), 0)
        



    # ### Awaits report template for more calculations
    # 



    #### The weighted category score (sum of weighted question scores times categ weight
    df['cs_categ_scw'] = ((df['cs_loyal_customer_scw'].fillna(0)
                        + df['cs_offer_credit_scw'].fillna(0)
                            )*0.20).round(1)

    df['pfstdi _categ_scw'] = ((df['pts_records_scw'].fillna(0)
                            + df['pts_bk_scw'].fillna(0)
                            + df['pts_bk_how_scw'].fillna(0)
                            + df['pts_ledger_scw'].fillna(0)
                            + df['pts_fs_audit_scw'].fillna(0)
                            + df['pts_fs_scw'].fillna(0)
                            + df['pts_tdiapps_scw'].fillna(0))*0.20).round(1)
    df['pp_categ_scw'] = ((df['pp_goals_scw'].fillna(0)
                        + df['pp_ap_scw'].fillna(0)
                        + df['pp_written_scw'].fillna(0)
                        + df['pp_ap_budget_scw'].fillna(0))*0.20).round(1)
    df['rmee_categ_scw'] = ((df['rmee_ledger_update_scw'].fillna(0)
                        + df['rmee_scw'].fillna(0)
                        + df['rmee_reg_scw'].fillna(0)
                        + df['rmee_training_scw'].fillna(0)
                        + df['rmee_groups_scw'].fillna(0)
                        + df['rmee_otherbusiness_scw'].fillna(0)
                        + df['rmee_inventory_scw'].fillna(0))*0.20).round(1)
    df['ofp_categ_scw'] = ((df['ofp_acct_scw'].fillna(0)
                            + df['ofp_borrowed_scw'].fillna(0)
                            + df['ofp_profit_nearestyear_scw'].fillna(0)
                            + df['ofp_current_fin_scw'].fillna(0)
                            + df['ofp_bankacct_scw'].fillna(0)
                            + df['ofp_income_scw'].fillna(0))*0.20).round(1)
                            



    #### ALP Total Scores
    df['total_sc'] = (df['cs_categ_scw']
                    + df['pfstdi _categ_scw']
                    + df['pp_categ_scw']
                    + df['rmee_categ_scw']
                    + df['ofp_categ_scw']).round(1)
                    



    ### calculate average score per category and total score
    df['cs_categ_avg'] = np.round(df['cs_categ_scw'].mean(), 0)
    df['pts_categ_avg'] = np.round(df['pfstdi _categ_scw'].mean(), 0)
    df['pp_categ_avg'] = np.round(df['pp_categ_scw'].mean(), 0)
    df['rm_categ_avg'] = np.round(df['rmee_categ_scw'].mean(), 0)
    df['ofp_categ_avg'] = np.round(df['ofp_categ_scw'].mean(), 0)
    df['total_sc_avg'] = np.round(df['total_sc'].mean(), 0)
    ### calculate min score per category and total score
    df['cs_categ_min'] = np.round(df['cs_categ_scw'].min(), 0)
    df['pts_categ_min'] = np.round(df['pfstdi _categ_scw'].min(), 0)
    df['pp_categ_min'] = np.round(df['pp_categ_scw'].min(), 0)
    df['rm_categ_min'] = np.round(df['rmee_categ_scw'].min(), 0)
    df['ofp_categ_min'] = np.round(df['ofp_categ_scw'].min(), 0)
    df['total_sc_min'] = np.round(df['total_sc'].min(), 0)
    ### calculate max score per category and total score
    df['cs_categ_max'] = np.round(df['cs_categ_scw'].max(), 0)
    df['pts_categ_max'] = np.round(df['pfstdi _categ_scw'].max(), 0)
    df['pp_categ_max'] = np.round(df['pp_categ_scw'].max(), 0)
    df['rm_categ_max'] = np.round(df['rmee_categ_scw'].max(), 0)
    df['ofp_categ_max'] = np.round(df['ofp_categ_scw'].max(), 0)
    df['total_sc_max'] = np.round(df['total_sc'].max(), 0)
    ### calculate median score per category and total score
    df['cs_categ_median'] = np.round(df['cs_categ_scw'].median(), 0)
    df['pts_categ_median'] = np.round(df['pfstdi _categ_scw'].median(), 0)
    df['pp_categ_median'] = np.round(df['pp_categ_scw'].median(), 0)
    df['rm_categ_median'] = np.round(df['rmee_categ_scw'].median(), 0)
    df['ofp_categ_median'] = np.round(df['ofp_categ_scw'].median(), 0)
    df['total_sc_median'] = np.round(df['total_sc'].median(), 0)
    ### calculate variance score per category and total score
    if(len(df)>1):
        df['cs_categ_variance'] = np.round(variance(df['cs_categ_scw']), 0)
        df['pts_categ_variance'] = np.round(variance(df['pfstdi _categ_scw']), 0)
        df['pp_categ_variance'] = np.round(variance(df['pp_categ_scw']), 0)
        df['rm_categ_variance'] = np.round(variance(df['rmee_categ_scw']), 0)
        df['ofp_categ_variance'] = np.round(variance(df['ofp_categ_scw']), 0)
        df['total_sc_variance'] = np.round(variance(df['total_sc']), 0)
    else:
        df[['cs_categ_variance', 'pts_categ_variance', 'pp_categ_variance', 'rm_categ_variance', 'ofp_categ_variance', 'total_sc_variance']] = 0
    ### Breaking down all individual scores according to ALP score out of 100
    bd_ALP_scorce_conditions = [(df['total_sc'] <= 33.0),
                                ((df['total_sc'] > 33.0) &
                                (df['total_sc'] <= 66.0)),
                                (df['total_sc'] > 66.0)]
    bd_ALP_scorce_values = ['Basic Performance',
                            'Moderate Performance', 'Top Performance']
    df['bd_ALP_scorce'] = np.select(
        bd_ALP_scorce_conditions, bd_ALP_scorce_values)
    ### Breaking down all individual scores according to project top scorer
    bd_project_top_scorce_conditions = [(df['bd_ALP_scorce'] == 'Basic Performance'),
                                        (df['bd_ALP_scorce'] ==
                                        'Moderate Performance'),
                                        (df['bd_ALP_scorce'] == 'Top Performance')]
    bd_project_top_scorce_values = ['Bottom 1/3',
                                    'Middle 1/3', 'Top 1/3']
    df['bd_project_top_scorce'] = np.select(
        bd_project_top_scorce_conditions, bd_project_top_scorce_values)
    ### Calculate years that farmer has bank account “bankyear” = (currentyear) – (ofp_bankacct_years)
    df['bankyear'] = date.today().year - df['ofp_bankacct_years']
    ## Conditionality: Cannot be in green category if:
    ### - Does not do bookkeeping.
    ### - Is not officially registered.
    #### conditionality check - No bookkeeping
    conditions_bk = [(df['total_sc'] <= 66.0),
                    ((df['total_sc'] > 66.0) & (df['pts_bk'] == 1)),
                    ((df['total_sc'] > 66.0) & (df['pts_bk'] == 0))]
    values_conditions_bk = ['Conditionality check not required',
                            'Passes conditionality check',
                            'FAILS conditionality check - cannot score above 66 because does not do bookkeeping']
    df['cc_bk'] = np.select(conditions_bk, values_conditions_bk)
    ### conditionality check - not officially registered
    conditions_reg = [(df['total_sc'] <= 66.0),
                    ((df['total_sc'] > 66.0) & (df['ee_reg'] == 1)),
                    ((df['total_sc'] > 66.0) & (df['ee_reg'] == 0))]
    values_conditions_reg = ['Conditionality check not required',
                            'Passes conditionality check',
                            'FAILS conditionality check - cannot score above 66 because not officially registred']
    df['cc_reg'] = np.select(conditions_reg, values_conditions_reg)



    # # Benchmarks - Average & Top Quartiles
    # 



    ## Generate final score based on adjustments from conditionality check -
    ### if failed conditionalities, drop to 66 (yellow category)
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



    # 
    # # Caculate size of shed/warehouses
    # 



    df['land_comm_size_converted'] = np.where(df['land_comm_um'] == 2, df['land_comm_size'],
                                            np.where(df['land_comm_um'] == 3, df['land_comm_size']*10000,
                                                    np.where(df['land_comm_um'] == 4, df['land_comm_size']*4046.86,
                                                                np.where(df['land_comm_um'] == 1, df['land_comm_size']*0.0929,
                                                                        np.where(df['land_comm_um'] == 5, df['land_comm_size']*0.8361,
                                                                                np.where(df['land_comm_um'] == 97, df['land_comm_size'].astype(str) + "land_comm_um97", df['land_comm_size']
                                                                                        ))))))
    df['land_ag_size_converted'] = np.where(df['land_ag_um'] == 2, df['land_ag_size'],
                                            np.where(df['land_ag_um'] == 3, df['land_ag_size']*10000,
                                            np.where(df['land_ag_um'] == 4, df['land_ag_size']*4046.86,
                                                    np.where(df['land_ag_um'] == 1, df['land_ag_size']*0.0929,
                                                    np.where(df['land_ag_um'] == 5, df['land_ag_size']*0.8361,
                                                            np.where(df['land_ag_um'] == 97, df['land_ag_size'].astype(str) + "land_ag_um97", df['land_ag_size']
                                                                    ))))))

    ### Calculate top quartile per category

    df['cs_categ_topq'] = df['cs_categ_scw'].quantile(0.75).round(1)
    df['pts_categ_topq'] = df['pfstdi _categ_scw'].quantile(0.75).round(1)
    df['pp_categ_topq'] = df['pp_categ_scw'].quantile(0.75).round(1)
    df['rm_categ_topq'] = df['rmee_categ_scw'].quantile(0.75).round(1)
    df['ofp_categ_topq'] = df['ofp_categ_scw'].quantile(0.75).round(1)
    df['total_sc_final_topq'] = df['total_sc_final'].quantile(0.75).round(1)
    final_score_cols = ['cs_categ_scw', 'pfstdi _categ_scw', 'pp_categ_scw', 'rmee_categ_scw',
                        'ofp_categ_scw', 'total_sc_final', 'primary_resp_name_final', 'rtid_key']
    #### Create dataframe of scores and export
    final_score_file_name = "ALP_Retail_FinalScores.csv"
    df_categ_scores_df = df[final_score_cols]
    df_categ_scores_df.describe()
    df_categ_scores_df.index = np.arange(1, len(df_categ_scores_df)+1)
    
    load_csv(client, realtime_path, final_score_file_name, df_categ_scores_df)
    load_csv(client, path, final_score_file_name, df_categ_scores_df)
   
    ## Apply Label Columns
    ### Cleansing data
    #### Fill null value of metrics
    col_null = ['cs_manager', 'cs_manager_men', 'cs_manager_women',
                'cs_empl_unpaid_men', 'cs_empl_unpaid_women']
    df[col_null] = df[col_null].fillna(0)
    ### Changes columns' dtypes from int to float
    columns_int = df.select_dtypes(include=[np.int64, np.int32]).columns
    df[columns_int] = df[columns_int].astype(float)
    ### Adding new columns with new formulation - revised with Not applicable
    df['pts_inventory_yesno'] = np.where(
        df['pts_inventory'].isnull(), 'N/A', 'Yes')
    ### Adding new columns using "ALP_LabelsForPython"
    pre_azure_label_path = "/label"
    sub_azure_label_path = "ALP_LabelsForPython.xlsx"
    ALP_labels = download_file_from_directory(client, filesystem_name, pre_azure_label_path, sub_azure_label_path)
    list_old_columns_yes_no = ALP_labels[ALP_labels['choice_list']
                                     == 'yesno']['name'].unique().tolist()
    list_old_columns_yes_no_99 = ALP_labels[ALP_labels['choice_list']
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
                                            'N/A')).copy()
    for i in list_old_columns_yes_no_99:
        new_values = i
        if df[i].dtype == 'object':
            continue
        if(new_values in ['ofp_acct','ofp_bankacct','ofp_borrowed']):
            df[new_values + '_label'] = np.where(df[new_values] == 1, 'Yes',
                                                np.where(df[new_values] == 0, 'No',
                                                np.where(df[new_values] == 99, "Do not know",
                                                        "N/A"))).copy()
        else:
            df[new_values + '_label'] = np.where(df[new_values] == 1, 'Yes',
                                                np.where(df[new_values] == 0, 'No',
                                                np.where(df[new_values] == 99, "I don't know",
                                                        "N/A"))).copy()

    ### looping adding new columns
    for i in list_old_columns:
        new_values = i
        if df[i].dtype == 'object':
            continue
        df_label = ALP_labels[ALP_labels['name'] == i]
        df = df.merge(df_label[['value', 'label']], left_on=new_values,
                      right_on='value', how='left')

        if(new_values not in ['land_comm_um', 'land_ag_um', 'ofp_profit_loss_nearestyear', 'ofp_profit_loss_middleyear', 'ofp_profit_loss_furthestyear','ofp_asset_shed','pts_bk_how']):
            df[new_values] = df[new_values].fillna(10000)
        df.drop(columns={'value'}, inplace=True)
        df.rename(columns={'label': new_values + '_label'}, inplace=True)
        df.index = np.arange(1, len(df)+1)


    # 
    # ### Fill null value
    # 

    object_col_not_change_list = ['pts_ledger_update_label', 'pts_inventory_label','pts_bk_how_label']
    non_object_col_not_change_list = ["ofp_borrowed_issues", "pts_fs_audit","cs_credit_1","cs_credit_2","cs_credit_97"]
    col_object = df.select_dtypes(include=[np.object]).columns
    col_other = df.columns.drop(col_object)




    df.loc[:, col_object.drop(object_col_not_change_list)] = df.loc[:, col_object.drop(
        object_col_not_change_list)].fillna('N/A')




    # df.loc[:, non_object_col_not_change_list] = df.loc[:,].fillna(10000)
    list_numeric = ['ofp_borrowed_issues',
                    'pts_fs_audit', 'rm_locked', 'rm_security', 'rm_safe', 'rm_reserves', 'rm_succession',
                    'rm_cash', 'rm_inventory', 'rm_budget', 'rm_writtencash', 'rm_writteninvent', 'rm_v_insurance',
                    'rm_h_insurance', 'rm_p_insurance', 'rm_l_insurance', 'rm_storage', 'rm_97_insurance']
    df.loc[:, list_numeric] = df.loc[:, list_numeric].fillna(10000)
    df['sale_per_customer'] = df['sale_per_customer'].fillna('N/A')
    df['businessname_final'] = df['businessname_final'].fillna('Not available')



    # 
    # # Exceptional columns
    # 



    df['monthscashreserve'] = df['monthscashreserve'].fillna(0)
    df['monthscashreserve_avg'] = df['monthscashreserve_avg'].fillna(0)
    df['monthscashreserve_topq'] = df['monthscashreserve_topq'].fillna(0)
    ### Edit cs
    df['cs_empl_paid_men'] = df['cs_empl_men'] - df['cs_empl_unpaid_men']
    df['cs_empl_paid_women'] = df['cs_empl_women'] - \
        df['cs_empl_unpaid_women']
    df['cs_empl_paid_condition'] = np.where(
        (df['cs_empl_paid_men'] == 0) & (df['cs_empl_paid_women'] == 0), 1, 0)
    df['cs_empl_condition'] = np.where(
        (df['cs_empl_men'] == 0) & (df['cs_empl_women'] == 0), 1, 0)
    df['cs_empl_unpaid_condition'] = np.where(
        (df['cs_empl_unpaid_men'] == 0) & (df['cs_empl_unpaid_women'] == 0), 1, 0)
    df['cs_manager_condition'] = np.where(
        (df['cs_manager_men'] == 0) & (df['cs_manager_women'] == 0), 1, 0)
    df['cs_empl_paid_condition_label'] = np.where(
        df['cs_empl_paid_condition'] == 1, 'Not available', '')
    df['cs_empl_condition_label'] = np.where(
        df['cs_empl_condition'] == 1, 'Not available', '')
    df['cs_empl_unpaid_condition_label'] = np.where(
        df['cs_empl_unpaid_condition'] == 1, 'Not available', '')
    df['cs_manager_condition_label'] = np.where(
        df['cs_empl_condition_label'] == 1, 'Not available', '')
                      

        
    # New columns
    df['loyal_customer_avg'] = df['loyal_customer_ratio'].mean()
    df['loyal_customer_topq'] = df['loyal_customer_ratio'].quantile(0.75).round(1)
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
    if len(df[df['ofp_asset_truck'] == 1]) != 0:
        df['total_trucksize_label'] = 'Yes'
    else:
        df['total_trucksize_label'] = 'No'

    df['pp_goal_label'] = np.where(df['pp_goals'] == 1, 'Yes',
                                np.where(df['pp_goals'] == 0, 'No',
                                "N/A"))
    df['shed_size_total'] = df.apply(lambda x: count_size(x), axis=1)
    df['ofp_asset_shed_size_converted'] = np.where(df['ofp_asset_shed_um'] == 2, df['shed_size_total'],
                                                np.where(df['ofp_asset_shed_um'] == 3, df['shed_size_total']*10000,
                                                            np.where(df['ofp_asset_shed_um'] == 4, df['shed_size_total']*4046.86,
                                                                    np.where(df['ofp_asset_shed_um'] == 1, df['shed_size_total']*0.0929,
                                                                            np.where(df['ofp_asset_shed_um'] == 5, df['shed_size_total']*0.8361,
                                                                                    np.where(df['ofp_asset_shed_um'] == 97, df['shed_size_total'].astype(str) + "ofp_asset_shed_um97", df['shed_size_total']
                                                                                                ))))))

    df['total_warehouses'] = np.where(
        df['ofp_asset_shed_size_know'] == 1, df['ofp_asset_shed_size_converted'], "")
    df['total_land_comm'] = np.where(
        df['land_comm_know'] == 1, df['land_comm_size_converted'], "")
    df['total_land_ag'] = np.where(
        df['land_ag_know'] == 1, df['land_ag_size_converted'], "")
    df.index = np.arange(1, len(df) + 1)
    df.index = df.index.set_names(['ID'])

    full_process_filename = "ALP_Retail_FullProcessedDataWithLabels.csv"
    load_csv(client, realtime_path, full_process_filename, df)
    load_csv(client, path, full_process_filename, df)

def load_csv(datalake_service_client, pre_path, suf_path, df):
 
    filesystem_name = "data"

    file_client = datalake_service_client.get_file_client(filesystem_name, pre_path + suf_path)

    raw_df = df.to_csv(index=False)

    file_client.upload_data(data=raw_df,overwrite=True, length=len(raw_df))

    file_client.flush_data(len(raw_df))

    logging.info("Updated data")

def main(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.info('The timer is past due!')

    server_name = "ifcafrica"
    username = "squiroga@ifc.org"
    password = "IFCMAS2021!"
    account_name='sretailer'
    account_key='S35o6XpraTjY8oilLI1m1rORVv3C8YmIO92JfD0dau0OcJveIVVCRSSho+T1l5TmX4rAgGAebCO4+AStd8wZzw=='

    form_id = "alp_retailer_survey_for_testing"
    project = 'Project 1 (Test) (2022)'
    phase = 'Midline' 
    root_dir = "/{}/{}".format(project, phase)

    df = extract(server_name, username, password, form_id, project, phase)

    client = init_datalake_service_client(account_name, account_key)

    current_date_str = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y/%m/%d/") 

    raw_pre_path = f"{root_dir}/raw/{current_date_str}"  

    processed_pre_path = f"{root_dir}/processed/{current_date_str}"

    load_csv(client, raw_pre_path, "surveycto_data.csv", df)

    transform(root_dir, project, client, df, processed_pre_path)
