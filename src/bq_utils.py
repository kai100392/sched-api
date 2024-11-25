from google.cloud import bigquery
import pandas as pd
import numpy as np

def process_patient_group(group_, call_in_date):
    # from GenAI
    LIST_CONDITION=[]
    LIST_IMAGING=['MR PROSTATE WITHOUT AND WITH IV CONTRAST', 'MR PROSTATE WITHOUT IV CONTRAST', 'CT ABDOMEN PELVIS WITH IV CONTRAST', 'SCAN BLADDER', 'UROLOGY IMAGE EXAM', 'PET CT SKULL TO THIGH PSMA', 'URO MR FUSION', 'URO Prostate US', 'PR US TRANSRECTAL', 'URO CYSTO W/ PROSTATE US', 'PR MRI PELVIS WO/W CNTRST', 'HC MRI PELVIS WO/W CNTRST', 'PR CT ABD&PELVIS W CNTRST', 'URO Transperineal MR Fusion', 'Cysto w/ Prostate US', 'PET CT Prostate PSMA', 'US, Transrectal', 'UROLOGY Image Exam', 'PETCT with Diagnostic CT of the neck-chest-abd-pelvis w/IV contrast; PSMA', 'MR PROSTATE BIOPSY WITH IMAGING GUIDANCE', 'URO KUB', 'PR CT ABDOMEN WO/W CNTRST', 'US PROSTATE FIDUCIAL MARKER PLACEMENT WITH HYDROGEL SPACER', 'US Renal And Urinary Bladder', 'PR CT ABDOMEN W CNTRST', 'CT chest abdomen pelvis with contrast', 'PET CT FLUCICLOVINE PROSTATE', 'MRI Brain with and without Contrast', 'CT Abdomen Pelvis', 'PR CTA HRT CORONARY W CNTRST', 'US Renal Complete', 'CT Abdomen/Pelvis']
    LIST_BIOPSY=['PR BIOPSY PROSTATE NEEDLE/PUNCH', 'URO BIOPSY - PROSTATE', 'CYTOLOGY FINE NEEDLE ASPIRATION (INCLUDES CORE BIOPSIES', 'PR US TRANSRECTAL', 'PR US GUIDE PLC NDL', 'Bx, Prostate', 'Bx, Prostate Fusion', 'PR BX PROSTATE NDL TRANSPRNEAL', 'URO PROSTATE TRANSPERINEAL BIOPSY']
    LIST_PSA=['PROSTATE-SPECIFIC AG (PSA) DIAGNOSTIC, S', 'PROSTATE-SPECIFIC AG (PSA) SCRN, S', 'TESTOSTERONE, TOTAL BY MASS SPECROMETRY, S', 'PROSTATE-SPECIFIC AG (PSA), TOT AND FR, S', 'PROSTATE-SPECIFIC AG (PSA)', 'PSA, ULTRASENSITIVE, S', 'PROSTATE-SPECIFIC AG (PSA), TOT AND FR, S', 'PSA Total Diagnostic', 'PSA', 'PSA Total', 'PROSTATIC SPECIFIC ANTIGEN (PSA)', 'Prostate Specific Antigen (Total), Blood', 'PSA, Diagnostic', 'PSA, Screen', 'PSA Total Diagnostic', 'PROSTATIC SPECIFIC ANTIGEN-PSA', 'Prostatic Specific Antigen Diagnostic', 'PSA diagnostic Blood, Venous', 'PSA (Prostate Specific Antigen) - DIAGNOSTIC', 'PSA diagnostic (monitoring)', 'PSA Screening', 'PSA,Total (Screen)', 'PSA,Total (Diagnostic)', 'PROSTATE SPECIFIC ANTIGEN (PSA), TOTAL DIAGNOSTIC', 'PSA screen', 'PSA Monitor', 'PSA diagnostic', 'Prostatic Specific Antigen Screen', 'PSA total, diagnostic', 'PSA, Total and Free', 'PSA Total', 'Prostate Specific Ag (PSA), Total', 'PSA, Diagnostic/Monitoring', 'PSA TOTAL', 'PSA, Ultrasensitive', 'PSA,Total', 'Prostate Specific Antigen', 'Prostatic Specific Antigen', 'PSA Antigen (PSA Screen)', 'PSA/PROSTSPECAG DIAG', 'PSA,Total (Diagnostic)', 'Prostate Specific Ag', 'PSA SCREEN', 'PROSTATIC SPECIFIC ANTIGEN', 'PSA, Reflex PSA Free', 'PSA-DIAGNOSTIC, BL (ROCHE)', 'PROSTATIC SPC AG, SYMPTOMATIC', 'PSA (Screen), Blood - See Instructions', 'PROSTATE SPECIFIC ANTIGEN, DIAGNOSTIC', 'PSA, TOTAL', 'PROSTATE SPECIFIC ANTIGEN, DIAGNOSTIC', 'PSA total and free', 'PSA, total and free', 'PSA TOTAL, DIAGNOSTIC', 'TOTAL SERUM PSA - FOR HCH ONLY', 'PSA,Free & Total Profile', 'PSA,TOTAL (SCREEN)', 'PSA,TOTAL (DIAGNOSTIC)', 'Prostate Specific Antigen (PSA), Diagnostic', 'Prostate Specific AG (PSA), Total']
    LIST_OTHERS=['UROLOGY OFFICE VISIT (CLINIC)', 'ONCOLOGY OFFICE VISIT (CLINIC)', 'RADIATION ONCOLOGY OFFICE VISIT (CLINIC)', 'TESTOSTERONE, TOT AND FR, S', 'URO UROFLOW', 'URO Uroflow', 'URO Urodynamic study (with flow)', 'NM BONE SCAN WHOLE BODY', 'TESTOSTERONE, TOTAL BY IMMUNOASSAY, S', 'BMD BONE DENSITY SPINE HIPS', 'URO URETHRAL CATH FILL / REMOVE / VOIDING TRIAL (FILL/PULL)', 'URO CYSTOSCOPY (SPECIFIC PROVIDER)', 'URO Uroflow', 'Initial Rad Onc Treatment Planning CT Simulation', 'URO MR Fusion', 'URO Urethral cath fill / remove / voiding trial (fill/pull)', 'URO CYSTOSCOPY (GENERAL)', 'URO Prostate US', 'TESTOSTERONE, TOT, BIOAVAILABLE, AND FREE, S', 'URO Seed placement - prostate - fiducial markers', 'URO Urethral cath fill / remove / voiding trial (fill / pull)', 'URO Urodynamic study (with flow)', 'URO SPT CHANGE', 'URO BIOPSY - PROSTATE', 'URO Residual urine - ultrasound', 'URO CYSTO W/ PROSTATE US', 'URO Transperineal MR Fusion', 'Cystoscopy (specific provider)', 'URO Prostate transperineal biopsy', 'URO Rezum Treatment', 'URO CYSTO W/STENT REMOVAL', 'Cysto w/stent removal', 'URO INJECTION VISIT - CEFTRIAXONE (ROCEPHIN)', 'URO Urethral cath removal & voiding trial (UCO/VT)', 'Cysto w/botox', 'URO Retrograde Urethrogram', 'URO Urethral cath change (UCC)']

    LIST_PSA.remove('TESTOSTERONE, TOTAL BY MASS SPECROMETRY, S')
    LIST_IMAGING.remove('URO MR FUSION')
    LIST_BIOPSY.append('URO MR FUSION')
    LIST_BIOPSY.append('URO MR Fusion')
    LIST_IMAGING.remove('URO Transperineal MR Fusion')
    LIST_BIOPSY.append('URO Transperineal MR Fusion')
    LIST_IMAGING.remove('MR PROSTATE BIOPSY WITH IMAGING GUIDANCE')
    LIST_BIOPSY.append('MR PROSTATE BIOPSY WITH IMAGING GUIDANCE')
    LIST_BIOPSY.append('URO BIOPSY - PROSTATE')
    LIST_BIOPSY.append('URO Prostate transperineal biopsy')
    
    """Processes data for a single patient (group)."""
    # Find the first Urology visit date
    first_urology_visit = call_in_date
    first_urology_contact = call_in_date  
    #first_urology_contact = group_['PROSTATE_CANCER_REFERRAL_ENTRY_DATE'].min()
    #first_urology_visit = group_['PROSTATE_CANCER_ENC_APPT_TIME_FIRST'].min()

    # Calculate days_from_first_contact
    group_['days_from_first_contact'] = (pd.to_datetime(group_['ORDER_TIME']) - first_urology_contact).dt.days

    # Filter data within two year before the first visit
    group = group_[(group_['days_from_first_contact'] < 0) & (group_['days_from_first_contact'] >= -730)].copy()
    group_target = group_[(group_['days_from_first_contact'] >= 0) &
                          (pd.to_datetime(group_['ORDER_TIME']) < first_urology_visit)].copy() # <appt_time
    group_outofscope = group_[(pd.to_datetime(group_['ORDER_TIME']) > first_urology_visit)].copy()

    patient_data = {'PAT_MRN_ID': group_['PAT_MRN_ID'].iloc[0]}
    
    if 'PROSTATE_CANCER_ENC_APPT_TIME_FIRST' in group_.columns:
        patient_data['APPT_TIME'] = group_['PROSTATE_CANCER_ENC_APPT_TIME_FIRST'].min()
    
    if 'PROSTATE_CANCER_ENC_VISIT_NAME_FIRST' in group_.columns:
        patient_data['PROSTATE_CANCER_ENC_VISIT_NAME_FIRST'] = group_['PROSTATE_CANCER_ENC_VISIT_NAME_FIRST'].min()

    if 'PROSTATE_CANCER_VISIT_AGE_FIRST' in group_.columns:
        patient_data['PROSTATE_CANCER_VISIT_AGE_FIRST'] = group_['PROSTATE_CANCER_VISIT_AGE_FIRST'].min()

    if 'PROSTATE_CANCER_REQUEST_METHOD_C_FIRST' in group_.columns:
        patient_data['PROSTATE_CANCER_REQUEST_METHOD_C_FIRST'] = group_['PROSTATE_CANCER_REQUEST_METHOD_C_FIRST'].min()

    if 'PROSTATE_CANCER_ENC_APPT_PRC_ID_FIRST' in group_.columns:
        patient_data['PROSTATE_CANCER_ENC_APPT_PRC_ID_FIRST'] = group_['PROSTATE_CANCER_ENC_APPT_PRC_ID_FIRST'].min()

    if 'PROSTATE_CANCER_ENC_APPT_DEP_SPECIALTY_FIRST' in group_.columns:
        patient_data['PROSTATE_CANCER_ENC_APPT_DEP_SPECIALTY_FIRST'] = group_['PROSTATE_CANCER_ENC_APPT_DEP_SPECIALTY_FIRST'].min()

    if 'PROSTATE_CANCER_REFERRAL_CLASS' in group_.columns:
        patient_data['PROSTATE_CANCER_REFERRAL_CLASS'] = group_['PROSTATE_CANCER_REFERRAL_CLASS'].min()

    if 'icd_10_1' in group_.columns:
        patient_data['icd_10_1'] = group_['icd_10_1'].min()

    if 'icd_10_2' in group_.columns:
        patient_data['icd_10_2'] = group_['icd_10_2'].min()


    if not group.empty:
        # Conditions (2 most recent)
        conditions = group[group['DESCRIPTION'].isin(LIST_CONDITION)].sort_values('ORDER_TIME', ascending=False).head(2)
        for i, (_, row) in enumerate(conditions.iterrows()):
            patient_data[f'condition_{i+1}'] = row['DESCRIPTION']
            patient_data[f'condition_{i+1}_days'] = row['days_from_first_contact']
        # Imaging studies (3 most recent)
        imaging = group[group['DESCRIPTION'].isin(LIST_IMAGING)].sort_values('ORDER_TIME', ascending=False).head(3)
        for i, (_, row) in enumerate(imaging.iterrows()):
            patient_data[f'imaging_{i+1}'] = row['DESCRIPTION']
            patient_data[f'imaging_{i+1}_abnormal'] = row['ABNORMAL_YN']
            patient_data[f'imaging_{i+1}_days'] = row['days_from_first_contact']
        # Biopsy results (2 most recent)
        biopsy = group[group['DESCRIPTION'].isin(LIST_BIOPSY)].sort_values('ORDER_TIME', ascending=False).head(2)
        for i, (_, row) in enumerate(biopsy.iterrows()):
            patient_data[f'biopsy_{i+1}'] = row['DESCRIPTION']
            patient_data[f'biopsy_{i+1}_abnormal'] = row['ABNORMAL_YN']
            patient_data[f'biopsy_{i+1}_days'] = row['days_from_first_contact']
        # PSA exams (4 most recent)
        psa = group[group['DESCRIPTION'].isin(LIST_PSA)].sort_values('ORDER_TIME', ascending=False).head(4)
        #psa.loc[psa['ORD_NUM_VALUE'] >= 9999999,'ORD_NUM_VALUE'] = 0.1
        psa.loc[psa['ORD_VALUE'] == '<0.10', 'ORD_VALUE'] = '0.1'
        psa.loc[psa['ORD_VALUE'] == '<0.1', 'ORD_VALUE'] = '0.1'
        psa.loc[psa['ORD_VALUE'] == '<0.01', 'ORD_VALUE'] = '0.01'
        psa.loc[psa['ORD_VALUE'] == '>4500', 'ORD_VALUE'] = '4500'
        psa.loc[psa['ORD_VALUE'].notna() & (psa['ORD_VALUE'].str.contains(' ')), 'ORD_VALUE'] = np.nan
        psa = psa.loc[(psa['ORD_VALUE'] != 'CANCELED') & (psa['ORD_VALUE'] != 'Canceled')]

        psa['ORD_VALUE'] = psa['ORD_VALUE'].astype(float)
        for i, (_, row) in enumerate(psa.iterrows()):
            patient_data[f'psa_{i+1}'] = row['DESCRIPTION']
            patient_data[f'psa_{i+1}_abnormal'] = row['ABNORMAL_YN']
            patient_data[f'psa_{i+1}_value'] = row['ORD_VALUE']
            patient_data[f'psa_{i+1}_unit'] = row['REFERENCE_UNIT']
            patient_data[f'psa_{i+1}_days'] = row['days_from_first_contact']
        psa = psa[psa['REFERENCE_UNIT'] == 'ng/mL']
        if len(psa) >= 2:
            patient_data['psa_recent_increase_percent'] = ((psa['ORD_VALUE'].iloc[0] - psa['ORD_VALUE'].iloc[1]) / psa['ORD_VALUE'].iloc[1]) * 100
        else:
            patient_data['psa_recent_increase_percent'] = np.nan  # Handle cases with less than 2 PSA values
        patient_data['all_orders'] = '; '.join(group['DESCRIPTION'].astype(str).tolist())
        patient_data['CE_data'] = group['PROC_CODE'].isin(['LABEXT00001', 'IMG13031']).any()

    return pd.Series(patient_data)

def update_df(df,call_in_date):
    final_df = df.groupby('PAT_ID').apply(process_patient_group, call_in_date=call_in_date) #PAT_MRN_ID groupby gives a weird error with df[267:269]
    if len(final_df)==1:
        final_df = final_df.reset_index()
    else:
        final_df = final_df.unstack().reset_index()

    bi = [-730, -180, -60, -30, -10, 0]
    bl = ['long ago', 'months ago', 'a month ago', 'a few weeks ago', 'days ago']
    for series_name, series in final_df.loc[:, final_df.columns.str.contains('_days')].iloc[:, :9].items():
        final_df[series_name + '_cat'] = pd.cut(series, bins=bi, labels=bl)

    bvi = [0, 2, 4, 10, 20, 1000]
    bvl = ['low', 'medium', 'med-high', 'high', 'very high']
    for series_name, series in final_df.loc[:, final_df.columns.str.contains('_value')].items():
        final_df[series_name + '_cat'] = pd.cut(series, bins=bvi, labels=bvl)

    bpi = [-10000, -50, -10, 10, 50, 10000]
    bpl = ['decreased a lot', 'slightly decreased', 'not changed', 'slightly increased', 'increased a lot']
    for series_name, series in final_df.loc[:, final_df.columns.str.contains('_percent')].items():
        final_df[series_name + '_cat'] = pd.cut(series, bins=bpi, labels=bpl)

    bai = [0, 40, 50, 60, 70, 80, 120]
    bal = ['below 40s', 'in 40s', 'in 50s', 'in 60s', 'in 70s', 'above 80s']
    for series_name, series in final_df.loc[:, final_df.columns.str.contains('_AGE_')].items():
        final_df[series_name + '_cat'] = pd.cut(series, bins=bai, labels=bal)
    
    if 'APPT_TIME' in final_df.columns:
        final_df["APPT_TIME"] = final_df["APPT_TIME"].astype("string")
    if 'PROSTATE_CANCER_ENC_VISIT_NAME_FIRST' in final_df.columns:
        final_df["PROSTATE_CANCER_ENC_VISIT_NAME_FIRST"] = final_df["PROSTATE_CANCER_ENC_VISIT_NAME_FIRST"].astype("string")
    if 'PROSTATE_CANCER_VISIT_AGE_FIRST' in final_df.columns:
        final_df["PROSTATE_CANCER_VISIT_AGE_FIRST"] = final_df["PROSTATE_CANCER_VISIT_AGE_FIRST"].astype("string")
    if 'PROSTATE_CANCER_REQUEST_METHOD_C_FIRST' in final_df.columns:
        final_df["PROSTATE_CANCER_REQUEST_METHOD_C_FIRST"] = final_df["PROSTATE_CANCER_REQUEST_METHOD_C_FIRST"].astype("string")
    if 'PROSTATE_CANCER_ENC_APPT_PRC_ID_FIRST' in final_df.columns:
        final_df["PROSTATE_CANCER_ENC_APPT_PRC_ID_FIRST"] = final_df["PROSTATE_CANCER_ENC_APPT_PRC_ID_FIRST"].astype("string")
    if 'PROSTATE_CANCER_ENC_APPT_DEP_SPECIALTY_FIRST' in final_df.columns:
        final_df["PROSTATE_CANCER_ENC_APPT_DEP_SPECIALTY_FIRST"] = final_df["PROSTATE_CANCER_ENC_APPT_DEP_SPECIALTY_FIRST"].astype("string")
    if 'PROSTATE_CANCER_REFERRAL_CLASS' in final_df.columns:
        final_df["PROSTATE_CANCER_REFERRAL_CLASS"] = final_df["PROSTATE_CANCER_REFERRAL_CLASS"].astype("string")
    if 'icd_10_1' in final_df.columns:
        final_df["icd_10_1"] = final_df["icd_10_1"].astype("string")
    if 'icd_10_2' in final_df.columns:
        final_df["icd_10_2"] = final_df["icd_10_2"].astype("string")
    if 'CE_data' in final_df.columns:
        final_df['CE_data'] = final_df['CE_data'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'biopsy_1_abnormal' in final_df.columns:
        final_df['biopsy_1_abnormal'] = final_df['biopsy_1_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'biopsy_2_abnormal' in final_df.columns:
        final_df['biopsy_2_abnormal'] = final_df['biopsy_2_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'imaging_1_abnormal' in final_df.columns:
        final_df['imaging_1_abnormal'] = final_df['imaging_1_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'imaging_2_abnormal' in final_df.columns:
        final_df['imaging_2_abnormal'] = final_df['imaging_2_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'imaging_3_abnormal' in final_df.columns:
        final_df['imaging_3_abnormal'] = final_df['imaging_3_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'psa_1_abnormal' in final_df.columns:    
        final_df['psa_1_abnormal'] = final_df['psa_1_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'psa_2_abnormal' in final_df.columns:
        final_df['psa_2_abnormal'] = final_df['psa_2_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'psa_3_abnormal' in final_df.columns:
        final_df['psa_3_abnormal'] = final_df['psa_3_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'psa_4_abnormal' in final_df.columns:
        final_df['psa_4_abnormal'] = final_df['psa_4_abnormal'].fillna('N').map({'Y': True, 'N': False}).astype(bool)
    if 'PAT_ID' in final_df.columns:
        final_df["PAT_ID"] = final_df["PAT_ID"].astype("string")
    if 'PAT_MRN_ID' in final_df.columns:
        final_df["PAT_MRN_ID"] = final_df["PAT_MRN_ID"].astype("string")
    if 'all_orders' in final_df.columns:
        final_df["all_orders"] = final_df["all_orders"].astype("string")
    if 'biopsy_1' in final_df.columns:
        final_df["biopsy_1"] = final_df["biopsy_1"].astype("string")
    if 'biopsy_1_days' in final_df.columns:
        final_df["biopsy_1_days"] = final_df["biopsy_1_days"].astype("float64")
    if 'biopsy_2' in final_df.columns:
        final_df["biopsy_2"] = final_df["biopsy_2"].astype("string")
    if 'biopsy_2_days' in final_df.columns:
        final_df["biopsy_2_days"] = final_df["biopsy_2_days"].astype("float64")
    if 'icd_10_1' in final_df.columns:
        final_df["icd_10_1"] = final_df["icd_10_1"].astype("string")
    if 'icd_10_2' in final_df.columns:
        final_df["icd_10_2"] = final_df["icd_10_2"].astype("string")
    if 'imaging_1' in final_df.columns:
        final_df["imaging_1"] = final_df["imaging_1"].astype("string")
    if 'imaging_1_days' in final_df.columns:
        final_df["imaging_1_days"] = final_df["imaging_1_days"].astype("float64")
    if 'imaging_2' in final_df.columns:
        final_df["imaging_2"] = final_df["imaging_2"].astype("string")
    if 'imaging_2_days' in final_df.columns:
        final_df["imaging_2_days"] = final_df["imaging_2_days"].astype("float64")
    if 'imaging_3' in final_df.columns:
        final_df["imaging_3"] = final_df["imaging_3"].astype("string")
    if 'imaging_3_days' in final_df.columns:
        final_df["imaging_3_days"] = final_df["imaging_3_days"].astype("float64")
    if 'psa_1' in final_df.columns:
        final_df["psa_1"] = final_df["psa_1"].astype("string")
    if 'psa_1_days' in final_df.columns:
        final_df["psa_1_days"] = final_df["psa_1_days"].astype("float64")
    if 'psa_1_unit' in final_df.columns:
        final_df["psa_1_unit"] = final_df["psa_1_unit"].astype("string")
    if 'psa_1_value' in final_df.columns:
        final_df["psa_1_value"] = final_df["psa_1_value"].astype("float64")
    if 'psa_2' in final_df.columns:
        final_df["psa_2"] = final_df["psa_2"].astype("string")
    if 'psa_2_days' in final_df.columns:
        final_df["psa_2_days"] = final_df["psa_2_days"].astype("float64")
    if 'psa_2_unit' in final_df.columns:
        final_df["psa_2_unit"] = final_df["psa_2_unit"].astype("string")
    if 'psa_2_value' in final_df.columns:
        final_df["psa_2_value"] = final_df["psa_2_value"].astype("float64")
    if 'psa_3' in final_df.columns:
        final_df["psa_3"] = final_df["psa_3"].astype("string")
    if 'psa_3_days' in final_df.columns:
        final_df["psa_3_days"] = final_df["psa_3_days"].astype("float64")
    if 'psa_3_unit' in final_df.columns:
        final_df["psa_3_unit"] = final_df["psa_3_unit"].astype("string")
    if 'psa_3_value' in final_df.columns:
        final_df["psa_3_value"] = final_df["psa_3_value"].astype("float64")
    if 'psa_4' in final_df.columns:
        final_df["psa_4"] = final_df["psa_4"].astype("string")
    if 'psa_4_days' in final_df.columns:
        final_df["psa_4_days"] = final_df["psa_4_days"].astype("float64")
    if 'psa_4_unit' in final_df.columns:
        final_df["psa_4_unit"] = final_df["psa_4_unit"].astype("string")
    if 'psa_4_value' in final_df.columns:
        final_df["psa_4_value"] = final_df["psa_4_value"].astype("float64")
    if 'psa_recent_increase_percent' in final_df.columns:
        final_df["psa_recent_increase_percent"] = final_df["psa_recent_increase_percent"].astype("float64")
    if 'biopsy_1_days_cat' in final_df.columns:
        final_df["biopsy_1_days_cat"] = final_df["biopsy_1_days_cat"].astype("string")
    if 'biopsy_2_days_cat' in final_df.columns:
        final_df["biopsy_2_days_cat"] = final_df["biopsy_2_days_cat"].astype("string")
    if 'imaging_1_days_cat' in final_df.columns:
        final_df["imaging_1_days_cat"] = final_df["imaging_1_days_cat"].astype("string")
    if 'imaging_2_days_cat' in final_df.columns:
        final_df["imaging_2_days_cat"] = final_df["imaging_2_days_cat"].astype("string")
    if 'imaging_3_days_cat' in final_df.columns:
        final_df["imaging_3_days_cat"] = final_df["imaging_3_days_cat"].astype("string")
    if 'psa_1_days_cat' in final_df.columns:
        final_df["psa_1_days_cat"] = final_df["psa_1_days_cat"].astype("string")
    if 'psa_2_days_cat' in final_df.columns:
        final_df["psa_2_days_cat"] = final_df["psa_2_days_cat"].astype("string")
    if 'psa_3_days_cat' in final_df.columns:
        final_df["psa_3_days_cat"] = final_df["psa_3_days_cat"].astype("string")
    if 'psa_4_days_cat' in final_df.columns:
        final_df["psa_4_days_cat"] = final_df["psa_4_days_cat"].astype("string")
    if 'psa_1_value_cat' in final_df.columns:
        final_df["psa_1_value_cat"] = final_df["psa_1_value_cat"].astype("string")
    if 'psa_2_value_cat' in final_df.columns:
        final_df["psa_2_value_cat"] = final_df["psa_2_value_cat"].astype("string")
    if 'psa_3_value_cat' in final_df.columns:
        final_df["psa_3_value_cat"] = final_df["psa_3_value_cat"].astype("string")
    if 'psa_4_value_cat' in final_df.columns:
        final_df["psa_4_value_cat"] = final_df["psa_4_value_cat"].astype("string")
    if 'psa_recent_increase_percent_cat' in final_df.columns:
        final_df["psa_recent_increase_percent_cat"] = final_df["psa_recent_increase_percent_cat"].astype("string")   
    
    for column in final_df.columns:
        if final_df[column].dtype == 'string':
            final_df[column].fillna('', inplace=True)
        elif final_df[column].dtype in ['int64', 'float64']:
            final_df[column].fillna(0, inplace=True)
    
    return final_df

def convert_df(df):
    patlist = df.to_dict(orient='records')
    return patlist

def get_env_project_dataset(env) :
    dev_list = ['development','d','dev', 'n']
    prod_list = ['production','p','prod']
    if env.lower() in dev_list:
        return "ml-mps-adl-intudpcl-phi-d-f9bc", "d"
    elif env.lower() in prod_list:
        return "ml-mps-adl-intudpcl-phi-p-deec", "p"
    else:
        return "",""

def get_query_by_env(env, proj, dataset, call_in_date, mcn):
    query = ""
    if env == 'd':
        query = f"""
        SELECT PAT.PAT_ID, PAT.PAT_MRN_ID, 
        DATE_DIFF(date('{call_in_date.strftime('%Y-%m-%d')}'), date(PAT.BIRTH_DATE), year) as PROSTATE_CANCER_VISIT_AGE_FIRST, 
        O.ORDER_TIME, O.DISPLAY_NAME, O.DESCRIPTION, 
        O.RESULT_TIME, O2.ORD_VALUE, O2.REFERENCE_UNIT, O2.RESULT_STATUS_C, O2.RESULT_FLAG_C, O.ABNORMAL_YN, 
        DATE_DIFF(O.RESULT_TIME, O.ORDER_TIME, day) as Result_datediff, 
        DATE_DIFF(PARSE_DATE('%Y-%m-%d','{call_in_date.strftime('%Y-%m-%d')}'), O.ORDER_TIME, day) as Order_datediff, 
        EAP.PROC_CODE, EAP.PROC_ID, EAP.PROC_NAME, EAP.PROC_CAT_ID 
        FROM 
        `{proj}.phi_patient_us_{dataset}.PATIENT` PAT
        LEFT OUTER JOIN `{proj}.phi_clarity_us_{dataset}.ORDER_PROC` O ON O.PAT_ID = PAT.PAT_ID 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.ORDER_RESULTS` O2 ON O2.ORDER_PROC_ID = O.ORDER_PROC_ID AND O2.LINE = 1 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.CLARITY_EAP` EAP ON EAP.PROC_ID = O.PROC_ID 
        WHERE 
        O.ORDER_TIME <= '{call_in_date.strftime('%Y-%m-%d')}'
        and O.ORDER_STATUS_C = 5 
        and O.FUTURE_OR_STAND is null 
        and O.DESCRIPTION in ( 
        'PR BIOPSY PROSTATE NEEDLE/PUNCH', 'URO BIOPSY - PROSTATE', 'CYTOLOGY FINE NEEDLE ASPIRATION (INCLUDES CORE BIOPSIES', 'PR US TRANSRECTAL', 'PR US GUIDE PLC NDL', 'Bx, Prostate', 'Bx, Prostate Fusion', 'PR BX PROSTATE NDL TRANSPRNEAL', 'URO PROSTATE TRANSPERINEAL BIOPSY', 
        'MR PROSTATE WITHOUT AND WITH IV CONTRAST', 'MR PROSTATE WITHOUT IV CONTRAST', 'CT ABDOMEN PELVIS WITH IV CONTRAST', 'SCAN BLADDER', 'UROLOGY IMAGE EXAM', 'PET CT SKULL TO THIGH PSMA', 'URO MR FUSION', 'URO Prostate US', 'PR US TRANSRECTAL', 'URO CYSTO W/ PROSTATE US', 'PR MRI PELVIS WO/W CNTRST', 'HC MRI PELVIS WO/W CNTRST', 'PR CT ABD&PELVIS W CNTRST', 'URO Transperineal MR Fusion', 'Cysto w/ Prostate US', 'PET CT Prostate PSMA', 'US, Transrectal', 'UROLOGY Image Exam', 'PETCT with Diagnostic CT of the neck-chest-abd-pelvis w/IV contrast; PSMA', 'MR PROSTATE BIOPSY WITH IMAGING GUIDANCE', 'URO KUB', 'PR CT ABDOMEN WO/W CNTRST', 'US PROSTATE FIDUCIAL MARKER PLACEMENT WITH HYDROGEL SPACER', 'US Renal And Urinary Bladder', 'PR CT ABDOMEN W CNTRST', 'CT chest abdomen pelvis with contrast', 'PET CT FLUCICLOVINE PROSTATE', 'MRI Brain with and without Contrast', 'CT Abdomen Pelvis', 'PR CTA HRT CORONARY W CNTRST', 'US Renal Complete', 'CT Abdomen/Pelvis', 
        'PROSTATE-SPECIFIC AG (PSA) DIAGNOSTIC, S', 'PROSTATE-SPECIFIC AG (PSA) SCRN, S', 'TESTOSTERONE, TOTAL BY MASS SPECROMETRY, S', 'PROSTATE-SPECIFIC AG (PSA), TOT AND FR, S', 'PROSTATE-SPECIFIC AG (PSA)', 'PSA, ULTRASENSITIVE, S', 'PROSTATE-SPECIFIC AG (PSA), TOT AND FR, S', 'PSA Total Diagnostic', 'PSA', 'PSA Total', 'PROSTATIC SPECIFIC ANTIGEN (PSA)', 'Prostate Specific Antigen (Total), Blood', 'PSA, Diagnostic', 'PSA, Screen', 'PSA Total Diagnostic', 'PROSTATIC SPECIFIC ANTIGEN-PSA', 'Prostatic Specific Antigen Diagnostic', 'PSA diagnostic Blood, Venous', 'PSA (Prostate Specific Antigen) - DIAGNOSTIC', 'PSA diagnostic (monitoring)', 'PSA Screening', 'PSA,Total (Screen)', 'PSA,Total (Diagnostic)', 'PROSTATE SPECIFIC ANTIGEN (PSA), TOTAL DIAGNOSTIC', 'PSA screen', 'PSA Monitor', 'PSA diagnostic', 'Prostatic Specific Antigen Screen', 'PSA total, diagnostic', 'PSA, Total and Free', 'PSA Total', 'Prostate Specific Ag (PSA), Total', 'PSA, Diagnostic/Monitoring', 'PSA TOTAL', 'PSA, Ultrasensitive', 'PSA,Total', 'Prostate Specific Antigen', 'Prostatic Specific Antigen', 'PSA Antigen (PSA Screen)', 'PSA/PROSTSPECAG DIAG', 'PSA,Total (Diagnostic)', 'Prostate Specific Ag', 'PSA SCREEN', 'PROSTATIC SPECIFIC ANTIGEN', 'PSA, Reflex PSA Free', 'PSA-DIAGNOSTIC, BL (ROCHE)', 'PROSTATIC SPC AG, SYMPTOMATIC', 'PSA (Screen), Blood - See Instructions', 'PROSTATE SPECIFIC ANTIGEN, DIAGNOSTIC', 'PSA, TOTAL', 'PROSTATE SPECIFIC ANTIGEN, DIAGNOSTIC', 'PSA total and free', 'PSA, total and free', 'PSA TOTAL, DIAGNOSTIC', 'TOTAL SERUM PSA - FOR HCH ONLY', 'PSA,Free & Total Profile', 'PSA,TOTAL (SCREEN)', 'PSA,TOTAL (DIAGNOSTIC)', 'Prostate Specific Antigen (PSA), Diagnostic', 'Prostate Specific AG (PSA), Total', 
        'UROLOGY OFFICE VISIT (CLINIC)', 'ONCOLOGY OFFICE VISIT (CLINIC)', 'RADIATION ONCOLOGY OFFICE VISIT (CLINIC)', 'TESTOSTERONE, TOT AND FR, S', 'URO UROFLOW', 'URO Uroflow', 'URO Urodynamic study (with flow)', 'NM BONE SCAN WHOLE BODY', 'TESTOSTERONE, TOTAL BY IMMUNOASSAY, S', 'BMD BONE DENSITY SPINE HIPS', 'URO URETHRAL CATH FILL / REMOVE / VOIDING TRIAL (FILL/PULL)', 'URO CYSTOSCOPY (SPECIFIC PROVIDER)', 'URO Uroflow', 'Initial Rad Onc Treatment Planning CT Simulation', 'URO MR Fusion', 'URO Urethral cath fill / remove / voiding trial (fill/pull)', 'URO CYSTOSCOPY (GENERAL)', 'URO Prostate US', 'TESTOSTERONE, TOT, BIOAVAILABLE, AND FREE, S', 'URO Seed placement - prostate - fiducial markers', 'URO Urethral cath fill / remove / voiding trial (fill / pull)', 'URO Urodynamic study (with flow)', 'URO SPT CHANGE', 'URO BIOPSY - PROSTATE', 'URO Residual urine - ultrasound', 'URO CYSTO W/ PROSTATE US', 'URO Transperineal MR Fusion', 'Cystoscopy (specific provider)', 'URO Prostate transperineal biopsy', 'URO Rezum Treatment', 'URO CYSTO W/STENT REMOVAL', 'Cysto w/stent removal', 'URO INJECTION VISIT - CEFTRIAXONE (ROCEPHIN)', 'URO Urethral cath removal & voiding trial (UCO/VT)', 'Cysto w/botox', 'URO Retrograde Urethrogram', 'URO Urethral cath change (UCC)' 
        ) 
        and PAT.PAT_MRN_ID = '{mcn}'"""  
    else:
        query = f""" 
        WITH 
        COHORT AS ( 
        SELECT 
        pat_enc.PAT_ID 
        ,PAT.PAT_MRN_ID 
        ,DATE_DIFF(date('{call_in_date.strftime('%Y-%m-%d')}'), date(min(PAT.BIRTH_DATE)), year) as PROSTATE_CANCER_VISIT_AGE_FIRST 
        ,MIN(PAT_ENC.PAT_ENC_CSN_ID) AS PROSTATE_CANCER_VISIT_CSN_FIRST 
        ,MIN(PAT_ENC.APPT_TIME) AS PROSTATE_CANCER_ENC_APPT_TIME_FIRST 
        ,MIN(ZSP.NAME)  AS PROSTATE_CANCER_ENC_APPT_DEP_SPECIALTY_FIRST 
        ,MIN(PAT_ENC.APPT_PRC_ID) AS PROSTATE_CANCER_ENC_APPT_PRC_ID_FIRST 
        ,MIN(a.REQUEST_METHOD_C) AS PROSTATE_CANCER_REQUEST_METHOD_C_FIRST 
        ,MIN(A.REQUEST_ID) AS PROSTATE_CANCER_REQUEST_ID_FIRST 
        ,MIN(R.ENTRY_DATE) AS PROSTATE_CANCER_REFERRAL_ENTRY_DATE 
        ,MIN(PRC.PRC_NAME) AS PROSTATE_CANCER_ENC_VISIT_NAME_FIRST 
        ,MIN(r.rfl_class_c) as PROSTATE_CANCER_REFERRAL_CLASS --1 - Internal, 2 - Incoming 
        ,min( e.CURRENT_ICD10_LIST) as icd_10_1 
        ,min(e2.CURRENT_ICD10_LIST) as icd_10_2 
        ,min(r.REFD_TO_SPEC_C) as PROSTATE_CANCER_REFD_TO_SPEC_C 
        FROM `{proj}.phi_clarity_us_{dataset}.PAT_ENC` pat_enc 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.CLARITY_DEP` dep on dep.DEPARTMENT_ID = pat_enc.department_id 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.CLARITY_LOC`    CL         ON DEP.REV_LOC_ID                  = CL.LOC_ID 
        LEFT JOIN `{proj}.phi_patient_us_{dataset}.PATIENT` pat on pat.PAT_ID = pat_enc.PAT_ID 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.ZC_SPECIALTY_DEP`   ZSP            ON dep.SPECIALTY_DEP_C             = ZSP.SPECIALTY_DEP_C 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.APPT_REQUEST`a ON a.REFERRAL_ID = PAT_ENC.REFERRAL_ID 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.REFERRAL`R ON R.REFERRAL_ID = PAT_ENC.REFERRAL_ID 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.CLARITY_PRC` PRC ON PRC.PRC_ID = PAT_ENC.aPPT_PRC_ID 
        left join `{proj}.phi_clarity_us_{dataset}.REFERRAL_DX` d on a.REFERRAL_ID = d.REFERRAL_ID and d.line = 1  
        left join `{proj}.phi_clarity_us_{dataset}.CLARITY_EDG` e on d.dx_id = e.DX_ID 
        left join `{proj}.phi_clarity_us_{dataset}.REFERRAL_DX` d2 on a.REFERRAL_ID = d2.REFERRAL_ID and d2.line = 2  
        left join `{proj}.phi_clarity_us_{dataset}.CLARITY_EDG` e2 on d2.dx_id = e2.DX_ID 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.CLARITY_PRC_2` prc2 on prc2.PRC_ID = PAT_ENC.aPPT_PRC_ID 
        WHERE 
        PAT.PAT_MRN_ID = '{mcn}'
        and prc2.REPORT_CATEGORY_C in (105, 101)  --New and Consult 
        and e.CURRENT_ICD10_LIST in ('R97.20', 'C61', 'N40.0') 
        and r.REFD_TO_SPEC_C in ('43', '39') --Urology 
        AND PAT_ENC.APPT_STATUS_C = 2 -- 2 - Completed 
        Group by  pat_enc.PAT_ID ,PAT.PAT_MRN_ID 
        ) 
        SELECT C.*, O.ORDER_TIME, O.DISPLAY_NAME, O.DESCRIPTION, 
        O.RESULT_TIME, --O2.ORD_NUM_VALUE, 
        O2.ORD_VALUE, O2.REFERENCE_UNIT, O2.RESULT_STATUS_C, O2.RESULT_FLAG_C, O.ABNORMAL_YN, 
        DATE_DIFF(O.RESULT_TIME, O.ORDER_TIME, day) as Result_datediff, 
        DATE_DIFF(PARSE_DATE('%Y-%m-%d','{call_in_date.strftime('%Y-%m-%d')}'), O.ORDER_TIME, day) as Order_datediff, 
        EAP.PROC_CODE, EAP.PROC_ID, EAP.PROC_NAME, EAP.PROC_CAT_ID 
        FROM 
        COHORT C 
        LEFT OUTER JOIN `{proj}.phi_clarity_us_{dataset}.ORDER_PROC` O ON O.PAT_ID = C.PAT_ID 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.ORDER_RESULTS` O2 ON O2.ORDER_PROC_ID = O.ORDER_PROC_ID AND O2.LINE = 1 
        LEFT JOIN `{proj}.phi_clarity_us_{dataset}.CLARITY_EAP` EAP ON EAP.PROC_ID = O.PROC_ID 
        WHERE 
        O.ORDER_TIME <= '{call_in_date.strftime('%Y-%m-%d')}'
        and O.ORDER_STATUS_C = 5 
        and O.FUTURE_OR_STAND is null 
        and O.DESCRIPTION in ( 
        'PR BIOPSY PROSTATE NEEDLE/PUNCH', 'URO BIOPSY - PROSTATE', 'CYTOLOGY FINE NEEDLE ASPIRATION (INCLUDES CORE BIOPSIES', 'PR US TRANSRECTAL', 'PR US GUIDE PLC NDL', 'Bx, Prostate', 'Bx, Prostate Fusion', 'PR BX PROSTATE NDL TRANSPRNEAL', 'URO PROSTATE TRANSPERINEAL BIOPSY', 
        'MR PROSTATE WITHOUT AND WITH IV CONTRAST', 'MR PROSTATE WITHOUT IV CONTRAST', 'CT ABDOMEN PELVIS WITH IV CONTRAST', 'SCAN BLADDER', 'UROLOGY IMAGE EXAM', 'PET CT SKULL TO THIGH PSMA', 'URO MR FUSION', 'URO Prostate US', 'PR US TRANSRECTAL', 'URO CYSTO W/ PROSTATE US', 'PR MRI PELVIS WO/W CNTRST', 'HC MRI PELVIS WO/W CNTRST', 'PR CT ABD&PELVIS W CNTRST', 'URO Transperineal MR Fusion', 'Cysto w/ Prostate US', 'PET CT Prostate PSMA', 'US, Transrectal', 'UROLOGY Image Exam', 'PETCT with Diagnostic CT of the neck-chest-abd-pelvis w/IV contrast; PSMA', 'MR PROSTATE BIOPSY WITH IMAGING GUIDANCE', 'URO KUB', 'PR CT ABDOMEN WO/W CNTRST', 'US PROSTATE FIDUCIAL MARKER PLACEMENT WITH HYDROGEL SPACER', 'US Renal And Urinary Bladder', 'PR CT ABDOMEN W CNTRST', 'CT chest abdomen pelvis with contrast', 'PET CT FLUCICLOVINE PROSTATE', 'MRI Brain with and without Contrast', 'CT Abdomen Pelvis', 'PR CTA HRT CORONARY W CNTRST', 'US Renal Complete', 'CT Abdomen/Pelvis', 
        'PROSTATE-SPECIFIC AG (PSA) DIAGNOSTIC, S', 'PROSTATE-SPECIFIC AG (PSA) SCRN, S', 'TESTOSTERONE, TOTAL BY MASS SPECROMETRY, S', 'PROSTATE-SPECIFIC AG (PSA), TOT AND FR, S', 'PROSTATE-SPECIFIC AG (PSA)', 'PSA, ULTRASENSITIVE, S', 'PROSTATE-SPECIFIC AG (PSA), TOT AND FR, S', 'PSA Total Diagnostic', 'PSA', 'PSA Total', 'PROSTATIC SPECIFIC ANTIGEN (PSA)', 'Prostate Specific Antigen (Total), Blood', 'PSA, Diagnostic', 'PSA, Screen', 'PSA Total Diagnostic', 'PROSTATIC SPECIFIC ANTIGEN-PSA', 'Prostatic Specific Antigen Diagnostic', 'PSA diagnostic Blood, Venous', 'PSA (Prostate Specific Antigen) - DIAGNOSTIC', 'PSA diagnostic (monitoring)', 'PSA Screening', 'PSA,Total (Screen)', 'PSA,Total (Diagnostic)', 'PROSTATE SPECIFIC ANTIGEN (PSA), TOTAL DIAGNOSTIC', 'PSA screen', 'PSA Monitor', 'PSA diagnostic', 'Prostatic Specific Antigen Screen', 'PSA total, diagnostic', 'PSA, Total and Free', 'PSA Total', 'Prostate Specific Ag (PSA), Total', 'PSA, Diagnostic/Monitoring', 'PSA TOTAL', 'PSA, Ultrasensitive', 'PSA,Total', 'Prostate Specific Antigen', 'Prostatic Specific Antigen', 'PSA Antigen (PSA Screen)', 'PSA/PROSTSPECAG DIAG', 'PSA,Total (Diagnostic)', 'Prostate Specific Ag', 'PSA SCREEN', 'PROSTATIC SPECIFIC ANTIGEN', 'PSA, Reflex PSA Free', 'PSA-DIAGNOSTIC, BL (ROCHE)', 'PROSTATIC SPC AG, SYMPTOMATIC', 'PSA (Screen), Blood - See Instructions', 'PROSTATE SPECIFIC ANTIGEN, DIAGNOSTIC', 'PSA, TOTAL', 'PROSTATE SPECIFIC ANTIGEN, DIAGNOSTIC', 'PSA total and free', 'PSA, total and free', 'PSA TOTAL, DIAGNOSTIC', 'TOTAL SERUM PSA - FOR HCH ONLY', 'PSA,Free & Total Profile', 'PSA,TOTAL (SCREEN)', 'PSA,TOTAL (DIAGNOSTIC)', 'Prostate Specific Antigen (PSA), Diagnostic', 'Prostate Specific AG (PSA), Total', 
        'UROLOGY OFFICE VISIT (CLINIC)', 'ONCOLOGY OFFICE VISIT (CLINIC)', 'RADIATION ONCOLOGY OFFICE VISIT (CLINIC)', 'TESTOSTERONE, TOT AND FR, S', 'URO UROFLOW', 'URO Uroflow', 'URO Urodynamic study (with flow)', 'NM BONE SCAN WHOLE BODY', 'TESTOSTERONE, TOTAL BY IMMUNOASSAY, S', 'BMD BONE DENSITY SPINE HIPS', 'URO URETHRAL CATH FILL / REMOVE / VOIDING TRIAL (FILL/PULL)', 'URO CYSTOSCOPY (SPECIFIC PROVIDER)', 'URO Uroflow', 'Initial Rad Onc Treatment Planning CT Simulation', 'URO MR Fusion', 'URO Urethral cath fill / remove / voiding trial (fill/pull)', 'URO CYSTOSCOPY (GENERAL)', 'URO Prostate US', 'TESTOSTERONE, TOT, BIOAVAILABLE, AND FREE, S', 'URO Seed placement - prostate - fiducial markers', 'URO Urethral cath fill / remove / voiding trial (fill / pull)', 'URO Urodynamic study (with flow)', 'URO SPT CHANGE', 'URO BIOPSY - PROSTATE', 'URO Residual urine - ultrasound', 'URO CYSTO W/ PROSTATE US', 'URO Transperineal MR Fusion', 'Cystoscopy (specific provider)', 'URO Prostate transperineal biopsy', 'URO Rezum Treatment', 'URO CYSTO W/STENT REMOVAL', 'Cysto w/stent removal', 'URO INJECTION VISIT - CEFTRIAXONE (ROCEPHIN)', 'URO Urethral cath removal & voiding trial (UCO/VT)', 'Cysto w/botox', 'URO Retrograde Urethrogram', 'URO Urethral cath change (UCC)' 
        )    
    """
    #print(query)
    return query

def get_sql_patient(mcn, call_in_date, env):
    client = bigquery.Client()
    proj,dataset = get_env_project_dataset(env)
    if not proj or not dataset:
        raise ValueError(f"project or dataset is not set.  Check the env parameter to ensure it is correct.  Should be d or p.  Your value was {env} ")
    
    query = get_query_by_env(env, proj, dataset, call_in_date, mcn)

    # Run the query and get the results
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    df = df.replace({np.nan: None})

    return df


###############################################
#get_expanded_patient Function
#   params:
#       mcn: (string) Needs to be XX-XXX-XXX format (hyphens need to be present)
#       call_in_date: Python datetime obj
#       env: d (for development clarity) or p (for production clarity)
#   returns:
#       pandas dataframe
###############################################
def get_expanded_patient(mcn, call_in_date, env):
    df =  get_sql_patient(mcn, call_in_date, env)  
    df = update_df(df,call_in_date)
    patlist = convert_df(df)
    return patlist