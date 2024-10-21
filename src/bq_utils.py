from google.cloud import bigquery
import pandas as pd
import numpy as np

def process_patient_group(group, call_in_date):
    # from GenAI
    LIST_CONDITION=[]
    LIST_IMAGING=['MR PROSTATE WITHOUT AND WITH IV CONTRAST', 'MR PROSTATE WITHOUT IV CONTRAST', 'CT ABDOMEN PELVIS WITH IV CONTRAST', 'SCAN BLADDER', 'UROLOGY IMAGE EXAM', 'PET CT SKULL TO THIGH PSMA', 'URO MR FUSION', 'URO Prostate US', 'PR US TRANSRECTAL', 'URO CYSTO W/ PROSTATE US', 'PR MRI PELVIS WO/W CNTRST', 'HC MRI PELVIS WO/W CNTRST', 'PR CT ABD&PELVIS W CNTRST', 'URO Transperineal MR Fusion', 'Cysto w/ Prostate US', 'PET CT Prostate PSMA', 'US, Transrectal', 'UROLOGY Image Exam', 'PETCT with Diagnostic CT of the neck-chest-abd-pelvis w/IV contrast; PSMA', 'MR PROSTATE BIOPSY WITH IMAGING GUIDANCE', 'URO KUB', 'PR CT ABDOMEN WO/W CNTRST', 'US PROSTATE FIDUCIAL MARKER PLACEMENT WITH HYDROGEL SPACER', 'US Renal And Urinary Bladder', 'PR CT ABDOMEN W CNTRST', 'CT chest abdomen pelvis with contrast', 'PET CT FLUCICLOVINE PROSTATE', 'MRI Brain with and without Contrast', 'CT Abdomen Pelvis', 'PR CTA HRT CORONARY W CNTRST', 'US Renal Complete', 'CT Abdomen/Pelvis']
    LIST_BIOPSY=['PR BIOPSY PROSTATE NEEDLE/PUNCH', 'URO BIOPSY - PROSTATE', 'CYTOLOGY FINE NEEDLE ASPIRATION (INCLUDES CORE BIOPSIES', 'PR US TRANSRECTAL', 'PR US GUIDE PLC NDL', 'Bx, Prostate', 'Bx, Prostate Fusion', 'PR BX PROSTATE NDL TRANSPRNEAL', 'URO PROSTATE TRANSPERINEAL BIOPSY']
    LIST_PSA=['PROSTATE-SPECIFIC AG (PSA) DIAGNOSTIC, S', 'PROSTATE-SPECIFIC AG (PSA) SCRN, S', 'TESTOSTERONE, TOTAL BY MASS SPECROMETRY, S', 'PROSTATE-SPECIFIC AG (PSA), TOT AND FR, S', 'PROSTATE-SPECIFIC AG (PSA)', 'PSA, ULTRASENSITIVE, S', 'PROSTATE-SPECIFIC AG (PSA), TOT AND FR, S', 'PSA Total Diagnostic', 'PSA', 'PSA Total', 'PROSTATIC SPECIFIC ANTIGEN (PSA)', 'Prostate Specific Antigen (Total), Blood', 'PSA, Diagnostic', 'PSA, Screen', 'PSA Total Diagnostic', 'PROSTATIC SPECIFIC ANTIGEN-PSA', 'Prostatic Specific Antigen Diagnostic', 'PSA diagnostic Blood, Venous', 'PSA (Prostate Specific Antigen) - DIAGNOSTIC', 'PSA diagnostic (monitoring)', 'PSA Screening', 'PSA,Total (Screen)', 'PSA,Total (Diagnostic)', 'PROSTATE SPECIFIC ANTIGEN (PSA), TOTAL DIAGNOSTIC', 'PSA screen', 'PSA Monitor', 'PSA diagnostic', 'Prostatic Specific Antigen Screen', 'PSA total, diagnostic', 'PSA, Total and Free', 'PSA Total', 'Prostate Specific Ag (PSA), Total', 'PSA, Diagnostic/Monitoring', 'PSA TOTAL', 'PSA, Ultrasensitive', 'PSA,Total', 'Prostate Specific Antigen', 'Prostatic Specific Antigen', 'PSA Antigen (PSA Screen)', 'PSA/PROSTSPECAG DIAG', 'PSA,Total (Diagnostic)', 'Prostate Specific Ag', 'PSA SCREEN', 'PROSTATIC SPECIFIC ANTIGEN', 'PSA, Reflex PSA Free', 'PSA-DIAGNOSTIC, BL (ROCHE)', 'PROSTATIC SPC AG, SYMPTOMATIC', 'PSA (Screen), Blood - See Instructions', 'PROSTATE SPECIFIC ANTIGEN, DIAGNOSTIC', 'PSA, TOTAL', 'PROSTATE SPECIFIC ANTIGEN, DIAGNOSTIC', 'PSA total and free', 'PSA, total and free', 'PSA TOTAL, DIAGNOSTIC', 'TOTAL SERUM PSA - FOR HCH ONLY', 'PSA,Free & Total Profile', 'PSA,TOTAL (SCREEN)', 'PSA,TOTAL (DIAGNOSTIC)', 'Prostate Specific Antigen (PSA), Diagnostic', 'Prostate Specific AG (PSA), Total']
    #LIST_OTHERS=['UROLOGY OFFICE VISIT (CLINIC)', 'ONCOLOGY OFFICE VISIT (CLINIC)', 'RADIATION ONCOLOGY OFFICE VISIT (CLINIC)', 'TESTOSTERONE, TOT AND FR, S', 'URO UROFLOW', 'URO Uroflow', 'URO Urodynamic study (with flow)', 'NM BONE SCAN WHOLE BODY', 'TESTOSTERONE, TOTAL BY IMMUNOASSAY, S', 'BMD BONE DENSITY SPINE HIPS', 'URO URETHRAL CATH FILL / REMOVE / VOIDING TRIAL (FILL/PULL)', 'URO CYSTOSCOPY (SPECIFIC PROVIDER)', 'URO Uroflow', 'Initial Rad Onc Treatment Planning CT Simulation', 'URO MR Fusion', 'URO Urethral cath fill / remove / voiding trial (fill/pull)', 'URO CYSTOSCOPY (GENERAL)', 'URO Prostate US', 'TESTOSTERONE, TOT, BIOAVAILABLE, AND FREE, S', 'URO Seed placement - prostate - fiducial markers', 'URO Urethral cath fill / remove / voiding trial (fill / pull)', 'URO Urodynamic study (with flow)', 'URO SPT CHANGE', 'URO BIOPSY - PROSTATE', 'URO Residual urine - ultrasound', 'URO CYSTO W/ PROSTATE US', 'URO Transperineal MR Fusion', 'Cystoscopy (specific provider)', 'URO Prostate transperineal biopsy', 'URO Rezum Treatment', 'URO CYSTO W/STENT REMOVAL', 'Cysto w/stent removal', 'URO INJECTION VISIT - CEFTRIAXONE (ROCEPHIN)', 'URO Urethral cath removal & voiding trial (UCO/VT)', 'Cysto w/botox', 'URO Retrograde Urethrogram', 'URO Urethral cath change (UCC)']

    # manual modification - need confirmation from billing data, practice, etc.
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
    
    # Calculate days_from_first_visit
    group['days_from_first_visit'] = (pd.to_datetime(group['ORDER_TIME']) - first_urology_visit).dt.days
    
    # Filter data within one year before the first visit
    group = group[(group['days_from_first_visit'] <= 0) & (group['days_from_first_visit'] >= -365)].copy()
    
    if not group.empty:
        patient_data = {'PAT_MRN_ID': group['PAT_MRN_ID'].iloc[0]}

        # Conditions (2 most recent)
        conditions = group[group['DESCRIPTION'].isin(LIST_CONDITION)].sort_values('ORDER_TIME', ascending=False).head(2)
        for i, (_, row) in enumerate(conditions.iterrows()):
            patient_data[f'condition_{i+1}'] = row['DESCRIPTION']
            patient_data[f'condition_{i+1}_days'] = row['days_from_first_visit']
        # Imaging studies (3 most recent)
        imaging = group[group['DESCRIPTION'].isin(LIST_IMAGING)].sort_values('ORDER_TIME', ascending=False).head(3)
        for i, (_, row) in enumerate(imaging.iterrows()):
            patient_data[f'imaging_{i+1}'] = row['DESCRIPTION']
            patient_data[f'imaging_{i+1}_abnormal'] = row['ABNORMAL_YN']
            patient_data[f'imaging_{i+1}_days'] = row['days_from_first_visit']
        # Biopsy results (2 most recent)
        biopsy = group[group['DESCRIPTION'].isin(LIST_BIOPSY)].sort_values('ORDER_TIME', ascending=False).head(2)
        for i, (_, row) in enumerate(biopsy.iterrows()):
            patient_data[f'biopsy_{i+1}'] = row['DESCRIPTION']
            patient_data[f'biopsy_{i+1}_abnormal'] = row['ABNORMAL_YN']
            patient_data[f'biopsy_{i+1}_days'] = row['days_from_first_visit']
        # PSA exams (4 most recent)
        psa = group[group['DESCRIPTION'].isin(LIST_PSA)].sort_values('ORDER_TIME', ascending=False).head(4)
        for i, (_, row) in enumerate(psa.iterrows()):
            patient_data[f'psa_{i+1}'] = row['DESCRIPTION']
            patient_data[f'psa_{i+1}_abnormal'] = row['ABNORMAL_YN']
            patient_data[f'psa_{i+1}_value'] = row['ORD_NUM_VALUE']
            patient_data[f'psa_{i+1}_unit'] = row['REFERENCE_UNIT']
            patient_data[f'psa_{i+1}_days'] = row['days_from_first_visit']
        psa = psa[psa['REFERENCE_UNIT'] == 'ng/mL']
        if len(psa) >= 2:
            patient_data['psa_recent_increase_percent'] = ((psa['ORD_NUM_VALUE'].iloc[0] - psa['ORD_NUM_VALUE'].iloc[1]) / psa['ORD_NUM_VALUE'].iloc[1]) * 100
        else:
            patient_data['psa_recent_increase_percent'] = None  # Handle cases with less than 2 PSA values
        patient_data['all_orders'] = '; '.join(group['DESCRIPTION'].astype(str).tolist())

        return pd.Series(patient_data)
    
    
def get_env_project_dataset(env) :
    dev_list = ['development','d','dev', 'n']
    prod_list = ['production','p','prod']
    if env.lower() in dev_list:
        return "ml-mps-adl-intudpcl-phi-d-f9bc", "d"
    elif env.lower() in prod_list:
        return "ml-mps-adl-intudpcl-phi-p-deec", "p"
    else:
        return "",""

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
    client = bigquery.Client()
    proj,dataset = get_env_project_dataset(env)
    if not proj or not dataset:
        raise ValueError(f"project or dataset is not set.  Check the env parameter to ensure it is correct.  Should be d or p.  Your value was {env}")
        
    
    query = f"""
    SELECT PAT.PAT_ID, PAT.PAT_MRN_ID, O.ORDER_TIME, O.DISPLAY_NAME, O.DESCRIPTION,
    O.RESULT_TIME, O2.ORD_NUM_VALUE, O2.REFERENCE_UNIT, O2.RESULT_STATUS_C, O2.RESULT_FLAG_C, O.ABNORMAL_YN,
    DATE_DIFF(O.RESULT_TIME, O.ORDER_TIME, day) as Result_datediff,
    EAP.PROC_CODE, EAP.PROC_ID, EAP.PROC_NAME, EAP.PROC_CAT_ID
    FROM 
    `{proj}.phi_patient_us_{dataset}.PATIENT` pat
    inner join `{proj}.phi_clarity_us_{dataset}.ORDER_PROC` O on pat.PAT_ID = O.PAT_ID
    LEFT JOIN `{proj}.phi_clarity_us_{dataset}.ORDER_RESULTS` O2 ON O2.ORDER_PROC_ID = O.ORDER_PROC_ID AND O2.LINE = 1
    LEFT JOIN `{proj}.phi_clarity_us_{dataset}.CLARITY_EAP` EAP ON EAP.PROC_ID = O.PROC_ID
    WHERE
    O.ORDER_TIME <= '{call_in_date.strftime('%Y-%m-%d')}'
    AND O.ORDER_STATUS_C <> 4 --5 - Completed
    and EAP.PROC_CODE in ('GU25', 'IIMS10506', 'IMG13002', 'IMG2572', 'IMG3785', 'IMG3793', 'IMG3856', 'IMG401', 'IMG4501', 'IMG573', 'IMG794', 'LAB103632', 'LAB103858', 'LAB105842', 'LAB116', 'LAB124', 'LAB171', 'LAB173', 'LAB578', 'LAB8014', 'NUR374', 'RAD800', 'URO11', 'URO14', 'URO16', 'URO178', 'URO179', 'URO19', 'URO198', 'URO22', 'URO23', 'URO40', 'URO5', 'URO56', 'URO58', 'URO61', 'URO62')
    and PAT.PAT_MRN_ID = '{mcn}'
    """

    # Run the query and get the results
    print(query)
    query_job = client.query(query)
    
    results = query_job.result()
    df = results.to_dataframe()
    df = df.replace({np.nan: None})
    
    #Apply the processing function to each patient group
    final_df = df.groupby('PAT_MRN_ID').apply(process_patient_group, call_in_date=call_in_date)
    if len(final_df)==1:
        final_df = final_df.drop(columns='PAT_MRN_ID').reset_index()
    else:
        final_df = final_df.unstack().drop(columns='PAT_MRN_ID').reset_index()

    patlist = final_df.to_dict(orient='records')
    return patlist