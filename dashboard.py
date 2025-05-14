import streamlit as st
import pandas as pd
import io
import tempfile
from simpledbf import Dbf5
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import time
import streamlit as st
import pandas as pd
from simpledbf import Dbf5
from datetime import datetime
import os
from google.oauth2 import service_account
from gspread_pandas import Spread
from dbfread import DBF
import pandas as pd
import io
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
from googleapiclient.http import MediaIoBaseUpload


# Period mapping
periode_mapping = {
    'Bulanan': 'Periode',
    'Triwulanan': 'triwulan',
    'Caturwulan': 'caturwulan',
    'Semesteran': 'semester',
    'Tahunan': 'tahun'
}

status_data_mapping = {
    'Angka Sementara':1, 
    'Angka Tetap':0
}

nilai_bobot_impor = {
    'Nilai': 'Nilai',
    'Bobot': 'BOBOT'
}

nilai_bobot_ekspor = {
    'Nilai': 'FOB',
    'Bobot': 'NETTO'
}

# --- Settingan ---
PASSWORD = st.secrets["password"]["password"]
GDRIVE_FOLDER_ID = st.secrets["GDRIVE_FOLDER_ID"]["GDRIVE_FOLDER_ID"]
SpreadSheet_ID = st.secrets["SpreadSheet_ID"]["SpreadSheet_ID"]
sheet_name = st.secrets["sheet_name"]["sheet_name"]


# Helper functions for period calculations
def get_triwulan(periode):
    bulan, tahun = periode.split('-')
    bulan = int(bulan)
    if bulan in [1, 2, 3]:
        return f'01-{tahun}'  # Q1
    elif bulan in [4, 5, 6]:
        return f'02-{tahun}'  # Q2
    elif bulan in [7, 8, 9]:
        return f'03-{tahun}'  # Q3
    return f'04-{tahun}'  # Q4

def get_tahun(periode):
    return periode.split('-')[1]

def get_caturwulan(periode):
    bulan, tahun = periode.split('-')
    bulan = int(bulan)
    if bulan in [1, 2, 3, 4]:
        return f'01-{tahun}'
    elif bulan in [5, 6, 7, 8]:
        return f'02-{tahun}'
    return f'03-{tahun}'

def get_semester(periode):
    bulan, tahun = periode.split('-')
    bulan = int(bulan)
    if bulan in [1, 2, 3, 4, 5, 6]:
        return f'01-{tahun}'
    return f'02-{tahun}'

# Load master files (cached)
# @st.cache_data
def load_impor_master_data():
    # st.write("Loading master data...")
    master_hs_full = pd.read_excel('impor_dict/master_hs_2022.xlsx', dtype=str)
    master_hs2_digit = pd.read_excel('impor_dict/Master 2HS New.xlsx', dtype=str)
    master_negara = pd.read_excel('impor_dict/Master Negara.xlsx', dtype=str)
    master_bec = pd.read_excel('impor_dict/Master BEC.xlsx', dtype=str)
    master_pelabuhan = pd.read_excel('pelabuhan.xlsx', dtype=str)
    konkordansi = pd.read_excel('ekspor_dict/konkordansi_2017_2022.xlsx', dtype=str)
    return master_hs_full, master_hs2_digit, master_negara, master_bec, master_pelabuhan, konkordansi

def load_ekspor_master_data():
    # st.write("Loading master data...")
    master_hs_full = pd.read_excel('ekspor_dict/master_hs_2022.xlsx', dtype=str)
    master_hs2_digit = pd.read_excel('ekspor_dict/master HS2_ind_terbaru.xlsx', dtype=str)
    master_negara = pd.read_excel('ekspor_dict/Master Negara.xlsx', dtype=str)
    konkordansi = pd.read_excel('ekspor_dict/konkordansi_2017_2022.xlsx', dtype=str)
    master_pelabuhan = pd.read_excel('pelabuhan.xlsx', dtype=str)
    return master_hs_full, master_hs2_digit, master_negara, konkordansi, master_pelabuhan
# Setup Google Drive API (cached resource)
@st.cache_resource
def setup_drive_service():
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

    credentials_info = {
        "type": st.secrets["google"]["type"],
        "project_id": st.secrets["google"]["project_id"],
        "private_key_id": st.secrets["google"]["private_key_id"],
        "private_key": st.secrets["google"]["private_key"],
        "client_email": st.secrets["google"]["client_email"],
        "client_id": st.secrets["google"]["client_id"],
        "auth_uri": st.secrets["google"]["auth_uri"],
        "token_uri": st.secrets["google"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["google"]["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["google"]["client_x509_cert_url"],
        "universe_domain": st.secrets["google"]["universe_domain"]
    }

    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=SCOPES
    )
    return build('drive', 'v3', credentials=credentials)
# def setup_drive_service():
#     # st.write("Setting up Google Drive service...")
#     SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
#     credentials_info = {
#     "type": st.secrets["google"]["type"],
#     "project_id": st.secrets["google"]["project_id"],
#     "private_key_id": st.secrets["google"]["private_key_id"],
#     "private_key": st.secrets["google"]["private_key"],
#     "client_email": st.secrets["google"]["client_email"],
#     "client_id": st.secrets["google"]["client_id"],
#     "auth_uri" : st.secrets["google"]["auth_uri"],
#     "token_uri" : st.secrets["google"]["token_uri"],
#     "auth_provider_x509_cert_url" : st.secrets["google"]["auth_provider_x509_cert_url"],
#     "client_x509_cert_url" : st.secrets["google"]["client_x509_cert_url"],
#     "universe_domain" : st.secrets["google"]["universe_domain"]
#     }
#     # SERVICE_ACCOUNT_FILE = 'cred2.json'
    
#     # credentials = service_account.Credentials.from_service_account_file(
#     #     credentials_info, scopes=SCOPES)
#     credentials = service_account.Credentials.from_service_account_info(
#     credentials_info, scopes=SCOPES)

#     return build('drive', 'v3', credentials=credentials)

# List .dbf file IDs from folder (cached)
# @st.cache_data
def get_dbf_file_ids(_drive_service, folder_id, prefix):
    st.write("Fetching DBF file IDs...")
    results = _drive_service.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        pageSize=1000,
        fields="files(id, name)"
    ).execute()
    items = results.get('files', [])
    target_files = [f for f in items if f['name'].startswith(prefix) and f['name'].lower().endswith('.dbf')]
    return [(f['id'], f['name']) for f in target_files]

# Download and process DBF files (cached)
# @st.cache_data(show_spinner=False)
def load_and_process_dbf(file_ids, _drive_service):
    st.write("Processing DBF files...")
    dfs = []
    for file_id, file_name in file_ids:
        request = _drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.dbf') as temp_file:
            temp_file.write(fh.read())
            temp_path = temp_file.name

        dbf = Dbf5(temp_path)
        df = dbf.to_dataframe()

        relevant_columns = [col for col in df.columns if (col.startswith('B0') or col.startswith('B1')) and len(col) == 5]
        if len(relevant_columns) > 1:
            transformed_dfs = []
            for col in relevant_columns:
                n_col = 'N' + col[1:]
                if n_col not in df.columns:
                    continue
                temp_df = df[['TIPE', 'JENIS', 'HS', 'K_PELB', 'NM_PELABUH',
                              'K_NEGARA', 'NM_NEGARA', 'NEG_ASAL', 'PROV_KPPBC', 'NM_PROV_BC',
                              col, n_col]].copy()
                temp_df.rename(columns={col: 'BOBOT', n_col: 'Nilai'}, inplace=True)
                temp_df['Periode'] = col[1:3] + '-' + '20' + col[3:5]
                transformed_dfs.append(temp_df)
            final_df = pd.concat(transformed_dfs)
        elif len(relevant_columns) == 1:
            col = relevant_columns[0]
            n_col = 'N' + col[1:]
            if n_col not in df.columns:
                continue
            df['BOBOT'] = df[col]
            df['Nilai'] = df[n_col]
            df['Periode'] = col[1:3] + '-' + '20' + col[3:5]
            final_df = df[['TIPE', 'JENIS', 'HS', 'K_PELB', 'NM_PELABUH',
                           'K_NEGARA', 'NM_NEGARA', 'NEG_ASAL', 'PROV_KPPBC', 'NM_PROV_BC',
                           'BOBOT', 'Nilai', 'Periode']]
        else:
            continue
        dfs.append(final_df)
    return pd.concat(dfs, ignore_index=True)

# @st.cache_data(show_spinner=True)
def get_impor_data(file_name):
    master_hs_full, master_hs2_digit, master_negara, master_bec, master_pelabuhan, konkordansi = load_impor_master_data()
    drive_service = setup_drive_service()

    # (Opsional) Filter folder tertentu juga:
    # file_query = f"name = '{file_name}' and '{FOLDER_ID}' in parents"
    file_query = f"name = '{file_name}'"

    # Cari file berdasarkan nama
    results = drive_service.files().list(
        q=file_query,
        spaces='drive',
        fields='files(id, name)',
        pageSize=1
    ).execute()
    items = results.get('files', [])
    file_id = items[0]['id']
    # print("Mengunduh file dengan ID:", file_id)

    # Download file
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)
    filtered_df = pd.read_parquet(fh)

    master_join = master_hs_full.merge(
        master_hs2_digit,
        left_on='HS2_2022',
        right_on='Kode HS 2 digit',
        how='left'
    )[['Kode HS 2 digit', 'HS_2022', 'Deskripsi', 'Description', 'OILGRDESC',
       'SECDESC', 'BRSDESC', 'COMGRPDESC', 'COMMDESC', 'BEC']]
    

    ##KONKORDANSI
    # Ambil df untuk tahun < 2020
    df_pre2020 = filtered_df[filtered_df['Periode'].str.contains(r'-2020$')].copy()
    mask = filtered_df['Periode'].str.contains(r'-2020$')
    # st.dataframe(df_pre2020.head(1000))

    # Lakukan merge konkordansi dan ubah HS
    df_pre2020 = df_pre2020.merge(konkordansi, how='left', left_on='HS', right_on='HS2017')
    df_pre2020['HS'] = df_pre2020['HS2022']
    df_pre2020.drop(columns=['HS2017', 'HS2022'], inplace=True)
    # st.dataframe(df_pre2020.head(1000))

    # Sekarang replace di impor_jakarta
    # filtered_df.update(df_pre2020)
    filtered_df.loc[mask, df_pre2020.columns] = df_pre2020.values
    
    negara_impor = filtered_df.merge(master_negara, left_on='K_NEGARA', right_on='KodeAngka', how='left')
    negara_join = negara_impor.merge(master_join, left_on='HS', right_on='HS_2022', how='left')
    negara_join['BENUA'] = negara_join['BENUA'].str.upper()
    negara_join['triwulan'] = negara_join['Periode'].apply(get_triwulan)
    negara_join['semester'] = negara_join['Periode'].apply(get_semester)
    negara_join['caturwulan'] = negara_join['Periode'].apply(get_caturwulan)
    negara_join['tahun'] = negara_join['Periode'].apply(get_tahun)



    impor_all_data = negara_join.merge(master_bec, left_on='BEC', right_on='KODE BEC', how='left')
    uncleaned_impor =  impor_all_data.merge(master_pelabuhan, left_on='PORT CODE', right_on='PORT CODE', how='left')
    # if 'Tahun' in uncleaned_impor.columns:
    #     drop_var = ['Tahun']
    #     impor_data = uncleaned_impor.drop(columns=drop_var)
    impor_data = uncleaned_impor.copy()
    impor_data.drop(columns=['KODEHURUF','HS'], inplace=True)
    return impor_data

# st.cache_data(show_spinner=True)
def get_ekspor_data(file_name):
    master_hs_full, master_hs2_digit, master_negara, konkordansi, master_pelabuhan = load_ekspor_master_data()
    drive_service = setup_drive_service()

    # (Opsional) Filter folder tertentu juga:
    # file_query = f"name = '{file_name}' and '{FOLDER_ID}' in parents"
    file_query = f"name = '{file_name}'"

    # Cari file berdasarkan nama
    results = drive_service.files().list(
        q=file_query,
        spaces='drive',
        fields='files(id, name)',
        pageSize=1
    ).execute()
    items = results.get('files', [])
    file_id = items[0]['id']
    # print("Mengunduh file dengan ID:", file_id)

    # Download file
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)
    filtered_df = pd.read_parquet(fh)

    master_join = master_hs_full.merge(master_hs2_digit, left_on='HS2_2022', right_on='hs2', how='left')[['hs2', 'HS_2022', 'descind','descrip','OILGRDESC','SECDESC','BRSDESC','COMGRPDESC','COMMDESC']] 
    negara_expor = filtered_df.merge(master_negara, left_on='NEGARA', right_on='KodeAngka', how='left')
    negara_expor.rename(columns = {'NEGARA_x':'kdnegara'}, inplace = True)
    negara_expor.rename(columns = {'NEGARA_y':'NEGARA'}, inplace = True)

    negara_join = negara_expor.merge(master_join, left_on='KODE_HS', right_on='HS_2022', how='left')
    negara_join['BENUA'] = negara_join['BENUA'].str.upper()

    negara_join.loc[negara_join['OILGRDESC'] == 'MIGAS', 'SECDESC'] = 'MIGAS'

    negara_join.loc[(negara_join['SECDESC'].isin(['PERTAMBANGAN', 'LAINNYA']) & (negara_join['OILGRDESC'] == 'NON MIGAS')), 'SECDESC'] = 'PERTAMBANGAN DAN LAINNYA'
    ekspor_data_all = negara_join.merge(master_pelabuhan, left_on='PORT CODE', right_on='PORT CODE', how='left')
    ekspor_data_all['triwulan'] = ekspor_data_all['Periode'].apply(get_triwulan)
    ekspor_data_all['semester'] = ekspor_data_all['Periode'].apply(get_semester)
    ekspor_data_all['caturwulan'] = ekspor_data_all['Periode'].apply(get_caturwulan)
    ekspor_data_all['tahun'] = ekspor_data_all['Periode'].apply(get_tahun)
    ekspor_data_all.drop(columns=['KODE_HS'], inplace=True)
    return ekspor_data_all


def process_ekspor_dbf(file):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.dbf') as tmp_file:
        tmp_file.write(file.read())
        tmp_file_path = tmp_file.name

    # Simpledbf butuh path
    dbf = Dbf5(tmp_file_path)
    df = dbf.to_dataframe()
    master_hs_full, master_hs2_digit, master_negara, konkordansi, master_pelabuhan = load_ekspor_master_data()
    st.dataframe(master_pelabuhan)
    if 'THN_PROSES' not in df.columns:
        df = df.rename(columns={'MTH': 'BLN_PROSES', 'YEAR': 'THN_PROSES'})
        if df['THN_PROSES'].astype(str).str.contains('2025').any():
            df = df.rename(columns={'NEWCTRYCOD': 'NEGARA', 'PODAL5': 'PELABUHAN'})
    else:
        if df['THN_PROSES'].astype(str).str.contains('2022').any():
            df = df.rename(columns={'HS_BTKI22': 'KODE_HS', 'NEWCTRYCOD': 'NEGARA', 'PODAL5': 'PELABUHAN'})
        elif df['THN_PROSES'].astype(str).str.contains('2021').any():
            df = df.rename(columns={'BTKI2022': 'KODE_HS', 'NEWCTRYCOD': 'NEGARA', 'PODAL5': 'PELABUHAN'})
        elif df['THN_PROSES'].astype(str).str.contains('2020').any():
            df = df.merge(konkordansi, left_on='KODE_HS', right_on='HS2017', how='left')[['BLN_PROSES', 'THN_PROSES', 'PROVPOD','HS2022','NEGARA','NETTO','FOB','PROVORIG','PELABUHAN']] 
            df = df.rename(columns={'HS2022': 'KODE_HS'})
        elif df['THN_PROSES'].astype(str).str.isnumeric().all():
            tahun = df['THN_PROSES'].astype(int)
            if ((tahun > 2017) & (tahun < 2020)).any():
                df = df.merge(konkordansi, left_on='KODE_HS', right_on='HS2017', how='left')[['BLN_PROSES', 'THN_PROSES', 'PROVPOD','HS2022','NEWCTRYCOD','NETTO','FOB','PODAL5']] 
                df = df.rename(columns={'HS2022': 'KODE_HS', 'NEWCTRYCOD': 'NEGARA', 'PODAL5': 'PELABUHAN'})
            elif (tahun < 2018).any():
                df = df.merge(konkordansi, left_on='KODE_HS', right_on='HS2017', how='left')[['BLN_PROSES', 'THN_PROSES', 'PROVPOD','HS2022','NEGARA','NETTO','FOB','PELABUHAN']] 
                df = df.rename(columns={'HS2022': 'KODE_HS'})

    ekspor_jakarta = df[['BLN_PROSES', 'THN_PROSES', 'PELABUHAN', 'KODE_HS', 'NEGARA','NETTO', 'FOB']]
    # ekspor_jakarta['PORT CODE'] = ekspor_jakarta['PELABUHAN'].astype(str).str[-3:]
    # ekspor_jakarta['PROV PORT'] = ekspor_jakarta['PELABUHAN'].astype(str).str[:2]
    ekspor_jakarta['Periode'] = ekspor_jakarta['BLN_PROSES'].astype(str) + '-' + ekspor_jakarta['THN_PROSES'].astype(str).str.zfill(2)
    ekspor_jakarta.drop(columns=['BLN_PROSES','THN_PROSES'], inplace=True)
    ekspor_jakarta.rename(columns={'PELABUHAN': 'PORT CODE'}, inplace=True)
    master_join = master_hs_full.merge(master_hs2_digit, left_on='HS2_2022', right_on='hs2', how='left')[['hs2', 'HS_2022', 'descind','descrip','OILGRDESC','SECDESC','BRSDESC','COMGRPDESC','COMMDESC']] 

    negara_expor = ekspor_jakarta.merge(master_negara, left_on='NEGARA', right_on='KodeAngka', how='left')
    negara_expor.rename(columns = {'NEGARA_x':'kdnegara'}, inplace = True)
    negara_expor.rename(columns = {'NEGARA_y':'NEGARA'}, inplace = True)

    negara_join = negara_expor.merge(master_join, left_on='KODE_HS', right_on='HS_2022', how='left')
    negara_join['BENUA'] = negara_join['BENUA'].str.upper()

    negara_join.loc[negara_join['OILGRDESC'] == 'MIGAS', 'SECDESC'] = 'MIGAS'

    negara_join.loc[(negara_join['SECDESC'].isin(['PERTAMBANGAN', 'LAINNYA']) & (negara_join['OILGRDESC'] == 'NON MIGAS')), 'SECDESC'] = 'PERTAMBANGAN DAN LAINNYA'
    ekspor_data_all = negara_join.merge(master_pelabuhan, left_on='PORT CODE', right_on='PORT CODE', how='left')
    ekspor_data_all['triwulan'] = ekspor_data_all['Periode'].apply(get_triwulan)
    ekspor_data_all['semester'] = ekspor_data_all['Periode'].apply(get_semester)
    ekspor_data_all['caturwulan'] = ekspor_data_all['Periode'].apply(get_caturwulan)
    ekspor_data_all['tahun'] = ekspor_data_all['Periode'].apply(get_tahun)
    ekspor_data_all.drop(columns=['KODE_HS'], inplace=True)
    check_null_values(ekspor_data_all)

    return ekspor_data_all



# --- Fungsi Proses DBF ---
def process_impor_dbf(file):
    # Buat file temporary
    with tempfile.NamedTemporaryFile(delete=False, suffix='.dbf') as tmp_file:
        tmp_file.write(file.read())
        tmp_file_path = tmp_file.name

    # Simpledbf butuh path
    dbf = Dbf5(tmp_file_path)
    df = dbf.to_dataframe()
    master_hs_full, master_hs2_digit, master_negara, master_bec, master_pelabuhan, konkordansi = load_impor_master_data()

    relevant_columns = [col for col in df.columns if (col.startswith('B0') or col.startswith('B1')) and len(col) == 5]

    if len(relevant_columns) > 1:
        transformed_dfs = []
        for col in relevant_columns:
            n_col = 'N' + col[1:]
            if 'HS' not in df.columns and 'HS2022' in df.columns:
                df.rename(columns={'HS2022': 'HS'}, inplace=True)

            temp_df = df[['TIPE', 'JENIS', 'HS', 'K_PELB' 'NM_PELABUH',
                          'K_NEGARA', 'NM_NEGARA', 'NEG_ASAL', col, n_col]].copy()
            temp_df.rename(columns={col: 'BOBOT', n_col: 'Nilai'}, inplace=True)
            temp_df['Periode'] = col[1:3] + '-' + '20' + col[3:5]
            transformed_dfs.append(temp_df)
        final_df = pd.concat(transformed_dfs)
    elif len(relevant_columns) == 1:
        col = relevant_columns[0]
        n_col = 'N' + col[1:]
        df['BOBOT'] = df[col]
        df['Nilai'] = df[n_col]
        df['Periode'] = col[1:3] + '-' + '20' + col[3:5]
        final_df = df[['TIPE', 'JENIS', 'HS', 'K_PELB', 'NM_PELABUH',
                       'K_NEGARA', 'NM_NEGARA', 'NEG_ASAL', 'BOBOT', 'Nilai', 'Periode']]
    else:
        relevant_columns = [col for col in df.columns if (col.startswith('B0') or col.startswith('B1'))]
        col = relevant_columns[0]
        n_col = 'N' + col[1:]
        df['BOBOT'] = df[col]
        df['Nilai'] = df[n_col]
        df['Periode'] = col[1:3] + '-' + '20' + col[3:5]
        final_df = df[['TIPE', 'JENIS', 'HS', 'K_PELB', 'NM_PELABUH',
                       'K_NEGARA', 'NM_NEGARA', 'NEG_ASAL', 'BOBOT', 'Nilai', 'Periode']]
    res_df = final_df[(final_df['BOBOT'] != 0) & (final_df['Nilai'] != 0)]

    drop_var = ['NEG_ASAL','NM_NEGARA','NM_PELABUH','NEG_ASAL']
    res_df = res_df.drop(columns=drop_var)
    if res_df['Periode'].str.contains(r'-2020$').any():
        # Ambil df untuk tahun < 2020
        df_pre2020 = res_df[res_df['Periode'].str.contains(r'-2020$')].copy()
        mask = res_df['Periode'].str.contains(r'-2020$')

        # Lakukan merge konkordansi dan ubah HS
        df_pre2020 = df_pre2020.merge(konkordansi, how='left', left_on='HS', right_on='HS2017')
        df_pre2020['HS'] = df_pre2020['HS2022']
        df_pre2020.drop(columns=['HS2017', 'HS2022'], inplace=True)

        # Update hanya baris yang sesuai mask
        df_pre2020.reset_index(drop=True, inplace=True)
        res_df.loc[mask, df_pre2020.columns] = df_pre2020.values

    # res_df['Tahun'] = res_df['Periode'].str[-4:].astype(int)
    # res_df['PORT CODE'] = res_df['K_PELB'].astype(str).str[-3:]
    # res_df['PROV PORT'] = res_df['K_PELB'].astype(str).str[:2]
    # res_df.drop(columns=['Tahun'], inplace=True)

    res_df.rename(columns={'K_PELB': 'PORT CODE'}, inplace=True)
    master_join = master_hs_full.merge(
        master_hs2_digit,
        left_on='HS2_2022',
        right_on='Kode HS 2 digit',
        how='left'
    )[['Kode HS 2 digit', 'HS_2022', 'Deskripsi', 'Description', 'OILGRDESC',
       'SECDESC', 'BRSDESC', 'COMGRPDESC', 'COMMDESC', 'BEC']]

    negara_impor = res_df.merge(master_negara, left_on='K_NEGARA', right_on='KodeAngka', how='left')
    negara_join = negara_impor.merge(master_join, left_on='HS', right_on='HS_2022', how='left')
    negara_join['BENUA'] = negara_join['BENUA'].str.upper()
    negara_join['triwulan'] = negara_join['Periode'].apply(get_triwulan)
    negara_join['semester'] = negara_join['Periode'].apply(get_semester)
    negara_join['caturwulan'] = negara_join['Periode'].apply(get_caturwulan)
    negara_join['tahun'] = negara_join['Periode'].apply(get_tahun)

    impor_all_data = negara_join.merge(master_bec, left_on='BEC', right_on='KODE BEC', how='left')
    uncleaned_impor =  impor_all_data.merge(master_pelabuhan, left_on='PORT CODE', right_on='PORT CODE', how='left')
    # if 'Tahun' in uncleaned_impor.columns:
    #     drop_var = ['Tahun']
    #     impor_data = uncleaned_impor.drop(columns=drop_var)
    drop_var = ['HS','KODEHURUF']
    impor_data = uncleaned_impor.drop(columns=drop_var)
    # check_null_values(impor_data)
    return impor_data


def check_null_values(df):
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.heatmap(df.isnull(), cbar=False, cmap='viridis', yticklabels=False, ax=ax)
    ax.set_title("Visualisasi Nilai Null dalam DataFrame")
    # Tampilkan plot di Streamlit
    st.pyplot(fig)

# --- Fungsi Google Drive ---


def delete_and_upload_new_parquet(new_df, PARQUET_FILENAME):
    # --- Siapkan DataFrame yang akan di-upload ---
    if PARQUET_FILENAME in ("impor_jakarta.parquet", "impor_melalui_jakarta.parquet"):
        if 'HS' not in new_df.columns and 'HS_2022' in new_df.columns:
            new_df.rename(columns={'HS_2022': 'HS'}, inplace=True)
        uploaded_df = new_df[['TIPE', 'JENIS', 'HS', 'K_NEGARA', 'BOBOT', 'Nilai', 'Periode', 'PORT CODE']]
    else:
        uploaded_df = new_df[['KODE_HS', 'NEGARA', 'NETTO', 'FOB', 'PORT CODE', 'PROV PORT', 'Periode']]

    # --- Setup Google Drive API ---
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds_info = { k: st.secrets["google"][k] for k in (
        "type","project_id","private_key_id","private_key",
        "client_email","client_id","auth_uri","token_uri",
        "auth_provider_x509_cert_url","client_x509_cert_url","universe_domain"
    )}
    credentials = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)

    # --- Hapus file lama di Drive (jika ada) ---
    try:
        query = f"name='{PARQUET_FILENAME}' and '{GDRIVE_FOLDER_ID}' in parents"
        resp = service.files().list(q=query, spaces='drive').execute()
        for f in resp.get('files', []):
            service.files().delete(fileId=f['id']).execute()
    except Exception as e:
        st.error(f"Gagal menghapus file lama di Drive: {e}")
        return

    # --- Tulis DataFrame ke in-memory buffer Parquet ---
    try:
        buf = io.BytesIO()
        uploaded_df.to_parquet(buf, index=False)
        buf.seek(0)
    except Exception as e:
        st.error(f"Gagal menulis Parquet ke buffer: {e}")
        return

    # --- Upload buffer ke Drive ---
    try:
        metadata = {'name': PARQUET_FILENAME, 'parents': [GDRIVE_FOLDER_ID]}
        media = MediaIoBaseUpload(buf, mimetype='application/octet-stream')
        service.files().create(body=metadata, media_body=media, fields='id').execute()
        st.success(f"Berhasil upload {PARQUET_FILENAME} ke Google Drive.")
    except Exception as e:
        st.error(f"Gagal upload buffer ke Drive: {e}")
        return

    # (opsional) beri waktu Drive selesai memproses sebelum UI lanjut
    time.sleep(2)


# def delete_and_upload_new_parquet(new_df, PARQUET_FILENAME):
#     if PARQUET_FILENAME=="impor_jakarta.parquet" or PARQUET_FILENAME=="impor_melalui_jakarta.parquet":
#         if 'HS' not in new_df.columns and 'HS_2022' in new_df.columns:
#             new_df.rename(columns={'HS_2022': 'HS'}, inplace=True)

#         uploaded_df = new_df[['TIPE', 'JENIS', 'HS', 'K_NEGARA', 'BOBOT', 'Nilai','Periode', 'PORT CODE']]
#     else:
#         uploaded_df = new_df[['KODE_HS','NEGARA','NETTO','FOB','PORT CODE','PROV PORT','Periode']]
        
#     SCOPES = ['https://www.googleapis.com/auth/drive']
#     # SERVICE_ACCOUNT_FILE = 'cred2.json'
#     # credentials = service_account.Credentials.from_service_account_file(
#     #     SERVICE_ACCOUNT_FILE, scopes=SCOPES)
#     credentials_info = {
#         "type": st.secrets["google"]["type"],
#         "project_id": st.secrets["google"]["project_id"],
#         "private_key_id": st.secrets["google"]["private_key_id"],
#         "private_key": st.secrets["google"]["private_key"],
#         "client_email": st.secrets["google"]["client_email"],
#         "client_id": st.secrets["google"]["client_id"],
#         "auth_uri": st.secrets["google"]["auth_uri"],
#         "token_uri": st.secrets["google"]["token_uri"],
#         "auth_provider_x509_cert_url": st.secrets["google"]["auth_provider_x509_cert_url"],
#         "client_x509_cert_url": st.secrets["google"]["client_x509_cert_url"],
#         "universe_domain": st.secrets["google"]["universe_domain"]
#     }

#     credentials = service_account.Credentials.from_service_account_info(
#         credentials_info, scopes=SCOPES
#     )
#     service = build('drive', 'v3', credentials=credentials)

#     # Cari file lama
#     query = f"name='{PARQUET_FILENAME}' and '{GDRIVE_FOLDER_ID}' in parents"
#     response = service.files().list(q=query, spaces='drive').execute()
#     files = response.get('files', [])

#     # Hapus file lama
#     for file in files:
#         service.files().delete(fileId=file['id']).execute()

#     # Simpan ke file parquet lokal sementara
#     with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
#         tmp_parquet = tmp.name
#         uploaded_df.to_parquet(tmp_parquet, index=False)

#     # Upload file baru
#     file_metadata = {
#         'name': PARQUET_FILENAME,
#         'parents': [GDRIVE_FOLDER_ID]
#     }
#     media = MediaFileUpload(tmp_parquet, mimetype='application/octet-stream')
#     service.files().create(body=file_metadata, media_body=media, fields='id').execute()
#     # Tunggu sebentar untuk memberi waktu proses selesai
#     time.sleep(5)  # Tunggu 1 detik, bisa diubah sesuai kebutuhan
    
#     # Hapus file setelah selesai
#     try:
#         os.remove(tmp_parquet)
#         print(f"Temporary file {tmp_parquet} deleted.")
#     except PermissionError as e:
#         print(f"Error deleting file: {e}. The file might still be in use.")

# Untuk Google Drive

def update_google_sheet(jenis, periode, nama_file, is_asem, spreadsheet_id, sheet_name):
    # Setup credentials
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # SERVICE_ACCOUNT_FILE = 'cred2.json'

    # credentials = service_account.Credentials.from_service_account_file(
    #     SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    credentials_info = {
        "type": st.secrets["google"]["type"],
        "project_id": st.secrets["google"]["project_id"],
        "private_key_id": st.secrets["google"]["private_key_id"],
        "private_key": st.secrets["google"]["private_key"],
        "client_email": st.secrets["google"]["client_email"],
        "client_id": st.secrets["google"]["client_id"],
        "auth_uri": st.secrets["google"]["auth_uri"],
        "token_uri": st.secrets["google"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["google"]["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["google"]["client_x509_cert_url"],
        "universe_domain": st.secrets["google"]["universe_domain"]
    }

    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=SCOPES
    )
    service = build('sheets', 'v4', credentials=credentials)

    sheet = service.spreadsheets()
    
    # Baca semua data dari sheet
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    values = result.get('values', [])

    # Konversi ke DataFrame biar gampang
    if not values:
        df = pd.DataFrame(columns=["Jenis", "Nama File", "Periode", "is ASEM", "last update"])
    else:
        header = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=header)
    
    # Pastikan kolom 'last update' dalam format datetime
    # Pastikan kolom 'last update' dalam format string sebelum upload
    if 'last update' in df.columns:
        df['last update'] = df['last update'].astype(str)

    data_to_write = [df.columns.tolist()] + df.values.tolist()


    # Filter berdasarkan Jenis dan Periode
    matched = df[(df['Jenis'] == jenis) & (df['Periode'] == periode)]

    timestamp = pd.Timestamp.now()
    now_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")  # format last update

    if len(matched) == 0:
        # Tidak ditemukan, tambahkan baris baru
        new_row = {
            "Jenis": jenis,
            "Nama File": nama_file,
            "Periode": periode,
            "is ASEM": is_asem,
            "last update": now_str
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        # Kalau ada 2 atau lebih
        if len(matched) > 1:
            # Urutkan by last update
            matched = matched.sort_values(by="last update", ascending=False)
            newest_idx = matched.index[0]
            older_idxs = matched.index[1:]

            # Hapus older entries
            df = df.drop(older_idxs)

        else:
            newest_idx = matched.index[0]

        # Update baris
        df.loc[newest_idx, 'Nama File'] = nama_file
        df.loc[newest_idx, 'is ASEM'] = is_asem
        df.loc[newest_idx, 'last update'] = now_str

    # Write back ke Sheet (overwrite semua isi sheet)
    data_to_write = [df.columns.tolist()] + df.values.tolist()

    body = {
        'values': data_to_write
    }

    sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        valueInputOption='RAW',
        body=body
    ).execute()
    st.write("Google Sheet updated successfully.")
def make_choropleth(input_df, input_column, input_color_theme, target_period):
    # Filter untuk periode yang ditentukan
    filtered_df = input_df[input_df['Periode'] == target_period]
    
    # Group by untuk menghasilkan total per negara
    grouped_df = filtered_df.groupby('NEGARA', as_index=False).agg({input_column: 'sum'})  # Ubah 'sum' dengan fungsi agregasi lain jika perlu

    # Menghasilkan choropleth
    choropleth = px.choropleth(
        grouped_df, 
        locations='NEGARA',  # Gunakan kolom 'NEGARA' untuk lokasi
        locationmode='country names',  # Gunakan mode lokasi untuk nama negara
        color=input_column,
        color_continuous_scale=input_color_theme,
        range_color=(0, grouped_df[input_column].max()),  # Menggunakan kolom yang ditentukan
        labels={input_column: input_column}  # Menetapkan label sumbu warna
    )
    
    choropleth.update_layout(
        template='plotly_dark',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=350
    )
    return choropleth
def make_heatmap(input_df, input_y, input_x, input_color, input_color_theme):
    heatmap = alt.Chart(input_df).mark_rect().encode(
            y=alt.Y(
                f'{input_y}:O',
                sort=list(input_df[input_y].unique()),  # <--- penting untuk urut manual
                axis=alt.Axis(title="Periode", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)
            ),
            x=alt.X(
                f'{input_x}:O',
                axis=alt.Axis(title="", titleFontSize=18, titlePadding=15, titleFontWeight=900)
            ),
            color=alt.Color(f'max({input_color}):Q',
                             legend=None,
                             scale=alt.Scale(scheme=input_color_theme)),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.25),
        ).properties(width=900
        ).configure_axis(
        labelFontSize=12,
        titleFontSize=12
        ) 
    return heatmap


def describe_with_nulls(df):
    summary = pd.DataFrame({
        'Data Type': df.dtypes,
        'Non-Null Count': df.count(),
        'Null Count': df.isnull().sum(),
        'Unique Count': df.nunique()
    })
    summary['% Null'] = (summary['Null Count'] / len(df)) * 100
    return summary

st.set_page_config(
    page_title="Expor Impor Jakarta Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")
menu = st.sidebar.selectbox("Pilih Menu", ["Dashboard","Olah Data", "Upload Data"])
if 'impor_final' not in st.session_state:
    with st.spinner("Loading data..."):
        start_time = time.time()  # Mulai stopwatch

        file_name = 'impor_jakarta.parquet'
        # file_ekspor_name = 'ekspor_jakarta.parquet'
        st.session_state['impor_final'] = get_impor_data(file_name)
        # st.session_state['ekspor_final'] = get_ekspor_data(file_ekspor_name)

        # end_time = time.time()  # Stop stopwatch
        # duration = end_time - start_time  # Hitung durasi

        # st.success(f"Data berhasil dimuat dalam {duration:.2f} detik.")

if menu == "Dashboard":

    st.title('📦 Dashboard Ekspor Impor')

    # Create columns for the options
    option_col1, option_col2 = st.columns(2)

    with option_col1:
        # Year selection with card-like styling
        with st.container(border=True):
            st.markdown("**Pilih Jenis Dashboard**")
            dataset_dash = ["Impor Jakarta","Ekspor Jakarta","Impor Melalui Jakarta","Ekspor Melalui Jakarta"]
            dataset_dash_selected = st.selectbox('', dataset_dash, label_visibility='collapsed')
    if dataset_dash_selected == 'Impor Jakarta':
        if 'impor_final' not in st.session_state:
            st.session_state['impor_final'] = get_impor_data('impor_jakarta.parquet')
        data_olah_dashboard = st.session_state['impor_final']
        if 'HS' in data_olah_dashboard.columns:
            data_olah_dashboard.drop(columns=['HS'], inplace=True)

    elif dataset_dash_selected == 'Ekspor Jakarta':
        if 'ekspor_final' not in st.session_state:
            st.session_state['ekspor_final'] = get_ekspor_data('ekspor_jakarta.parquet')
        data_olah_dashboard = st.session_state['ekspor_final']

    elif dataset_dash_selected == 'Ekspor Melalui Jakarta':
        if 'ekspor_melalui' not in st.session_state:
            st.session_state['ekspor_melalui'] = get_ekspor_data('ekspor_melalui_jakarta.parquet')
        data_olah_dashboard = st.session_state['ekspor_melalui']

    else:
        if 'impor_melalui' not in st.session_state:
            st.session_state['impor_melalui'] = get_impor_data('impor_melalui_jakarta.parquet')
        data_olah_dashboard = st.session_state['impor_melalui']
        if 'HS' in data_olah_dashboard.columns:
            data_olah_dashboard.drop(columns=['HS'], inplace=True)

    # Extract list of unique years
    available_years_format = (
        pd.to_datetime(data_olah_dashboard['Periode'].unique(), format='%m-%Y')
        .sort_values(ascending=False)
        .strftime('%m-%Y')
    )

    # Pilih tahun
    with option_col2:
        with st.container(border=True):
            st.markdown("**Pilih Periode**")
            target_periode = st.selectbox(
                '',
                available_years_format,
                index=0,
                label_visibility='collapsed'
            )

    #######################
    # Dashboard Main Panel
    col = st.columns((1.5, 4.5, 2), gap='medium')
    # st.markdown("### Dashboard Impor")

    ## CHANGE THIS
    if dataset_dash_selected == 'Impor Jakarta':
        df = st.session_state['impor_final'].copy()
    elif dataset_dash_selected == 'Impor Melalui Jakarta':
        df = st.session_state['impor_melalui'].copy()
    elif dataset_dash_selected == 'Ekspor Jakarta':
        df = st.session_state['ekspor_final'].copy()
    else:
        df = st.session_state['ekspor_melalui'].copy()
    # df = st.session_state['impor_final'].copy()
    # df_selected = df[['Periode', 'BOBOT', 'Nilai']].copy()
    if df.columns.str.contains('Nilai', case=False).any() and df.columns.str.contains('BOBOT', case=False).any():
        # Masuk ke sini jika ADA kolom yang mengandung 'Nilai' DAN 'BOBOT'
        df_selected = df[['Periode', 'BOBOT', 'Nilai']].copy()
    else:
        df_selected = df[['Periode', 'NETTO','FOB']].copy()
        df_selected.rename(columns={'NETTO': 'BOBOT', 'FOB': 'Nilai'}, inplace=True)
        df.rename(columns={'NETTO': 'BOBOT', 'FOB': 'Nilai','hs2':'Kode HS 2 digit','descind':'Deskripsi'}, inplace=True)
    df_selected['Periode'] = pd.to_datetime(df_selected['Periode'], format='%m-%Y')

    # target_periode = '12-2024'


    with col[0]:
        st.markdown('#### Pertumbuhan')
        grouped = df_selected.groupby('Periode').sum().reset_index()
        selected_period = pd.to_datetime(target_periode, format='%m-%Y')

        # # Ambil nilai sekarang
        current_value = grouped.loc[grouped['Periode'] == selected_period, 'Nilai'].values[0]
        # st.write(current_value)
        current_value_juta = current_value / 1_000_000

        st.metric(label="Nilai (juta US$)", value=f"{current_value_juta:,.2f}")


        # Ambil nilai bulan sebelumnya
        previous_month = selected_period - pd.DateOffset(months=1)
        previous_value = grouped.loc[grouped['Periode'] == previous_month, 'Nilai']

        # Ambil nilai tahun sebelumnya
        previous_year = selected_period - pd.DateOffset(years=1)
        previous_year_value = grouped.loc[grouped['Periode'] == previous_year, 'Nilai']

        # Hitung MoM Growth
        if not previous_value.empty:
            mom_growth = (current_value - previous_value.values[0]) / previous_value.values[0] * 100
        else:
            mom_growth = None

        # Hitung YoY Growth
        if not previous_year_value.empty:
            yoy_growth = (current_value - previous_year_value.values[0]) / previous_year_value.values[0] * 100
        else:
            yoy_growth = None
            st.metric(label="Nilai Periode Sekarang", value=f"{current_value:,.0f}")

        if mom_growth is not None:
            st.metric(label="Growth M-to-M", value=f"{mom_growth:.2f}%")
            # st.metric(label="Growth M-to-M", value="-", delta=f"{mom_growth:.2f}%")
        else:
            st.metric(label="Growth M-to-M", value="Data tidak tersedia", delta="N/A")

        if yoy_growth is not None:
            st.metric(label="Growth Y-on-Y", value=f"{yoy_growth:.2f}%")
        else:
            st.metric(label="Growth Y-on-Y", value="Data tidak tersedia", delta="N/A")
    with col[1]:
        if dataset_dash_selected == 'Impor Jakarta':
            st.markdown('#### Negara Asal Import Jakarta')
        elif dataset_dash_selected == 'Ekspor Jakarta':
            st.markdown('#### Negara Tujuan Export Jakarta')
        elif dataset_dash_selected == 'Ekspor Melalui Jakarta':
            st.markdown('#### Negara Tujuan Export Melalui Jakarta')
        else:
            st.markdown('#### Negara Asal Import Melalui Jakarta')
        # st.write(', '.join(df.columns))

        choropleth_fig = make_choropleth(df, input_column='Nilai', input_color_theme='reds', target_period=target_periode)
        st.plotly_chart(choropleth_fig, use_container_width=True)
        # # Convert target_periode to datetime
        # str_target_periode = pd.to_datetime(target_periode, format='%m-%Y').strftime('%m-%Y')
        # Ini masih datetime, jangan .strftime dulu
        target_periode_dt = pd.to_datetime(target_periode, format='%m-%Y')

        # Baru tambah 1 bulan
        target_periode_plus_1 = target_periode_dt + pd.DateOffset(months=1)

        # Generate 12 bulan sampai target_periode_plus_1
        latest_12_months = pd.date_range(end=target_periode_plus_1, periods=12, freq='M').strftime('%m-%Y')

        # # 1. Buat kolom desc_hs yang menggabungkan Deskripsi dan Kode HS 2 digit
        df['desc_hs'] = ' [' + df['Kode HS 2 digit'].astype(str) + ']' + df['Deskripsi']

        # # 2. Konversi Periode ke datetime dan ekstrak bulan-tahun
        df['Periode_dt'] = pd.to_datetime(df['Periode'], format='%m-%Y', errors='coerce')

        # # 3. Ambil 12 bulan terakhir dari target_periode
        df_full_recent = df[df['Periode_dt'].dt.strftime('%m-%Y').isin(latest_12_months)]

        # # 4. Grouping data dan aggregasi
        heatmap_data = df_full_recent.groupby(['Periode', 'desc_hs'])['Nilai'].sum().reset_index()

        # 5. Urutkan berdasarkan periode terbaru
        heatmap_data['Periode_dt'] = pd.to_datetime(heatmap_data['Periode'], format='%m-%Y')
        heatmap_data = heatmap_data.sort_values('Periode_dt')

        # Reset index kalau perlu (opsional)
        heatmap_data = heatmap_data.reset_index(drop=True)

        # Baru setelah rapi, drop kolom Periode_dt
        heatmap_data.drop('Periode_dt', axis=1, inplace=True)


        # # 6. Buat heatmap
        heatmap = make_heatmap(
            input_df=heatmap_data,
            input_y='Periode',      # Periode di sumbu Y
            input_x='desc_hs',      # Deskripsi HS di sumbu X
            input_color='Nilai',    # Nilai perdagangan
            input_color_theme='reds'
        )
        st.altair_chart(heatmap, use_container_width=True)
        # data_group = impor_jakarta_full_recent.groupby(['Periode', 'Kode HS 2 digit'])['Nilai'].sum().reset_index()
        # str_target_periode = pd.to_datetime(target_periode, format='%m-%Y').strftime('%m-%Y')
        # data_fig = data_group[data_group['Periode'] == str_target_periode].copy()
        # data_fig_sorted = data_fig.sort_values(by='Nilai', ascending=False)
        # st.dataframe(impor_jakarta_full_recent)

    with col[2]:
        st.markdown('#### Top Komodities')
        # st.write(target_periode)

        data_group = df_full_recent.groupby(['Periode', 'Kode HS 2 digit'])['Nilai'].sum().reset_index()
        str_target_periode = pd.to_datetime(target_periode, format='%m-%Y').strftime('%m-%Y')
        data_fig = data_group[data_group['Periode'] == str_target_periode].copy()
        data_fig_sorted = data_fig.sort_values(by='Nilai', ascending=False)

        st.dataframe(data_fig_sorted,
            column_order=("Kode HS 2 digit", "Nilai"),
            hide_index=True,
            width=None,
            column_config={
                "Kode HS 2 digit": st.column_config.TextColumn(
                    "HS 2 Digit",
                ),
                "Nilai": st.column_config.ProgressColumn(
                    "Nilai",
                    format="%.2f",
                    min_value=0,
                    max_value=max(data_fig_sorted.Nilai),
                    )}
                )

elif menu == "Olah Data":
# Initialize session state
    st.session_state['olah_pressed'] = False

    # Title
    st.title("Tabulasi Ekspor-Impor DKI Jakarta")

    # Check if data is already in session state


    # UI components
    jenis_dataset = ['Impor Jakarta','Ekspor Jakarta','Impor Melalui Jakarta', 'Ekspor Melalui Jakarta']
    dataset_selected = st.selectbox(label='Jenis Data Diolah', options=jenis_dataset, index=0)

    # impor_final = st.session_state['impor_final']


    if dataset_selected == 'Impor Jakarta':
        if 'impor_final' not in st.session_state:
            st.session_state['impor_final'] = get_impor_data('impor_jakarta.parquet')
        data_olah = st.session_state['impor_final']
        if 'HS' in data_olah.columns:
            data_olah.drop(columns=['HS'], inplace=True)

    elif dataset_selected == 'Ekspor Jakarta':
        if 'ekspor_final' not in st.session_state:
            st.session_state['ekspor_final'] = get_ekspor_data('ekspor_jakarta.parquet')
        data_olah = st.session_state['ekspor_final']
        
    elif dataset_selected == 'Ekspor Melalui Jakarta':
        if 'ekspor_melalui' not in st.session_state:
            st.session_state['ekspor_melalui'] = get_ekspor_data('ekspor_melalui_jakarta.parquet')
        data_olah = st.session_state['ekspor_melalui']
    else:
        if 'impor_melalui' not in st.session_state:
            st.session_state['impor_melalui'] = get_impor_data('impor_melalui_jakarta.parquet')
        data_olah = st.session_state['impor_melalui']
        if 'HS' in data_olah.columns:
            data_olah.drop(columns=['HS'], inplace=True)


    periode_opt = ['Bulanan', 'Triwulanan', 'Caturwulan', 'Semesteran', 'Tahunan']
    periode = st.selectbox(label='Periode Olah', options=periode_opt, index=0)

    exclude = ['Periode', 'triwulan', 'semester', 'caturwulan', 'tahun', 'BOBOT', 'Nilai','FOB','NETTO']
    filtered_columns = [col for col in data_olah.columns if col not in exclude]
    
    selected_columns = st.multiselect(
        label="Pilih Variabel",
        options=filtered_columns
    )
    nilai_bobot = st.multiselect(
        label="Pilih yang akan disajikan",
        options=['Nilai', 'Bobot']
    )

    periode_hasil = periode_mapping.get(periode, periode.lower())
    if nilai_bobot:
        if dataset_selected in ['Impor Jakarta', 'Impor Melalui Jakarta']:
            mapping = nilai_bobot_impor
        else:
            mapping = nilai_bobot_ekspor

        # Susun urutan agar 'Nilai' selalu duluan jika dipilih
        ordered_keys = [key for key in ['Nilai', 'Bobot'] if key in nilai_bobot]
        hasil_olah = [mapping[key] for key in ordered_keys]

    # Process data on button click
    if st.button("Olah"):
        st.session_state['olah_pressed'] = True

    if st.session_state.get('olah_pressed'):
        if len(nilai_bobot) == 1:
            res_data = data_olah.pivot_table(
                index=selected_columns,
                columns=periode_hasil,
                values=hasil_olah[0],
                aggfunc='sum'
            ).fillna(0).reset_index()
            fixed_columns = selected_columns

            if(periode_hasil != 'tahun'):
                date_columns = [col for col in res_data.columns if '-' in str(col)]
                date_columns_sorted = sorted(date_columns, key=lambda x: (int(x.split('-')[1]), int(x.split('-')[0])), reverse=True)
                new_column_order = fixed_columns + date_columns_sorted

                result_final = res_data.reindex(columns=new_column_order)
                result_final = result_final.sort_values(by=date_columns_sorted[0], ascending=False).reset_index(drop=True)
            else:
                # Ambil kolom tahun (angka)
                year_columns = [col for col in res_data.columns if str(col).isdigit()]

                # Urutkan tahun secara descending (2024, 2023, 2022, ...)
                year_columns_sorted = sorted(year_columns, key=lambda x: int(x), reverse=True)

                # Susun ulang kolom: selected_columns + tahun terbaru ke terlama + sisanya (jika ada)
                new_column_order = selected_columns + year_columns_sorted + [col for col in res_data.columns if col not in selected_columns + year_columns_sorted]

                # Terapkan urutan kolom baru
                result_final = res_data[new_column_order]
                result_final = result_final.sort_values(by=year_columns_sorted[0], ascending=False).reset_index(drop=True)


            st.dataframe(result_final)
        if len(nilai_bobot) > 1:
            if(len(selected_columns) == 0):
                st.warning("Pilih Variabel yang ingin ditampilkan.")
            else:
                get_nilai = data_olah.pivot_table(
                    index=selected_columns,
                    columns=periode_hasil,
                    values=hasil_olah[0],
                    aggfunc='sum'
                ).fillna(0)

                get_nilai = get_nilai.reset_index()
                fixed_columns = selected_columns

                # date_columns = [col for col in get_nilai.columns if '-' in col]
                # date_columns_sorted = sorted(date_columns, key=lambda x: (int(x.split('-')[1]), int(x.split('-')[0])), reverse=True)

                # new_column_order = fixed_columns + date_columns_sorted

                # get_nilai_val = get_nilai.reindex(columns=new_column_order)


                ## Netto
                get_bobot = data_olah.pivot_table(
                    index=selected_columns,
                    columns=periode_hasil,
                    values=hasil_olah[1],
                    aggfunc='sum'
                ).fillna(0)
                get_bobot = get_bobot.reset_index()
                fixed_columns = selected_columns

                # Sorting tanggal dari kolom yang ada
                date_columns = [col for col in get_nilai.columns if '-' in col]
                date_columns_sorted = sorted(date_columns, key=lambda x: (int(x.split('-')[1]), int(x.split('-')[0])), reverse=True)

                # Rename kolom hasil pivot agar mudah dikenali sebelum merge
                get_nilai = get_nilai.rename(columns={col: f"{col} NILAI" for col in date_columns})
                get_bobot = get_bobot.rename(columns={col: f"{col} BOBOT" for col in date_columns})

                # Urutan kolom baru: pasangkan NILAI dan BOBOT
                combined_date_columns = []
                for date in date_columns_sorted:
                    combined_date_columns.append(f"{date} NILAI")
                    combined_date_columns.append(f"{date} BOBOT")

                # Susun ulang kolom sesuai urutan baru
                new_column_order = fixed_columns + combined_date_columns

                # Gabungkan data
                get_nilai_val = get_nilai.reset_index()
                get_bobot_val = get_bobot.reset_index()

                join_bobot_nilai = pd.merge(get_nilai_val, get_bobot_val, how='left', on=selected_columns)

                # Susun kolom sesuai urutan yang sudah ditentukan
                join_bobot_nilai = join_bobot_nilai.reindex(columns=new_column_order)

                # Urutkan baris berdasarkan kolom NILAI periode terbaru
                join_bobot_nilai = join_bobot_nilai.sort_values(by=f"{date_columns_sorted[0]} NILAI", ascending=False)

                # Tampilkan
                st.dataframe(join_bobot_nilai)


                # date_columns = [col for col in get_bobot.columns if '-' in col]
                # date_columns_sorted = sorted(date_columns, key=lambda x: (int(x.split('-')[1]), int(x.split('-')[0])), reverse=True)

                # new_column_order = fixed_columns + date_columns_sorted

                # get_bobot_val = get_bobot.reindex(columns=new_column_order)

                # join_bobot_nilai = pd.merge(get_nilai_val, get_bobot_val,  how='left', on=selected_columns)
                
                # new_column_names = {c: c.replace('_x', ' NILAI').replace('_y', ' BOBOT') for c in join_bobot_nilai.columns}
                # join_bobot_nilai.rename(columns=new_column_names, inplace=True)

                # join_bobot_nilai = join_bobot_nilai.sort_values(by=date_columns_sorted[0]+' NILAI', ascending=False)
                # st.dataframe(join_bobot_nilai)

        elif len(nilai_bobot) == 0:
            st.warning("Pilih salah satu dari 'Nilai' atau 'Bobot'.")
    else:
        # st.dataframe(impor_final.head(1000))
        if dataset_selected == 'Impor Jakarta':
            st.dataframe(st.session_state['impor_final'].head(1000))
        elif dataset_selected == 'Ekspor Jakarta':
            st.dataframe(st.session_state['ekspor_final'].head(1000))
        elif dataset_selected == 'Ekspor Melalui Jakarta':
            st.dataframe(st.session_state['ekspor_melalui'].head(1000))
        elif dataset_selected == 'Impor Melalui Jakarta':   
            st.dataframe(st.session_state['impor_melalui'].head(1000))

else:
    # --- UI ---
    st.title("Update File")

    upload_dataset = ['Impor Jakarta', 'Ekspor Jakarta', 'Impor Melalui Jakarta', 'Ekspor Melalui Jakarta']
    upload_selected = st.selectbox(label='Jenis Data Diolah', options=upload_dataset, index=0)

    jenis_data = ['Angka Sementara', 'Angka Tetap']
    jenis_selected = st.selectbox(label='Status Data', options=jenis_data, index=0)

    password_input = st.text_input('Password', type='password')

    uploaded_file = st.file_uploader('Upload file', type=['dbf'])

    tombol_upload = st.button('Proses Upload')

    # --- Proses ---
    if tombol_upload:
        if uploaded_file and password_input:
            if password_input == PASSWORD:
                if uploaded_file.name.lower().endswith('.dbf'):
                    st.success('Password benar dan file valid, memproses...')
                    file = uploaded_file

                    if upload_selected == 'Impor Jakarta':
                        # Filter periode yang sudah ada
                        existing_df = st.session_state['impor_final']
                        final_df = process_impor_dbf(file)
                        # check_null_values(existing_df)
                        
                        # st.dataframe(existing_df.head(1000))

                    elif upload_selected == 'Impor Melalui Jakarta':
                        if 'impor_melalui' not in st.session_state:
                            st.session_state['impor_melalui'] = get_impor_data('impor_melalui_jakarta.parquet')
                        existing_df = st.session_state['impor_melalui']
                        final_df = process_impor_dbf(file)
                    elif upload_selected == 'Ekspor Jakarta':
                        if 'ekspor_final' not in st.session_state:
                            st.session_state['ekspor_final'] = get_ekspor_data('ekspor_jakarta.parquet')
                        existing_df = st.session_state['ekspor_final']
                        final_df = process_ekspor_dbf(file)
                    else:
                        if 'ekspor_melalui' not in st.session_state:
                            st.session_state['ekspor_melalui'] = get_ekspor_data('ekspor_melalui_jakarta.parquet')
                        existing_df = st.session_state['ekspor_melalui']
                        final_df = process_ekspor_dbf(file)
                        # check_null_values(existing_df)


                    # st.dataframe(final_df.head(1000))

                    if 'Periode' in final_df.columns:
                        periode_baru = final_df['Periode'].unique()
                        existing_df = existing_df[~existing_df['Periode'].isin(periode_baru)]

                        # st.dataframe(existing_df.head(1000))

                        if upload_selected == 'Impor Jakarta':

                            # Tambahkan data baru
                            gabungan_df_baru = pd.concat([existing_df, final_df], ignore_index=True)
                            # df_null_hs2 = st.session_state['impor_final'][st.session_state['impor_final']['Kode HS 2 digit'].isnull()]
                            # st.dataframe(st.session_state['impor_final'].head(1000))
                            # check_null_values(existing_df)
                            # check_null_values(final_df)
                            # check_null_values(gabungan_df_baru)

                            # st.write(existing_df.shape)
                            # st.write(final_df.shape)
                            # st.write(gabungan_df_baru.shape)

                            st.session_state['impor_final'] = gabungan_df_baru
                            # # Hapus dan upload ulang parquet
                            status_data_res = status_data_mapping.get(jenis_selected)
                            delete_and_upload_new_parquet(gabungan_df_baru,'impor_jakarta.parquet')

                            for periode in periode_baru:
                                update_google_sheet(
                                    upload_selected,
                                    str(periode),  # pastikan periode dikonversi ke string
                                    uploaded_file.name,
                                    status_data_res,
                                    SpreadSheet_ID,
                                    sheet_name
                                )
                        elif upload_selected == 'Impor Melalui Jakarta':
                        
                                # Tambahkan data baru
                            gabungan_df_baru = pd.concat([existing_df, final_df], ignore_index=True)
                            # check_null_values(existing_df)
                            # check_null_values(final_df)
                            # check_null_values(gabungan_df_baru)

                            # st.text("Info: existing_df")
                            # st.dataframe(describe_with_nulls(existing_df))

                            # st.text("Info: final_df")
                            # st.dataframe(describe_with_nulls(final_df))

                            # st.text("Info: gabungan_df_baru")
                            # st.dataframe(describe_with_nulls(gabungan_df_baru))


                            # st.write(existing_df.shape)
                            # st.write(final_df.shape)
                            # st.write(gabungan_df_baru.shape)
                            st.session_state['impor_melalui'] = gabungan_df_baru
                            #     # Hapus dan upload ulang parquet
                            status_data_res = status_data_mapping.get(jenis_selected)
                            delete_and_upload_new_parquet(st.session_state['impor_melalui'],'impor_melalui_jakarta.parquet')
    
                            for periode in periode_baru:
                                update_google_sheet(
                                upload_selected,
                                str(periode),  # pastikan periode dikonversi ke string
                                uploaded_file.name,
                                status_data_res,
                                SpreadSheet_ID,
                                sheet_name
                            )
                        elif upload_selected == 'Ekspor Jakarta':
                            check_null_values(existing_df)
                            check_null_values(final_df)
                            tes = pd.concat([existing_df, final_df], ignore_index=True)
                            null_val = tes[tes['NAMA PELABUHAN'].isnull()]
                            st.dataframe(null_val)
                            check_null_values(tes)
                            
                            # status_data_res = status_data_mapping.get(jenis_selected)
                            # delete_and_upload_new_parquet(st.session_state['ekspor_final'],'ekspor_jakarta.parquet')
    
                            # for periode in periode_baru:
                            #     update_google_sheet(
                            #     upload_selected,
                            #     str(periode),  # pastikan periode dikonversi ke string
                            #     uploaded_file.name,
                            #     status_data_res,
                            #     SpreadSheet_ID,
                            #     sheet_name
                            # )
                        else:
                            gabungan_df_baru = pd.concat([existing_df, final_df], ignore_index=True)
                            st.session_state['ekspor_melalui'] = gabungan_df_baru
                            #     # Hapus dan upload ulang parquet
                            status_data_res = status_data_mapping.get(jenis_selected)
                            delete_and_upload_new_parquet(st.session_state['ekspor_melalui'],'ekspor_melalui_jakarta.parquet')
    
                            for periode in periode_baru:
                                update_google_sheet(
                                upload_selected,
                                str(periode),  # pastikan periode dikonversi ke string
                                uploaded_file.name,
                                status_data_res,
                                SpreadSheet_ID,
                                sheet_name
                            )

                        st.success('Data berhasil diproses dan diupdate!')
                    else:
                        st.error('Kolom Periode tidak ditemukan di file!')
                else:
                    st.error('File harus berformat .dbf!')
            else:
                st.error('Password salah!')
        else:
            st.error('Mohon upload file dan masukkan password terlebih dahulu.')
