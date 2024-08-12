# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 18:08:16 2024

@author: milind.rathod
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jun 12 17:58:49 2024

@author: milind.rathod
"""


import pandas as pd
import time
import zipfile
import os
from datetime import datetime, timedelta
from fuzzywuzzy import process, fuzz
import re

def normalize_string(s):
    return re.sub(r'\W+', '', s).strip()

def compare_strings(string1, string2):
    normalized_string1 = normalize_string(string1)
    normalized_string2 = normalize_string(string2)
    return normalized_string1 == normalized_string2

start_time = datetime.now().time().strftime("%H:%M:%S")
print('Start Time =',start_time)
# Get the current date
current_date = datetime.now().date()
current_year=datetime.now().strftime("%Y")

#Preprocessing of portal files
#Cosolidation of all state files
column_names = ['Sheet name','State','Claim Month','Month','GSTIN of Supplier','Trade/Legalname','Invoice Number','Invoice Type','Invoice Date','Invoice Value',
                'Place Of Supply','Supply Attract Revese Charge','Rate(%)','Taxable Value','Integrated Tax','Central Tax','State/UT Tax','Cess','Total GST','DFF Amount',
                'Remark','GSTR-1/IFF/GSTR-5 Period','GSTR-1/IFF/GSTR-5 Filling Date','ITC Availability','Reason','Applicable % of Tax Rate','Source','IRN','IRN Date',
                'Division','Document Number','Transaction Date','Eligibility']

# Create an empty DataFrame with the defined column names
sheets = ['B2B', 'B2BA', 'B2B-CDNR','B2B-CDNRA','Read me']

GSTR_portal = pd.DataFrame(columns=column_names)
# Specify the directory containing the Excel files
folder_path = r'D:\OneDrive - Radhakrishna Foodland Pvt Ltd\Python_Project\GST Reconciliation\Input\GSTR-2B'
# List all files in the folder
files = os.listdir(folder_path)
# Filter the list to include only Excel files (assuming .xlsx extension)
excel_files = [f for f in files if f.endswith('.xlsx')]

state_dict={
    
 '01':'Jammu And Kashmir',
 '02':'Himachal Pradesh',
 '03':'Punjab',
 '04':'Chandigarh',
 '05':'Uttrakahand',
 '06':'Haryana',
 '07':'Delhi',
 '08':'Rajasthan',
 '09':'Uttar Pradesh',
 '10':'Bihar',
 '11':'Sikkim',
 '12':'Arunachal Pradesh',
 '13':'Nagaland',
 '14':'Manipur',
 '15':'Mizoram',
 '16':'Tripura',
 '17':'Meghalaya',
 '18':'Assam',
 '19':'West Bengal',
 '20':'Jharkhand',
 '21':'Odisha',
 '22':'Chhattisgarh',
 '23':'Madhya Pradesh',
 '24':'Gujarat',
 '25':'Daman and Diu',
 '26':'Dadar and Nagar Haveli',
 '27':'Maharashtra',
 '29':'Karnataka',
 '30':'Goa',
 '31':'Lakshadweep',
 '32':'Kerala',
 '33':'Tamil Nadu',
 '34':'Puducherry',
 '35':'Andaman And Nicobar',
 '36':'Telangana',
 '37':'Andhra Pradesh',
 '38':'Ladhak',
 '96':'Other Country',
 '97':'Other Territory',
 'Jammu And Kashmir':'Jammu And Kashmir',
 'Himachal Pradesh':'Himachal Pradesh',
 'Punjab':'Punjab',
 'Chandigarh':'Chandigarh',
 'Uttrakahand':'Uttrakahand',
 'Haryana':'Haryana',
 'Delhi':'Delhi',
 'Rajasthan':'Rajasthan',
 'Uttar Pradesh':'Uttar Pradesh',
 'Bihar':'Bihar',
 'Sikkim':'Sikkim',
 'Arunachal Pradesh':'Arunachal Pradesh',
 'Nagaland':'Nagaland',
 'Manipur':'Manipur',
 'Mizoram':'Mizoram',
 'Tripura':'Tripura',
 'Megahlaya':'Meghalaya',
 'Assam':'Assam',
 'West Bengal':'West Bengal',
 'Jharkhand':'Jharkhand',
 'Odisha':'Odisha',
 'Chhattisgarh':'Chhattisgarh',
 'Madhya Pradesh':'Madhya Pradesh',
 'Gujarat':'Gujarat',
 'Daman and Diu':'Daman and Diu',
 'Dadar and Nager Haveli':'Dadar and Nagar Haveli',
 'Maharashtra':'Maharashtra',
 'Karnataka':'Karnataka',
 'Goa':'Goa',
 'Lakshadweep':'Lakshadweep',
 'Kerala':'Kerala',
 'Tamil Nadu':'Tamil Nadu',
 'Puducherry':'Puducherry',
 'Andaman And Nicobar':'Andaman And Nicobar',
 'Telangana':'Telangana',
 'Andhra Pradesh':'Andhra Pradesh',
 'Ladhak':'Ladhak',
 'Other Country':'Other Country',
 'Other Territory':'Other Territory'
 }

for file in excel_files:
    file_path = os.path.join(folder_path, file)
    file=pd.read_excel(file_path,sheet_name=sheets)
    B2B_df=file['B2B']
    B2B_CDNR_df =file['B2B-CDNR']
    read_me_df = file['Read me']
    #print(B2B_CDNR_df.iloc[2])
    B2BA_df=file['B2BA']
    for sheet_name in file:        
        if sheet_name=='B2B':
            B2B={
                'Sheet name' : 'B2B',
                'State':read_me_df.iloc[4,2][0:2],
                'Claim Month':B2B_df.iloc[5:,14].reset_index(drop=True),
                'Month':read_me_df.iloc[7,2][0:],
                'GSTIN of Supplier':B2B_df.iloc[5:,0].reset_index(drop=True),
                'Trade/Legalname':B2B_df.iloc[5:,1].reset_index(drop=True),
                'Invoice Number':B2B_df.iloc[5:,2].reset_index(drop=True),
                'Invoice Type':B2B_df.iloc[5:,3].reset_index(drop=True),     
                'Invoice Date':B2B_df.iloc[5:,4].reset_index(drop=True),
                'Invoice Value':B2B_df.iloc[5:,5].reset_index(drop=True),
                'Place Of Supply':B2B_df.iloc[5:,6].reset_index(drop=True),
                'Supply Attract Revese Charge':B2B_df.iloc[5:,7].reset_index(drop=True),
                'Rate(%)':B2B_df.iloc[5:,8].reset_index(drop=True),
                'Taxable Value':B2B_df.iloc[5:,9].reset_index(drop=True),
                'Integrated Tax':B2B_df.iloc[5:,10].reset_index(drop=True),
                'Central Tax':B2B_df.iloc[5:,11].reset_index(drop=True),
                'State/UT Tax':B2B_df.iloc[5:,12].reset_index(drop=True),
                'Cess':B2B_df.iloc[5:,13].reset_index(drop=True),
                'Total GST':B2B_df.iloc[5:,10].reset_index(drop=True)+B2B_df.iloc[5:,11].reset_index(drop=True)+B2B_df.iloc[5:,12].reset_index(drop=True)+B2B_df.iloc[5:,13].reset_index(drop=True),
                'GSTR-1/IFF/GSTR-5 Period':B2B_df.iloc[5:,14].reset_index(drop=True),
                'GSTR-1/IFF/GSTR-5 Filling Date':B2B_df.iloc[5:,15].reset_index(drop=True),
                'ITC Availability':B2B_df.iloc[5:,16].reset_index(drop=True),
               'Total CESS':'',
                'Applicable % of Tax Rate':B2B_df.iloc[5:,18].reset_index(drop=True),
                'Source':B2B_df.iloc[5:,19].reset_index(drop=True),
                'IRN':B2B_df.iloc[5:,20].reset_index(drop=True),
                'IRN Date':B2B_df.iloc[5:,21].reset_index(drop=True)
                }
            B2B=pd.DataFrame(B2B)
            B2B.loc[B2B['Invoice Type'] == 'Credit Note', ['Invoice Value', 'Taxable Value', 'Integrated Tax', 'Central Tax', 'State/UT Tax', 'Total GST']] *= -1
            GSTR_portal=pd.concat([GSTR_portal,B2B],ignore_index=True)
    
        if sheet_name=='B2B-CDNR':
            B2B_CDNR={
                 'Sheet name' : 'B2B-CDNR',
                 'State':read_me_df.iloc[4,2][0:2],
                 'Claim Month':B2B_CDNR_df.iloc[5:,15].reset_index(drop=True),
                 'Month': read_me_df.iloc[7,2][0:],
                 'GSTIN of Supplier':B2B_CDNR_df.iloc[5:,0].reset_index(drop=True),
                 'Trade/Legalname':B2B_CDNR_df.iloc[5:,1].reset_index(drop=True),
                 'Invoice Number':B2B_CDNR_df.iloc[5:,2].reset_index(drop=True),
                 'Invoice Type':B2B_CDNR_df.iloc[5:,3].reset_index(drop=True),
                 'Invoice Date':B2B_CDNR_df.iloc[5:,5].reset_index(drop=True),
                 'Invoice Value':B2B_CDNR_df.iloc[5:,6].reset_index(drop=True),
                 'Place Of Supply':B2B_CDNR_df.iloc[5:,7].reset_index(drop=True),
                 'Supply Attract Revese Charge':B2B_CDNR_df.iloc[5:,8].reset_index(drop=True),
                 'Rate(%)':B2B_CDNR_df.iloc[5:,9].reset_index(drop=True),
                 'Taxable Value':B2B_CDNR_df.iloc[5:,10].reset_index(drop=True),
                 'Integrated Tax':B2B_CDNR_df.iloc[5:,11].reset_index(drop=True),
                 'Central Tax':B2B_CDNR_df.iloc[5:,12].reset_index(drop=True),
                 'State/UT Tax':B2B_CDNR_df.iloc[5:,13].reset_index(drop=True),
                 'Cess':B2B_CDNR_df.iloc[5:,14].reset_index(drop=True),
                 'Total GST':B2B_CDNR_df.iloc[5:,11].reset_index(drop=True)+B2B_CDNR_df.iloc[5:,12].reset_index(drop=True)+B2B_CDNR_df.iloc[5:,13].reset_index(drop=True)+B2B_CDNR_df.iloc[5:,14].reset_index(drop=True),
                 'GSTR-1/IFF/GSTR-5 Period':B2B_CDNR_df.iloc[5:,15].reset_index(drop=True),
                 'GSTR-1/IFF/GSTR-5 Filling Date':B2B_CDNR_df.iloc[5:,16].reset_index(drop=True),
                 'ITC Availability':B2B_CDNR_df.iloc[5:,17].reset_index(drop=True),
                 'Applicable % of Tax Rate':B2B_CDNR_df.iloc[5:,19].reset_index(drop=True),
                 'Source':B2B_CDNR_df.iloc[5:,20].reset_index(drop=True),
                 'IRN':B2B_CDNR_df.iloc[5:,21].reset_index(drop=True),
                 'IRN Date':B2B_CDNR_df.iloc[5:,22].reset_index(drop=True),
               }
            B2B_CDNR = pd.DataFrame(B2B_CDNR)
            B2B_CDNR.loc[B2B_CDNR['Invoice Type'] == 'Credit Note', ['Invoice Value', 'Taxable Value', 'Integrated Tax', 'Central Tax', 'State/UT Tax', 'Total GST','Cess']] *= -1  
            # Apply conditions to set multiple columns
            GSTR_portal = pd.concat([GSTR_portal, B2B_CDNR], ignore_index=True)
        if sheet_name=='B2BA':
            B2BA={
                 'Sheet name' : 'B2BA',
                 'State':read_me_df.iloc[4,2][0:2],
                 'Claim Month':B2BA_df.iloc[6:,16].reset_index(drop=True),
                 'Month': read_me_df.iloc[7,2][0:],
                 'GSTIN of Supplier':B2BA_df.iloc[6:,2].reset_index(drop=True),
                 'Trade/Legalname':B2BA_df.iloc[6:,3].reset_index(drop=True),
                 'Invoice Number':B2BA_df.iloc[6:,0].reset_index(drop=True),
                 'Invoice Type':B2BA_df.iloc[6:,5].reset_index(drop=True),
                 'Invoice Date':B2BA_df.iloc[6:,6].reset_index(drop=True),
                 'Invoice Value':B2BA_df.iloc[6:,7].reset_index(drop=True),
                 'Place Of Supply':B2BA_df.iloc[6:,8].reset_index(drop=True),
                 'Supply Attract Revese Charge':B2BA_df.iloc[6:,9].reset_index(drop=True),
                 'Rate(%)': B2BA_df.iloc[6:,10].reset_index(drop=True),
                 'Taxable Value':B2BA_df.iloc[6:,11].reset_index(drop=True),
                 'Integrated Tax':B2BA_df.iloc[6:,12].reset_index(drop=True),
                 'Central Tax':B2BA_df.iloc[6:,13].reset_index(drop=True),
                 'State/UT Tax':B2BA_df.iloc[6:,14].reset_index(drop=True),
                 'Cess':B2BA_df.iloc[6:,15].reset_index(drop=True),
                 'Total GST':B2BA_df.iloc[6:,12].reset_index(drop=True)+B2BA_df.iloc[6:,13].reset_index(drop=True)+B2BA_df.iloc[6:,14].reset_index(drop=True)+B2BA_df.iloc[6:,15].reset_index(drop=True),
                 'GSTR-1/IFF/GSTR-5 Period':B2BA_df.iloc[6:,16].reset_index(drop=True),
                 'GSTR-1/IFF/GSTR-5 Filling Date':B2BA_df.iloc[6:,17].reset_index(drop=True),
                 'ITC Availability':B2BA_df.iloc[6:,18].reset_index(drop=True),
                 'Applicable % of Tax Rate':B2BA_df.iloc[6:,20].reset_index(drop=True),
                }
            B2BA=pd.DataFrame(B2BA)
            B2BA.loc[B2BA['Invoice Type'] == 'Credit Note', ['Invoice Value', 'Taxable Value', 'Integrated Tax', 'Central Tax', 'State/UT Tax', 'Total GST']] *= -1
            GSTR_portal=pd.concat([GSTR_portal,B2BA],ignore_index=True) 
# 30 days ago          
GSTR_portal['Month'] = pd.to_datetime(GSTR_portal['Month'])
GSTR_portal['Month'] = (GSTR_portal['Month'] - pd.Timedelta(days=30)).dt.strftime("%b'%y")

GSTR_portal['Invoice Date'] = pd.to_datetime(GSTR_portal['Invoice Date'], format="%d/%m/%Y").dt.strftime("%b'%y")


#apply dictionary mapping on state column
GSTR_portal['State'] = GSTR_portal['State'].map(state_dict) 

# Preprocessing of all state and create into GSTR_Portal_Consolidated  excel file          
output_file = 'D:\\OneDrive - Radhakrishna Foodland Pvt Ltd\\Python_Project\\GST Reconciliation\\output\\GSTR_Portal_Consolidated.xlsx'             
GSTR_portal.to_excel(output_file, index=False)


date_30_days_ago=GSTR_portal['Month'].iloc[0]


#preprocessing of system files
file_path_GSTR_consolidated='D:\\OneDrive - Radhakrishna Foodland Pvt Ltd\\Python_Project\\GST Reconciliation\\Input\\GSTR-2.xlsx'
GSTR_portal_unmatched='D:\\OneDrive - Radhakrishna Foodland Pvt Ltd\\Python_Project\\GST Reconciliation\\Input\\GSTR 2B_Unmatched.xlsx'
GSTR_system_unmatched='D:\\OneDrive - Radhakrishna Foodland Pvt Ltd\\Python_Project\\GST Reconciliation\\Input\\GSTR2_Unmatched.xlsx'
GSTR_portal_unmatched=pd.read_excel(GSTR_portal_unmatched)
GSTR_system_unmatched=pd.read_excel(GSTR_system_unmatched)
GSTR_portal_unmatched['Sheet name']='B2B'


GSTR_portal_unmatched['Month']=pd.to_datetime(GSTR_portal_unmatched['Month']).dt.strftime("%b'%y")
GSTR_system_unmatched['Month']=pd.to_datetime(GSTR_system_unmatched['Month']).dt.strftime("%b'%y")

GSTR_portal_unmatched['Invoice Date']=pd.to_datetime(GSTR_portal_unmatched['Invoice Date'],format="%d/%m/%Y").dt.strftime("%b'%y")
GSTR_system_unmatched['Invoice Date']=pd.to_datetime(GSTR_system_unmatched['Invoice Date'],format="%d/%m/%Y").dt.strftime("%b'%y")

#sheet_name_1='GSTR3B'
sheet_name_1='Sheet1'
# Lists of columns to compare GSTR_consolidated
GSTR_system =pd.read_excel(file_path_GSTR_consolidated,sheet_name=sheet_name_1)
#GSTR_system['Claim Month']=pd.to_datetime(GSTR_system['Claim Month']).dt.strftime("%b'%y")
GSTR_system['Month']=date_30_days_ago

GSTR_system['Invoice Date']=pd.to_datetime(GSTR_system['Invoice Date'],format="%d/%m/%Y").dt.strftime("%b'%y")

#Appending current month and unmatched data
GSTR_system = pd.concat([GSTR_system, GSTR_system_unmatched], ignore_index=True)
GSTR_portal = pd.concat([GSTR_portal, GSTR_portal_unmatched], ignore_index=True)


#Aggregation of files
aggregated_gstr_portal = GSTR_portal.groupby(['GSTIN of Supplier', 'Invoice Number'], as_index=False).agg({
    'Sheet name' : 'first',
    'State': 'first',
    'Month':'first',
    'Trade/Legalname':'first',
    'Invoice Type':'first',
    'Invoice Date':'first',
    'Invoice Value':'first',
    'Place Of Supply':'first',
    'Supply Attract Revese Charge':'first',
    'Rate(%)':'first',
    'Taxable Value':'sum',
    'Integrated Tax':'sum',
    'Central Tax':'sum',
    'State/UT Tax':'sum',
    'Cess':'sum',
    'Total GST':'sum',
    'DFF Amount':'first',
    'Remark':'first',
    'GSTR-1/IFF/GSTR-5 Period':'first',
    'GSTR-1/IFF/GSTR-5 Filling Date':'first',
    'ITC Availability':'first',
    'Reason':'first',
    'Applicable % of Tax Rate':'first',
    'Source':'first',
    'IRN':'first',
    'IRN Date':'first',
    'Division':'first',
    'Document Number':'first',
    'Transaction Date':'first',
    'Eligibility':'first'
    })

aggregated_gstr_system = GSTR_system.groupby(['Vendor GSTIN', 'Invoice Number'], as_index=False).agg({
    'State': 'first',
    'RFPL GSTIN':'first',
    'Month':'first',
    'Invoice Date':'first',
    'Vendor Name':'first',
    'Account Head':'first',
    'Item Quantity':'sum',
    'Item Unit of Measurement':'first',
    'Item Taxable Value':'sum',
    'Rate':'first', 
    'HSN':'first',
    'IGST Amount': 'sum',
    'CGST Amount': 'sum',
    'SGST Amount': 'sum',
    'CESS Rate':'sum',
    'CESS Amount':'sum',
    'Absolute tax Amount':'sum',
    'Absolute tax rate':'sum',
    'State Code - Place of Supply':'first',
    'Whether ineligible for ITC?':'first',
    'Division':'first',
    'Document Number': lambda x: ', '.join(set(x)),
    'Transaction Date':'first',
    'Location':'first',
    'Remark':'first',
    'Concern person ':'first',
    'Weaving GSTIN':'first'
    
})

aggregated_gstr_system['Total GST_System']=aggregated_gstr_system['IGST Amount']+aggregated_gstr_system['CGST Amount']+aggregated_gstr_system['SGST Amount']+aggregated_gstr_system['CESS Amount']+aggregated_gstr_system['Absolute tax Amount']
aggregated_gstr_system['Total CESS']=aggregated_gstr_system['CESS Amount']+aggregated_gstr_system['Absolute tax Amount']
#Comparison of System file with Portal files
aggregated_gstr_system['Claim Month'] = None
aggregated_gstr_system['Remark2']=''
aggregated_gstr_portal['Remark'] = ''
aggregated_gstr_portal['Claim Month'] = None
aggregated_gstr_system['Invoice Number as Per GSTR 2B']=''
aggregated_gstr_portal['Invoice Number as Per GSTR 2']=''
aggregated_gstr_portal['As per GSTR 2 GSTIN Number']=''
aggregated_gstr_system['As per GSTR2B GSTIN Number']=''
aggregated_gstr_portal['Created By Name']=''

aggregated_gstr_portal['Invoice Number']=aggregated_gstr_portal['Invoice Number'].astype(str)
aggregated_gstr_system['Invoice Number']=aggregated_gstr_system['Invoice Number'].astype(str) 

#aggregated_gstr_portal=aggregated_gstr_portal[aggregated_gstr_portal['Invoice Number']=='KASO070324123409'] #TNIN0149/0624-25  KASO070324123409
#aggregated_gstr_system=aggregated_gstr_system[aggregated_gstr_system['Invoice Number']=='KASO070324123409'] # TNIN01490624-25  KASO070324123409


#Comparison of system and portal
for i, x in aggregated_gstr_system.iterrows():
    for j, y in aggregated_gstr_portal.iterrows():
        if y['Remark'] !='':
            continue
        if x['Vendor GSTIN'] == y['GSTIN of Supplier'] and normalize_string(x['Invoice Number']) == normalize_string(y['Invoice Number']) :
            remarks1 = []
            remarks2 = []
            if x['State'] != y['State']:             
                remarks1.append(f"Bill accounted in {x['State']} instead of {y['State']}")
                remarks2.append(f"Bill accounted in {y['State']} instead of {x['State']}")
                  
            if abs(x['CGST Amount'] - y['Central Tax']) >= 1:
                remarks1.append('CGST value does not match')
                remarks2.append('CGST value does not match')
            
            if abs(x['SGST Amount'] - y['State/UT Tax']) >= 1:
                remarks1.append('SGST value does not match')
                remarks2.append('SGST value does not match')
            
            if abs(x['IGST Amount'] - y['Integrated Tax']) >= 1:
                remarks1.append('IGST value does not match')
                remarks2.append('IGST value does not match')
            
            if abs(x['CESS Amount']+x['Absolute tax Amount'] - y['Cess']) >= 1:
                remarks1.append('CESS value does not match')
                remarks2.append('CESS value does not match')
                
            if abs(x['Item Taxable Value'] - y['Taxable Value']) >=1:
                remarks1.append('Taxable Value does not match')
                remarks2.append('Taxable Value does not match')

                
            if remarks1:
                aggregated_gstr_system.at[i, 'Remark2'] = '-'.join(remarks1)
                aggregated_gstr_system.at[i, 'Claim Month'] = y['Month'] 
                aggregated_gstr_system.at[i,'Differences of Total GST Between 2B Vs 2']=y['Total GST']-x['Total GST_System']
            if remarks2:
                aggregated_gstr_portal.at[j, 'Remark'] = '-'.join(remarks2)
                aggregated_gstr_portal.at[j, 'Claim Month'] = x['Month']
                aggregated_gstr_portal.at[j, 'DFF Amount']=x['Total GST_System']-y['Total GST']
                aggregated_gstr_portal.at[j,'Division']= x['Division']
                aggregated_gstr_portal.at[j,'Document Number']=x['Document Number']
                aggregated_gstr_portal.at[j,'Transaction Date']=x['Transaction Date']
                aggregated_gstr_portal.at[j,'Location']=x['Location']
              
            else:
                aggregated_gstr_system.at[i, 'Remark2'] = 'Match'
                aggregated_gstr_portal.at[j, 'Remark'] = 'Match'
                aggregated_gstr_system.at[i,'Differences of Total GST Between 2B Vs 2']=y['Total GST']-x['Total GST_System']
                aggregated_gstr_portal.at[j, 'DFF Amount']=x['Total GST_System']-y['Total GST']
                aggregated_gstr_system.at[i, 'Claim Month'] = y['Month']
                aggregated_gstr_portal.at[j, 'Claim Month'] = x['Month']
                aggregated_gstr_portal.at[j,'Division']= x['Division']
                aggregated_gstr_portal.at[j,'Document Number']=x['Document Number']
                aggregated_gstr_portal.at[j,'Transaction Date']=x['Transaction Date']
                aggregated_gstr_portal.at[j,'Location']=x['Location']
                aggregated_gstr_portal.at[j,'Created By Name']=x['Concern person ']
               
            break 
        
        elif x['Vendor GSTIN'][7:11]== y['GSTIN of Supplier'][7:11] and normalize_string(x['Invoice Number']) == normalize_string(y['Invoice Number']):

            remark1 = []
            remark2 = []
            remark1.append('GSTIN Partially Match')
            remark2.append('GSTIN Partially Match')
            
            if x['State'] != y['State']:
                remark1.append(f"Bill accounted in {x['State']} instead of {y['State']}")
                remark2.append(f"Bill accounted in {y['State']} instead of {x['State']}")
                            
            if abs(x['CGST Amount'] - y['Central Tax']) >= 1:
                remark1.append('CGST value does not match')
                remark2.append('CGST value does not match')
                            
            if abs(x['SGST Amount'] - y['State/UT Tax']) >= 1:
                remark1.append('SGST value does not match')
                remark2.append('SGST value does not match')
                            
            if abs(x['IGST Amount'] - y['Integrated Tax']) >= 1:
                remark1.append('IGST value does not match')
                remark2.append('IGST value does not match')
                
            
            if abs(x['CESS Amount']+x['Absolute tax Amount'] - y['Cess']) >= 1:
                remark1.append('CESS value does not match')
                remark2.append('CESS value does not match')
                
            if abs(x['Item Taxable Value'] - y['Taxable Value']) >=1:
                remarks1.append('Taxable Value does not match')
                remarks2.append('Taxable Value does not match')
                
            if remark1:
                aggregated_gstr_system.at[i, 'Remark2'] = '-'.join(remark1)
                aggregated_gstr_system.at[i, 'Claim Month'] = y['Month'] #Added Month for GSTIN Partially matched cases
                aggregated_gstr_system.at[i,'Differences of Total GST Between 2B Vs 2']=y['Total GST']-x['Total GST_System']
                aggregated_gstr_system.at[i,'As per GSTR2B GSTIN Number']=y['GSTIN of Supplier']

            if remark2:
                aggregated_gstr_portal.at[j, 'Remark'] = '-'.join(remark2)
                aggregated_gstr_portal.at[j, 'Claim Month'] = x['Month']
                aggregated_gstr_portal.at[j, 'DFF Amount']=x['Total GST_System']-y['Total GST']
                aggregated_gstr_portal.at[j,'Division']= x['Division']
                aggregated_gstr_portal.at[j,'Document Number']=x['Document Number']
                aggregated_gstr_portal.at[j,'Transaction Date']=x['Transaction Date']
                aggregated_gstr_portal.at[j,'Location']=x['Location']
                aggregated_gstr_portal.at[j,'As per GSTR 2 GSTIN Number']=x['Vendor GSTIN']
                aggregated_gstr_portal.at[j,'Created By Name']=x['Concern person ']
    

#Cases where everything except invoice number matches
for i, x in aggregated_gstr_system.iterrows():
    for j, y in aggregated_gstr_portal.iterrows():
        if y['Remark'] !='' or x['Remark2'] !='':
            continue
        
        elif x['Vendor GSTIN']== y['GSTIN of Supplier'] and  normalize_string(x['Invoice Number']) == normalize_string(y['Invoice Number']) and x['State'] == y['State'] and abs(x['IGST Amount'] - y['Integrated Tax']) < 1 and abs(x['CGST Amount'] - y['Central Tax'])< 1 and abs(x['SGST Amount'] - y['State/UT Tax']) < 1 and abs(x['CESS Amount']+x['Absolute tax Amount'] - y['Cess']) <1:#Removing taxable value check
            aggregated_gstr_system.at[i, 'Remark2'] = 'Match'
            aggregated_gstr_portal.at[j, 'Remark'] = 'Match'
            
            aggregated_gstr_system.at[i, 'Claim Month'] = y['Month'] #Added month for cases where Invoice number are wrong
            aggregated_gstr_system.at[i,'Invoice Number as Per GSTR 2B'] = y['Invoice Number']
            aggregated_gstr_portal.at[j,'Invoice Number as Per GSTR 2']=x['Invoice Number']
            aggregated_gstr_system.at[i,'Differences of Total GST Between 2B Vs 2']=y['Total GST']-x['Total GST_System']
            aggregated_gstr_portal.at[j, 'DFF Amount']=x['Total GST_System']-y['Total GST']
            aggregated_gstr_portal.at[j, 'Claim Month'] = x['Month']
            aggregated_gstr_portal.at[j,'Division']= x['Division']
            aggregated_gstr_portal.at[j,'Document Number']=x['Document Number']
            aggregated_gstr_portal.at[j,'Transaction Date']=x['Transaction Date']
            aggregated_gstr_portal.at[j,'Location']=x['Location']
            aggregated_gstr_portal.at[j,'Created By Name']=x['Concern person ']
        
            break
        elif x['Vendor GSTIN']== y['GSTIN of Supplier'] and  normalize_string(x['Invoice Number']) != normalize_string(y['Invoice Number']) and x['State'] == y['State'] and abs(x['IGST Amount'] - y['Integrated Tax']) < 1 and abs(x['CGST Amount'] - y['Central Tax'])< 1 and abs(x['SGST Amount'] - y['State/UT Tax']) < 1 and abs(x['CESS Amount']+x['Absolute tax Amount'] - y['Cess']) <1:
            aggregated_gstr_system.at[i, 'Remark2'] ='Invoice Number Approximately Match'
            aggregated_gstr_portal.at[j, 'Remark'] ='Invoice Number Approximately Match'
            
            aggregated_gstr_system.at[i,'Invoice Number as Per GSTR 2B'] = y['Invoice Number']
            aggregated_gstr_portal.at[j,'Invoice Number as Per GSTR 2']=x['Invoice Number']
            aggregated_gstr_system.at[i,'Differences of Total GST Between 2B Vs 2']=y['Total GST']-x['Total GST_System']
            aggregated_gstr_portal.at[j, 'DFF Amount']=x['Total GST_System']-y['Total GST']
            aggregated_gstr_portal.at[j, 'Claim Month'] = x['Month']
            aggregated_gstr_portal.at[j,'Division']= x['Division']
            aggregated_gstr_portal.at[j,'Document Number']=x['Document Number']
            aggregated_gstr_portal.at[j,'Transaction Date']=x['Transaction Date']
            aggregated_gstr_portal.at[j,'Location']=x['Location']
            aggregated_gstr_portal.at[j,'Created By Name']=x['Concern person ']
            break
            
#Comparison of System file with itself
for i, x in aggregated_gstr_system.iterrows():
    current_remark = aggregated_gstr_system.at[i, 'Remark2']
    if current_remark != '':
        if x['RFPL GSTIN'][0:2] == x['Vendor GSTIN'][0:2] and x['IGST Amount'] > 0:
            new_remark = 'CGST and SGST As per GSTR-2B'
            aggregated_gstr_system.at[i, 'Remark2'] = current_remark + '-' + new_remark 
            #aggregated_gstr_system.at[i,'Claim Month']=date_30_days_ago
        if x['RFPL GSTIN'][0:2] != x['Vendor GSTIN'][0:2] and (x['CGST Amount'] > 0 or x['SGST Amount'] > 0):
            new_remark = 'IGST As per GSTR-2B'
            aggregated_gstr_system.at[i, 'Remark2'] = current_remark + '-' + new_remark 
            #aggregated_gstr_system.at[i,'Claim Month']=date_30_days_ago

aggregated_gstr_system.loc[aggregated_gstr_system['Remark2'].eq(''), 'Remark2'] = 'Not Match'
aggregated_gstr_portal.loc[aggregated_gstr_portal['Remark'].eq(''),'Remark'] = 'Not Match'
aggregated_gstr_system.loc[aggregated_gstr_system['Remark2'].eq('Not Match'),'Claim Month']='Not in GSTR 2B'
aggregated_gstr_portal.loc[aggregated_gstr_portal['Remark'].eq('Not Match'),'Claim Month'] = 'Not in GSTR 2'

new_positions_portal=['State','Claim Month','Month','GSTIN of Supplier', 'Trade/Legalname','Invoice Number', 'Invoice Type','Invoice Date','Invoice Value',
               'Place Of Supply','Supply Attract Revese Charge','Rate(%)','Taxable Value','Integrated Tax','Central Tax','State/UT Tax','Cess','Total GST','DFF Amount','Remark','GSTR-1/IFF/GSTR-5 Period','GSTR-1/IFF/GSTR-5 Filling Date','ITC Availability','Reason','Applicable % of Tax Rate','Source','IRN','IRN Date','Division','Document Number',
               'Transaction Date','Eligibility','As per GSTR 2 GSTIN Number','Invoice Number as Per GSTR 2','Created By Name','Sheet name']

aggregated_gstr_portal=aggregated_gstr_portal[new_positions_portal]
aggregated_gstr_portal.rename(columns={'Claim Month': 'As per GSTR-2'}, inplace=True)
aggregated_gstr_portal.rename(columns={'DFF Amount': 'Differences of Total GST Between 2 Vs 2B'}, inplace=True)
 
output_file = 'D:\\OneDrive - Radhakrishna Foodland Pvt Ltd\\Python_Project\\GST Reconciliation\\output\\GSTR2B.xlsx' 
aggregated_gstr_portal.to_excel(output_file, index=False)

aggregated_gstr_system['Supply Attract Revese Charge']=''
aggregated_gstr_system['Year']=current_year
aggregated_gstr_system['Invoice type']='Regular'
aggregated_gstr_system.rename(columns={'State Code - Place of Supply': 'Place of supply'}, inplace=True)
aggregated_gstr_system.rename(columns={'Whether ineligible for ITC?': 'ITC Eligible'}, inplace=True)
aggregated_gstr_system.rename(columns={'Concern person ':'Created By Name'}, inplace=True)

new_positions_system = ['RFPL GSTIN','State','Vendor GSTIN','Claim Month','Month','Year','Invoice Number','Invoice type','Invoice Date' ,'Vendor Name','Account Head','Place of supply','Supply Attract Revese Charge',
                        'Item Quantity','Item Unit of Measurement','HSN','Item Taxable Value','Rate','IGST Amount','CGST Amount','SGST Amount','CESS Rate',
                        'CESS Amount','Absolute tax Amount','Absolute tax rate','Total CESS','Total GST_System','Differences of Total GST Between 2B Vs 2','Division','Document Number',
                        'Transaction Date','Location','ITC Eligible','Created By Name','Remark2','As per GSTR2B GSTIN Number','Invoice Number as Per GSTR 2B']

aggregated_gstr_system=aggregated_gstr_system[new_positions_system]

#aggregated_gstr_system['Month']=pd.to_datetime(aggregated_gstr_system['Month']).dt.strftime("%b'%y")
aggregated_gstr_system.rename(columns={'Claim Month':'GSTR 2B Claim Month '}, inplace=True)
output_file = 'D:\\OneDrive - Radhakrishna Foodland Pvt Ltd\\Python_Project\\GST Reconciliation\\output\\GSTR2.xlsx'             
aggregated_gstr_system.to_excel(output_file, index=False)

'''
import pyautogui
        
class KeyTriggers:
    def __init__(self):
        self.key_delay = 1

    def trigger_key_pa_flow(self):
        pyautogui.sleep(self.key_delay)
        pyautogui.hotkey('ctrl', 'shift', 'alt', 's')
        print('trigger Called')  
        
KeyTriggers().trigger_key_pa_flow()
'''
end_time = datetime.now().time().strftime("%H:%M:%S")
print('Finished Time =',end_time)







