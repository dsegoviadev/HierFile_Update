import pandas as pd
from datetime import datetime

jcfpath = "C:\\Users\\dsegovia\\OneDrive - AutoZone Parts, Inc\\Documents\\Hierarchy tool\\jcf.xlsx"
#CREATE FULL JOB CODE FILE#
JCFBASE = pd.read_excel(jcfpath,sheet_name = 'BASE')
JCFDM = pd.read_excel(jcfpath,sheet_name = 'Base DM TSM')
FULLJCF = JCFBASE.merge(right=JCFDM,left_on=('STORE_COD'), right_on=('CR'))
#END CREATION OF FULL JOB CODE FILE#

#CREATE TIME DATA#
currD = datetime.now()
#END CREATE TIME DATA# 

#CREATE COMMENTS DATAFRAME#
COMMENTS = pd.DataFrame(columns=['Store','Change Type', 'Comment', 'Store Len(max|min)'])
#END CREATION OF  COMMENTS DATAFRAME#

#CREATION OF HIERFILE#
hierpath = "C:\\Users\\dsegovia\\OneDrive - AutoZone Parts, Inc\\Documents\\Hierarchy tool\\HierFile.csv"
HIER = pd.read_csv(hierpath,index_col=None, header=0, encoding='ISO-8859-9')
#END OF CREATION OF HIERFILE#

#CREATION OF FILE TO WORK WITH#
FULLDF = HIER.merge(FULLJCF, left_on='STORE_ID', right_on='STORE_COD')
FULLDF = FULLDF.apply(lambda x: x.astype(str).str.upper())
FULLDF = FULLDF.replace('NAN', "''")
#END OF CREATION OF FILE TO WORKK WITH#

#LOGIC#
    #CREATE A COMMENT FOR LEN OF STORE ID#
COMMENTS.at[0,'Store Len(max|min)'] = str(min(FULLDF['STORE_ID'].str.len())) + "|" + str(max(FULLDF['STORE_ID'].str.len()))
    #END OF CREATE A COMMENT FOR LEN OF STORE ID#
i = 0
c = 1
#LOGIC FOR CHANGES IN STORE AND COMM MANAGERS#
for x in FULLDF['STORE_ID']:
    store = ''
    changeType = ''
    comment = ''
    if FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'] != FULLDF.iloc[i]['STORE_MGR_EMP_ID_y']:
        store = FULLDF.iloc[i]['STORE_ID']
        changeType = 'Store MGR'
        if FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'] == "''":
            comment = 'Changing: ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'] + 'for ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_y'][:-2]
        elif FULLDF.iloc[i]['STORE_MGR_EMP_ID_y'] == "''":
            comment = 'Changing: ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'][:-2] + 'for ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_y']
        else:
            comment = 'Changing: ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'][:-2] + 'for ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_y'][:-2] 
        
        COMMENTS.at[c, 'Store'] = store
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

        commentNM = 'Changing: ' + FULLDF.iloc[i]['STORE_MGR_NAME_x'] + ' for ' + FULLDF.iloc[i]['STORE_MGR_NAME_y']
        COMMENTS.at[c, 'Store'] = store
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = commentNM
        c += 1

    if FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_x'] != FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_y']:
        store = FULLDF.iloc[i]['STORE_ID']
        changeType = 'COMM Sales'
        if FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_x'] == "''":
            comment = 'Changing: ' + FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_x'] + ' for ' + FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_y'][:-2]
        elif FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_y'] == "''":
            comment = 'Changing: ' + FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_x'][:-2] + ' for ' + FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_y']
        else:
            comment = 'Changing: ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'][:-2] + ' for ' + FULLDF.iloc[i]['COMM_SALES_MGR_EMP_ID_y'][:-2]
        COMMENTS.at[c, 'Store'] = store
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
        commentNM = 'Changing: ' + FULLDF.iloc[i]['COMM_SALES_MGR_NAME_x'] + ' for ' + FULLDF.iloc[i]['COMM_SALES_MGR_NAME_y']
        COMMENTS.at[c, 'Store'] = store
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = commentNM
        c += 1
    i += 1

#CHANGE THE CELL VALUES#
FULLDF['STORE_MGR_EMP_ID_x'].mask(FULLDF['STORE_MGR_EMP_ID_x'] != FULLDF['STORE_MGR_EMP_ID_y'], FULLDF['STORE_MGR_EMP_ID_y'], inplace=True)
FULLDF['STORE_MGR_NAME_x'].mask(FULLDF['STORE_MGR_NAME_x'] != FULLDF['STORE_MGR_NAME_y'], FULLDF['STORE_MGR_NAME_y'], inplace=True)
FULLDF['COMM_SALES_MGR_EMP_ID_x'].mask(FULLDF['COMM_SALES_MGR_EMP_ID_x'] != FULLDF['COMM_SALES_MGR_EMP_ID_y'], FULLDF['COMM_SALES_MGR_EMP_ID_y'], inplace=True)
FULLDF['COMM_SALES_MGR_NAME_x'].mask(FULLDF['COMM_SALES_MGR_NAME_x'] != FULLDF['COMM_SALES_MGR_NAME_y'], FULLDF['COMM_SALES_MGR_NAME_y'], inplace=True)
#END THE CHANGE VALUES BLOCK#
# END OF LOGIC FOR CHANGES IN STORE AND COMM MANAGERS#

#LOGIC FOR DM AND TSM UPDATE#
i = 0
for x in FULLDF['STORE_ID']:
    store = ''
    changeType = ''
    comment = ''
    if FULLDF.iloc[i]['HIERFLG'] == '1':
        if FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_x'] != FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_y']:
            if FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_x'] == "''":
                comment = Comment = 'Changing: ' + FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_x'] + ' for ' + FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_y'][:-2]
            elif FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_y'] == "''":
                comment = Comment = 'Changing: ' + FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_x'][:-2] + ' for ' + FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_y']
            else:
                comment = Comment = 'Changing: ' + FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_x'][:-2] + ' for ' + FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_y'][:-2]
            store = FULLDF.iloc[i]['STORE_ID']
            changeType = 'DM Change'
            COMMENTS.at[c, 'Store'] = store
            COMMENTS.at[c, 'Change Type'] = changeType
            COMMENTS.at[c, 'Comment'] = comment
            c += 1
            commentNM = 'Changing: ' + FULLDF.iloc[i]['DISTRICT_MGR_NAME_x'] + ' for ' + FULLDF.iloc[i]['DISTRICT_MGR_NAME_y']
            COMMENTS.at[c, 'Store'] = store
            COMMENTS.at[c, 'Change Type'] = changeType
            COMMENTS.at[c, 'Comment'] = commentNM
            c += 1
            if FULLDF.iloc[i]['COM_DST_MGR_ID'] != FULLDF.iloc[i]['TSM Ignition']:
                comment = 'Changing: ' + FULLDF.iloc[i]['COM_DST_MGR_ID'] + ' for ' + FULLDF.iloc[i]['TSM Ignition']
                store = FULLDF.iloc[i]['STORE_ID']
                changeType = 'TSM Change'
                COMMENTS.at[c, 'Store'] = store
                COMMENTS.at[c, 'Change Type'] = changeType
                COMMENTS.at[c, 'Comment'] = comment
                c += 1
                commentNM = 'Changing: ' + FULLDF.iloc[i]['COM_DST_MGR_NAME'] + ' for ' + FULLDF.iloc[i]['TSM']
                COMMENTS.at[c, 'Store'] = store
                COMMENTS.at[c, 'Change Type'] = changeType
                COMMENTS.at[c, 'Comment'] = commentNM
    if FULLDF.iloc[i]['HIERFLG'] == '2':
        if FULLDF.iloc[i]['COM_DST_MGR_ID'] != FULLDF.iloc[i]['TSM Ignition']:
            comment = 'Changing: ' + FULLDF.iloc[i]['COM_DST_MGR_ID'] + ' for ' + FULLDF.iloc[i]['TSM Ignition']
            store = FULLDF.iloc[i]['STORE_ID']
            changeType = 'TSM Change'
            COMMENTS.at[c, 'Store'] = store
            COMMENTS.at[c, 'Change Type'] = changeType
            COMMENTS.at[c, 'Comment'] = comment
            c += 1
            commentNM = 'Changing: ' + FULLDF.iloc[i]['COM_DST_MGR_NAME'] + ' for ' + FULLDF.iloc[i]['TSM']
            COMMENTS.at[c, 'Store'] = store
            COMMENTS.at[c, 'Change Type'] = changeType
            COMMENTS.at[c, 'Comment'] = commentNM

    i += 1

#CHANGE CELL VALUES#
FULLDF['DISTRICT_MGR_EMP_ID_x'].mask(FULLDF['DISTRICT_MGR_EMP_ID_x'] != FULLDF['DISTRICT_MGR_EMP_ID_y'], FULLDF['DISTRICT_MGR_EMP_ID_y'], inplace=True)
FULLDF['DISTRICT_MGR_NAME_x'].mask(FULLDF['DISTRICT_MGR_NAME_x'] != FULLDF['DISTRICT_MGR_NAME_y'], FULLDF['DISTRICT_MGR_NAME_y'], inplace=True)
FULLDF['COM_DST_MGR_ID'].mask(FULLDF['COM_DST_MGR_ID'] != FULLDF['TSM Ignition'], FULLDF['TSM Ignition'], inplace=True)
FULLDF['COM_DST_MGR_NAME'].mask(FULLDF['COM_DST_MGR_NAME'] != FULLDF['TSM'], FULLDF['TSM'], inplace=True)
FULLDF['DISTRICT_MGR_EMP_ID_x'].mask(FULLDF['HIERFLG'] == '2', FULLDF['TSM Ignition'], inplace=True)
FULLDF['DISTRICT_MGR_NAME_x'].mask(FULLDF['HIERFLG'] == '2', FULLDF['TSM'], inplace=True)
#END CHANGE CELL VALUES#
#END OF LOGIC FOR DM AND TSM UPDATE#

#STORE OPEN DATE LOGIC#
i = 0

for x in FULLDF['STORE_ID']:
    stored = FULLDF.iloc[i]['STORE_OPENED_DATE']
    datetimeobject = datetime.strptime(stored, '%Y%m%d')
    NDays = currD - datetimeobject

    if NDays.days >= 730:
        FULLDF.at[i, 'SAME_STORE_FLAG']='S'
        #print('Mayor a dos años' + ' : ' + str(NDays.days))
    elif NDays.days <= 730 and NDays.days >= 0:
        FULLDF.at[i, 'SAME_STORE_FLAG']='N'
        #print('Menor a dos años' + ' : ' + str(NDays.days))
    else: 
        #print('fecha negativa' + ' : ' + str(NDays.days))
        FULLDF.at[i,'OPEN_CODE']='R'
        FULLDF.at[i,'SAME_STORE_FLAG']='N'
        FULLDF.at[i,'COMM_SALES_STORE_FLAG']='N'
    i += 1
#END STORE OPEN DATE LOGIC#

#LENGHT OF COLUMNS#
#HIERFLG#
if ("''" in FULLDF['HIERFLG'].values) == False:
    if max(FULLDF['HIERFLG'].str.len()) > 1:
        comment ='The HIERFLG column exceded limit 1 - found: ' + str(max(FULLDF['HIERFLG'].str.len()))
        changeType = 'HIERFLG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['HIERFLG'].values) == True:
    comment ='The HIERFLG column contains blank values'
    changeType = 'HIERFLG'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['HIERFLG'].str.len()) > 3:
        comment ='The HIERFLG column exceded limit 1 - found: ' + str(max(FULLDF['HIERFLG'].str.len()) - 2)
        changeType = 'HIERFLG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END HIERFLG#

#JOINKEY#
if ("''" in FULLDF['JOINKEY'].values) == False:
    if max(FULLDF['JOINKEY'].str.len()) > 5:
        comment ='The JOINKEY column exceded limit 5 - found: ' + str(max(FULLDF['JOINKEY'].str.len()))
        changeType = 'JOINKEY'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['JOINKEY'].values) == True:
    comment ='The JOINKEY column contains blank values'
    changeType = 'JOINKEY'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['JOINKEY'].str.len()) > 5:
        comment ='The JOINKEY column exceded limit 5 - found: ' + str(max(FULLDF['JOINKEY'].str.len()) - 2)
        changeType = 'JOINKEY'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END JOINKEY#
#HIERFLGDESC#
if ("''" in FULLDF['HIERFLGDESC'].values) == False:
    if max(FULLDF['HIERFLGDESC'].str.len()) > 5:
        comment ='The HIERFLGDESC column exceded limit 5 - found: ' + str(max(FULLDF['HIERFLGDESC'].str.len()))
        changeType = 'HIERFLGDESC'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['HIERFLGDESC'].values) == True:
    comment ='The HIERFLGDESC column contains blank values'
    changeType = 'HIERFLGDESC'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['HIERFLGDESC'].str.len()) > 5:
        comment ='The HIERFLGDESC column exceded limit 5 - found: ' + str(max(FULLDF['HIERFLGDESC'].str.len()))
        changeType = 'HIERFLGDESC'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END HIERFLG DESC#
#DIVISION_ID#
if ("''" in FULLDF['DIVISION_ID'].values) == False:
    if max(FULLDF['DIVISION_ID'].str.len()) > 2:
        comment ='The DIVISION_ID column exceded limit 2 - found: ' + str(max(FULLDF['DIVISION_ID'].str.len()))
        changeType = 'DIVISION_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DIVISION_ID'].values) == True:
    comment ='The DIVISION_ID column contains blank values'
    changeType = 'DIVISION_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DIVISION_ID'].str.len()) > 2:
        comment ='The DIVISION_ID column exceded limit 2 - found: ' + str(max(FULLDF['DIVISION_ID'].str.len())-2)
        changeType = 'DIVISION_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DIVISION_ID#

#REGION_ID#
if ("''" in FULLDF['REGION_ID'].values) == False:
    if max(FULLDF['REGION_ID'].str.len()) > 2:
        comment ='The REGION_ID column exceded limit 2 - found: ' + str(max(FULLDF['REGION_ID'].str.len()))
        changeType = 'REGION_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['REGION_ID'].values) == True:
    comment ='The REGION_ID column contains blank values'
    changeType = 'REGION_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['REGION_ID'].str.len()) > 3:
        comment ='The REGION_ID column exceded limit 2 - found: ' + str(max(FULLDF['REGION_ID'].str.len())-2)
        changeType = 'REGION_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1


#END REGION_ID#


#END LENGHT OF COLUMNS#
#END OF LOGIC#


#PREPARE FINAL FILES TO SAVE#
FULLDF = FULLDF.replace('Á', 'A', regex=True)
FULLDF = FULLDF.replace('É', 'E', regex=True)
FULLDF = FULLDF.replace('Í', 'I', regex=True)
FULLDF = FULLDF.replace('Ó', 'O', regex=True)
FULLDF = FULLDF.replace('Ú', 'U', regex=True)
#######################################################
FULLDF = FULLDF.replace('Â', 'A', regex=True)
FULLDF = FULLDF.replace('Ê', 'E', regex=True)
FULLDF = FULLDF.replace('Ô', 'O', regex=True)
#######################################################
FULLDF = FULLDF.replace('Ã', 'A', regex=True)
FULLDF = FULLDF.replace('Õ', 'O', regex=True)
#######################################################
FULLDF = FULLDF.replace('À', 'A', regex=True)
FULLDF = FULLDF.replace('Ç', 'C', regex=True)
FULLDF = FULLDF.replace("''", '')
CND = COMMENTS.drop_duplicates()
FULLDF.rename(columns={'STORE_NAME_x':'STORE_NAME', 'STORE_MGR_EMP_ID_x': 'STORE_MGR_EMP_ID', 'STORE_MGR_NAME_x': 'STORE_MGR_NAME', 'COMM_SALES_MGR_EMP_ID_x': 'COMM_SALES_MGR_EMP_ID', 'COMM_SALES_MGR_NAME_x': 'COMM_SALES_MGR_NAME'}, inplace=True)
FULLDF.rename(columns={'DISTRICT_MGR_EMP_ID_x': 'DISTRICT_MGR_EMP_ID', 'DISTRICT_MGR_EMAIL_ID_x': 'DISTRICT_MGR_EMAIL_ID'}, inplace=True)

#END PREPARING FINAL FILES TO SAVE#

# SAVE THE FILES#
final_path = "C:\\Users\\dsegovia\\OneDrive - AutoZone Parts, Inc\\Documents\\Hierarchy tool"
CND.to_csv(final_path + "\\Comments.csv", index=False)
FULLDF.to_csv(final_path + "\\ff_bsto_store_hierarchy_detail.csv", index=False, encoding='ISO-8859-9')
print("process ended")