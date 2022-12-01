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
FULLDF['DISTRICT_MGR_EMP_ID_y'] = FULLDF['DISTRICT_MGR_EMP_ID_y'].replace('.0' "''")
FULLDF['STORE_OPEN_OR_CLOSE'] = FULLDF['STORE_OPEN_OR_CLOSE'].replace('ABERTA', 'O', regex=True)
FULLDF['STORE_OPEN_OR_CLOSE'] = FULLDF['STORE_OPEN_OR_CLOSE'].replace('FECHADA', 'R', regex=True)
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
            comment = 'Changing: ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'] + ' for ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_y'][:-2]
        elif FULLDF.iloc[i]['STORE_MGR_EMP_ID_y'] == "''":
            comment = 'Changing: ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'][:-2] + ' for ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_y']
        else:
            comment = 'Changing: ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_x'][:-2] + ' for ' + FULLDF.iloc[i]['STORE_MGR_EMP_ID_y'][:-2] 
        
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
        if FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_x'] != FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_y'][:-2]:
            comment = 'Changing: ' + FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_x'] + ' for ' + FULLDF.iloc[i]['DISTRICT_MGR_EMP_ID_y'][:-2]
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


i = 0
for x in FULLDF['STORE_ID']:
    if FULLDF.iloc[i]['OPEN_CODE'] != FULLDF.iloc[i]['STORE_OPEN_OR_CLOSE']:
        c += 1
        comment = 'Changing Open code from: ' + FULLDF.iloc[i]['OPEN_CODE'] + 'to ' + FULLDF.iloc[i]['STORE_OPEN_OR_CLOSE']
        store = FULLDF.iloc[i]['STORE_ID']
        changeType = 'Open Code'

        COMMENTS.at[c, 'Store'] = store
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment

    i += 1

FULLDF['OPEN_CODE'].mask(FULLDF['OPEN_CODE'] != FULLDF['STORE_OPEN_OR_CLOSE'], FULLDF['STORE_OPEN_OR_CLOSE'], inplace=True)


#STORE OPEN DATE LOGIC#
i = 0

for x in FULLDF['STORE_ID']:
    stored = FULLDF.iloc[i]['STORE_OPENED_DATE']
    datetimeobject = datetime.strptime(stored, '%Y%m%d')
    NDays = currD - datetimeobject

    if NDays.days >= 365:
        FULLDF.at[i, 'SAME_STORE_FLAG']='S'
        #print('Mayor a dos años' + ' : ' + str(NDays.days))
    elif NDays.days <= 365 and NDays.days >= 0:
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
    if max(FULLDF['HIERFLG'].str.len()) > 1:
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
    if max(FULLDF['REGION_ID'].str.len()) > 2:
        comment ='The REGION_ID column exceded limit 2 - found: ' + str(max(FULLDF['REGION_ID'].str.len())-2)
        changeType = 'REGION_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END REGION_ID#
#DISTRICT_ID#
if ("''" in FULLDF['DISTRICT_ID'].values) == False:
    if max(FULLDF['DISTRICT_ID'].str.len()) > 3:
        comment ='The DISTRICT_ID column exceded limit 3 - found: ' + str(max(FULLDF['DISTRICT_ID'].str.len()))
        changeType = 'DISTRICT_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DISTRICT_ID'].values) == True:
    comment ='The DISTRICT_ID column contains blank values'
    changeType = 'DISTRICT_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DISTRICT_ID'].str.len()) > 3:
        comment ='The DISTRICT_ID column exceded limit 3 - found: ' + str(max(FULLDF['DISTRICT_ID'].str.len())-2)
        changeType = 'DISTRICT_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DISTRICT_ID#
#STORE_NAME_x#
if ("''" in FULLDF['STORE_NAME_x'].values) == False:
    if max(FULLDF['STORE_NAME_x'].str.len()) > 24:
        comment ='The STORE_NAME_x column exceded limit 24 - found: ' +  str(max(FULLDF['STORE_NAME_x'].str.len()))
        changeType = 'STORE_NAME_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STORE_NAME_x'].values) == True:
    comment ='The STORE_NAME_x column contains blank values'
    changeType = 'STORE_NAME_x'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 8
    if max(FULLDF['STORE_NAME_x'].str.len()) > 24:
        comment ='The STORE_NAME_x column exceded limit 24 - found: ' +  str(max(FULLDF['STORE_NAME_x'].str.len()))
        changeType = 'STORE_NAME_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 8
#END STORE_NAME_x#
#STORE_ADDR#
if ("''" in FULLDF['STORE_ADDR'].values) == False:
    if max(FULLDF['STORE_ADDR'].str.len()) > 66:
        comment ='The STORE_ADDR column exceded limit 66 - found: ' +  str(max(FULLDF['STORE_ADDR'].str.len()))
        changeType = 'STORE_ADDR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 9

if ("''" in FULLDF['STORE_ADDR'].values) == True:
    comment ='The STORE_ADDR column contains blank values'
    changeType = 'STORE_ADDR'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 9
    if max(FULLDF['STORE_ADDR'].str.len()) > 66:
        comment ='The STORE_ADDR column exceded limit 66 - found: ' +  str(max(FULLDF['STORE_ADDR'].str.len()))
        changeType = 'STORE_ADDR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 9
#END STORE_ADDR#
#CITY_NAME#
if ("''" in FULLDF['CITY_NAME'].values) == False:
    if max(FULLDF['CITY_NAME'].str.len()) > 21:
        comment ='The CITY_NAME column exceded limit 21 - found: ' +  str(max(FULLDF['CITY_NAME'].str.len()))
        changeType = 'CITY_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['CITY_NAME'].values) == True:
    comment ='The CITY_NAME column contains blank values'
    changeType = 'CITY_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['CITY_NAME'].str.len()) > 21:
        comment ='The CITY_NAME column exceded limit 21 - found: ' +  str(max(FULLDF['CITY_NAME'].str.len()))
        changeType = 'CITY_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END CITY_NAME#
#STATE_CODE#
if ("''" in FULLDF['STATE_CODE'].values) == False:
    if max(FULLDF['STATE_CODE'].str.len()) > 2:
        comment ='The STATE_CODE column exceded limit 2 - found: ' +  str(max(FULLDF['STATE_CODE'].str.len()))
        changeType = 'STATE_CODE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STATE_CODE'].values) == True:
    comment ='The STATE_CODE column contains blank values'
    changeType = 'STATE_CODE'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['STATE_CODE'].str.len()) > 2:
        comment ='The STATE_CODE column exceded limit 2 - found: ' +  str(max(FULLDF['STATE_CODE'].str.len()))
        changeType = 'STATE_CODE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END STATE_CODE#
#POSTAL_CODE#
if ("''" in FULLDF['POSTAL_CODE'].values) == False:
    if max(FULLDF['POSTAL_CODE'].str.len()) > 9:
        comment ='The POSTAL_CODE column exceded limit 9 - found: ' +  str(max(FULLDF['POSTAL_CODE'].str.len()))
        changeType = 'POSTAL_CODE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['POSTAL_CODE'].values) == True:
    comment ='The POSTAL_CODE column contains blank values'
    changeType = 'POSTAL_CODE'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['POSTAL_CODE'].str.len()) > 9:
        comment ='The POSTAL_CODE column exceded limit 9 - found: ' +  str(max(FULLDF['POSTAL_CODE'].str.len()))
        changeType = 'POSTAL_CODE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END POSTAL_CODE#
#COUNTY_NAME#
if ("''" in FULLDF['COUNTY_NAME'].values) == False:
    if max(FULLDF['COUNTY_NAME'].str.len()) > 6:
        comment ='The COUNTY_NAME column exceded limit 6 - found: ' +  str(max(FULLDF['COUNTY_NAME'].str.len()))
        changeType = 'COUNTY_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COUNTY_NAME'].values) == True:
    comment ='The COUNTY_NAME column contains blank values'
    changeType = 'COUNTY_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COUNTY_NAME'].str.len()) > 6:
        comment ='The COUNTY_NAME column exceded limit 6 - found: ' +  str(max(FULLDF['COUNTY_NAME'].str.len()))
        changeType = 'COUNTY_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COUNTY_NAME#
#COUNTRY_CODE#
if ("''" in FULLDF['COUNTRY_CODE'].values) == False:
    if max(FULLDF['COUNTRY_CODE'].str.len()) > 3:
        comment ='The COUNTRY_CODE column exceded limit 3 - found: ' +  str(max(FULLDF['COUNTRY_CODE'].str.len()))
        changeType = 'COUNTRY_CODE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COUNTRY_CODE'].values) == True:
    comment ='The COUNTRY_CODE column contains blank values'
    changeType = 'COUNTRY_CODE'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COUNTRY_CODE'].str.len()) > 3:
        comment ='The COUNTRY_CODE column exceded limit 3 - found: ' +  str(max(FULLDF['COUNTRY_CODE'].str.len()))
        changeType = 'COUNTRY_CODE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COUNTRY_CODE#
#STORE_PHONE_NBR#
if ("''" in FULLDF['STORE_PHONE_NBR'].values) == False:
    if max(FULLDF['STORE_PHONE_NBR'].str.len()) > 14:
        comment ='The STORE_PHONE_NBR column exceded limit 14 - found: ' +  str(max(FULLDF['STORE_PHONE_NBR'].str.len()))
        changeType = 'STORE_PHONE_NBR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STORE_PHONE_NBR'].values) == True:
    comment ='The STORE_PHONE_NBR column contains blank values'
    changeType = 'STORE_PHONE_NBR'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['STORE_PHONE_NBR'].str.len()) > 14:
        comment ='The STORE_PHONE_NBR column exceded limit 14 - found: ' +  str(max(FULLDF['STORE_PHONE_NBR'].str.len()))
        changeType = 'STORE_PHONE_NBR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END STORE_PHONE_NBR#
#STORE_MGR_EMP_ID_x#
if ("''" in FULLDF['STORE_MGR_EMP_ID_x'].values) == False:
    if max(FULLDF['STORE_MGR_EMP_ID_x'].str.len()) > 8:
        comment ='The STORE_MGR_EMP_ID_x column exceded limit 8 - found: ' +  str(max(FULLDF['STORE_MGR_EMP_ID_x'].str.len()))
        changeType = 'STORE_MGR_EMP_ID_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STORE_MGR_EMP_ID_x'].values) == True:
    comment ='The STORE_MGR_EMP_ID_x column contains blank values'
    changeType = 'STORE_MGR_EMP_ID_x'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['STORE_MGR_EMP_ID_x'].str.len()) > 10:
        comment ='The STORE_MGR_EMP_ID_x column exceded limit 8 - found: ' +  str(max(FULLDF['STORE_MGR_EMP_ID_x'].str.len()))
        changeType = 'STORE_MGR_EMP_ID_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END STORE_MGR_EMP_ID_x#
#STORE_MGR_NAME_x#
if ("''" in FULLDF['STORE_MGR_NAME_x'].values) == False:
    if max(FULLDF['STORE_MGR_NAME_x'].str.len()) > 40:
        comment ='The STORE_MGR_NAME_x column exceded limit 40 - found: ' +  str(max(FULLDF['STORE_MGR_NAME_x'].str.len()))
        changeType = 'STORE_MGR_NAME_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STORE_MGR_NAME_x'].values) == True:
    comment ='The STORE_MGR_NAME_x column contains blank values'
    changeType = 'STORE_MGR_NAME_x'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['STORE_MGR_NAME_x'].str.len()) > 40:
        comment ='The STORE_MGR_NAME_x column exceded limit 40 - found: ' +  str(max(FULLDF['STORE_MGR_NAME_x'].str.len()))
        changeType = 'STORE_MGR_NAME_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END STORE_MGR_NAME_x#
#COMM_SALES_MGR_EMP_ID_x#
if ("''" in FULLDF['COMM_SALES_MGR_EMP_ID_x'].values) == False:
    if max(FULLDF['COMM_SALES_MGR_EMP_ID_x'].str.len()) > 8:
        comment ='The COMM_SALES_MGR_EMP_ID_x column exceded limit 8 - found: ' +  str(max(FULLDF['COMM_SALES_MGR_EMP_ID_x'].str.len()))
        changeType = 'COMM_SALES_MGR_EMP_ID_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COMM_SALES_MGR_EMP_ID_x'].values) == True:
    comment ='The COMM_SALES_MGR_EMP_ID_x column contains blank values'
    changeType = 'COMM_SALES_MGR_EMP_ID_x'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COMM_SALES_MGR_EMP_ID_x'].str.len()) > 10:
        comment ='The COMM_SALES_MGR_EMP_ID_x column exceded limit 8 - found: ' +  str(max(FULLDF['COMM_SALES_MGR_EMP_ID_x'].str.len()))
        changeType = 'COMM_SALES_MGR_EMP_ID_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COMM_SALES_MGR_EMP_ID_x#
#COMM_SALES_MGR_NAME_x#
if ("''" in FULLDF['COMM_SALES_MGR_NAME_x'].values) == False:
    if max(FULLDF['COMM_SALES_MGR_NAME_x'].str.len()) > 40:
        comment ='The COMM_SALES_MGR_NAME_x column exceded limit 40 - found: ' +  str(max(FULLDF['COMM_SALES_MGR_NAME_x'].str.len()))
        changeType = 'COMM_SALES_MGR_NAME_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COMM_SALES_MGR_NAME_x'].values) == True:
    comment ='The COMM_SALES_MGR_NAME_x column contains blank values'
    changeType = 'COMM_SALES_MGR_NAME_x'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COMM_SALES_MGR_NAME_x'].str.len()) > 40:
        comment ='The COMM_SALES_MGR_NAME_x column exceded limit 40 - found: ' +  str(max(FULLDF['COMM_SALES_MGR_NAME_x'].str.len()))
        changeType = 'COMM_SALES_MGR_NAME_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COMM_SALES_MGR_NAME_x#
#STORE_EMAIL_ID#
if ("''" in FULLDF['STORE_EMAIL_ID'].values) == False:
    if max(FULLDF['STORE_EMAIL_ID'].str.len()) > 25:
        comment ='The STORE_EMAIL_ID column exceded limit 25 - found: ' +  str(max(FULLDF['STORE_EMAIL_ID'].str.len()))
        changeType = 'STORE_EMAIL_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STORE_EMAIL_ID'].values) == True:
    comment ='The STORE_EMAIL_ID column contains blank values'
    changeType = 'STORE_EMAIL_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['STORE_EMAIL_ID'].str.len()) > 25:
        comment ='The STORE_EMAIL_ID column exceded limit 25 - found: ' +  str(max(FULLDF['STORE_EMAIL_ID'].str.len()))
        changeType = 'STORE_EMAIL_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END STORE_EMAIL_ID#
#OPEN_CODE#
if ("''" in FULLDF['OPEN_CODE'].values) == False:
    if max(FULLDF['OPEN_CODE'].str.len()) > 1:
        comment ='The OPEN_CODE column exceded limit 1 - found: ' +  str(max(FULLDF['OPEN_CODE'].str.len()))
        changeType = 'OPEN_CODE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['OPEN_CODE'].values) == True:
    comment ='The OPEN_CODE column contains blank values'
    changeType = 'OPEN_CODE'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['OPEN_CODE'].str.len()) > 1:
        comment ='The OPEN_CODE column exceded limit 1 - found: ' +  str(max(FULLDF['OPEN_CODE'].str.len()))
        changeType = 'OPEN_CODE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END OPEN_CODE#
#STORE_OPENED_DATE#
if ("''" in FULLDF['STORE_OPENED_DATE'].values) == False:
    if max(FULLDF['STORE_OPENED_DATE'].str.len()) > 8:
        comment ='The STORE_OPENED_DATE column exceded limit 8 - found: ' +  str(max(FULLDF['STORE_OPENED_DATE'].str.len()))
        changeType = 'STORE_OPENED_DATE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STORE_OPENED_DATE'].values) == True:
    comment ='The STORE_OPENED_DATE column contains blank values'
    changeType = 'STORE_OPENED_DATE'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['STORE_OPENED_DATE'].str.len()) > 8:
        comment ='The STORE_OPENED_DATE column exceded limit 8 - found: ' +  str(max(FULLDF['STORE_OPENED_DATE'].str.len()))
        changeType = 'STORE_OPENED_DATE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END STORE_OPENED_DATE#
#STORE_OPENED_YYMD#
if ("''" in FULLDF['STORE_OPENED_YYMD'].values) == False:
    if max(FULLDF['STORE_OPENED_YYMD'].str.len()) > 8:
        comment ='The STORE_OPENED_YYMD column exceded limit 8 - found: ' +  str(max(FULLDF['STORE_OPENED_YYMD'].str.len()))
        changeType = 'STORE_OPENED_YYMD'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STORE_OPENED_YYMD'].values) == True:
    comment ='The STORE_OPENED_YYMD column contains blank values'
    changeType = 'STORE_OPENED_YYMD'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['STORE_OPENED_YYMD'].str.len()) > 8:
        comment ='The STORE_OPENED_YYMD column exceded limit 8 - found: ' +  str(max(FULLDF['STORE_OPENED_YYMD'].str.len()))
        changeType = 'STORE_OPENED_YYMD'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END STORE_OPENED_YYMD#
#STORE_OPENED_FISCAL_YEAR#
if ("''" in FULLDF['STORE_OPENED_FISCAL_YEAR'].values) == False:
    if max(FULLDF['STORE_OPENED_FISCAL_YEAR'].str.len()) > 4:
        comment ='The STORE_OPENED_FISCAL_YEAR column exceded limit 4 - found: ' +  str(max(FULLDF['STORE_OPENED_FISCAL_YEAR'].str.len()))
        changeType = 'STORE_OPENED_FISCAL_YEAR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['STORE_OPENED_FISCAL_YEAR'].values) == True:
    comment ='The STORE_OPENED_FISCAL_YEAR column contains blank values'
    changeType = 'STORE_OPENED_FISCAL_YEAR'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['STORE_OPENED_FISCAL_YEAR'].str.len()) > 4:
        comment ='The STORE_OPENED_FISCAL_YEAR column exceded limit 4 - found: ' +  str(max(FULLDF['STORE_OPENED_FISCAL_YEAR'].str.len()))
        changeType = 'STORE_OPENED_FISCAL_YEAR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END STORE_OPENED_FISCAL_YEAR#
#SAME_STORE_FLAG#
if ("''" in FULLDF['SAME_STORE_FLAG'].values) == False:
    if max(FULLDF['SAME_STORE_FLAG'].str.len()) > 1:
        comment ='The SAME_STORE_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['SAME_STORE_FLAG'].str.len()))
        changeType = 'SAME_STORE_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SAME_STORE_FLAG'].values) == True:
    comment ='The SAME_STORE_FLAG column contains blank values'
    changeType = 'SAME_STORE_FLAG'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SAME_STORE_FLAG'].str.len()) > 1:
        comment ='The SAME_STORE_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['SAME_STORE_FLAG'].str.len()))
        changeType = 'SAME_STORE_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SAME_STORE_FLAG#
#COMM_SALES_FLAG#
if ("''" in FULLDF['COMM_SALES_FLAG'].values) == False:
    if max(FULLDF['COMM_SALES_FLAG'].str.len()) > 1:
        comment ='The COMM_SALES_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['COMM_SALES_FLAG'].str.len()))
        changeType = 'COMM_SALES_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COMM_SALES_FLAG'].values) == True:
    comment ='The COMM_SALES_FLAG column contains blank values'
    changeType = 'COMM_SALES_FLAG'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COMM_SALES_FLAG'].str.len()) > 1:
        comment ='The COMM_SALES_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['COMM_SALES_FLAG'].str.len()))
        changeType = 'COMM_SALES_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COMM_SALES_FLAG#
#COMM_SALES_START_DATE#
if ("''" in FULLDF['COMM_SALES_START_DATE'].values) == False:
    if max(FULLDF['COMM_SALES_START_DATE'].str.len()) > 8:
        comment ='The COMM_SALES_START_DATE column exceded limit 8 - found: ' +  str(max(FULLDF['COMM_SALES_START_DATE'].str.len()))
        changeType = 'COMM_SALES_START_DATE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COMM_SALES_START_DATE'].values) == True:
    comment ='The COMM_SALES_START_DATE column contains blank values'
    changeType = 'COMM_SALES_START_DATE'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COMM_SALES_START_DATE'].str.len()) > 8:
        comment ='The COMM_SALES_START_DATE column exceded limit 8 - found: ' +  str(max(FULLDF['COMM_SALES_START_DATE'].str.len()))
        changeType = 'COMM_SALES_START_DATE'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COMM_SALES_START_DATE#
#COMM_SAME_STORE_FLAG#
if ("''" in FULLDF['COMM_SAME_STORE_FLAG'].values) == False:
    if max(FULLDF['COMM_SAME_STORE_FLAG'].str.len()) > 1:
        comment ='The COMM_SAME_STORE_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['COMM_SAME_STORE_FLAG'].str.len()))
        changeType = 'COMM_SAME_STORE_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COMM_SAME_STORE_FLAG'].values) == True:
    comment ='The COMM_SAME_STORE_FLAG column contains blank values'
    changeType = 'COMM_SAME_STORE_FLAG'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COMM_SAME_STORE_FLAG'].str.len()) > 1:
        comment ='The COMM_SAME_STORE_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['COMM_SAME_STORE_FLAG'].str.len()))
        changeType = 'COMM_SAME_STORE_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COMM_SAME_STORE_FLAG#
#COMM_STORE_OPENED_FISCAL_YEAR#
if ("''" in FULLDF['COMM_STORE_OPENED_FISCAL_YEAR'].values) == False:
    if max(FULLDF['COMM_STORE_OPENED_FISCAL_YEAR'].str.len()) > 4:
        comment ='The COMM_STORE_OPENED_FISCAL_YEAR column exceded limit 4 - found: ' +  str(max(FULLDF['COMM_STORE_OPENED_FISCAL_YEAR'].str.len()))
        changeType = 'COMM_STORE_OPENED_FISCAL_YEAR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COMM_STORE_OPENED_FISCAL_YEAR'].values) == True:
    comment ='The COMM_STORE_OPENED_FISCAL_YEAR column contains blank values'
    changeType = 'COMM_STORE_OPENED_FISCAL_YEAR'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COMM_STORE_OPENED_FISCAL_YEAR'].str.len()) > 4:
        comment ='The COMM_STORE_OPENED_FISCAL_YEAR column exceded limit 4 - found: ' +  str(max(FULLDF['COMM_STORE_OPENED_FISCAL_YEAR'].str.len()))
        changeType = 'COMM_STORE_OPENED_FISCAL_YEAR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COMM_STORE_OPENED_FISCAL_YEAR#
#COMM_PHONE_NBR#
if ("''" in FULLDF['COMM_PHONE_NBR'].values) == False:
    if max(FULLDF['COMM_PHONE_NBR'].str.len()) > 14:
        comment ='The COMM_PHONE_NBR column exceded limit 14 - found: ' +  str(max(FULLDF['COMM_PHONE_NBR'].str.len()))
        changeType = 'COMM_PHONE_NBR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COMM_PHONE_NBR'].values) == True:
    comment ='The COMM_PHONE_NBR column contains blank values'
    changeType = 'COMM_PHONE_NBR'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COMM_PHONE_NBR'].str.len()) > 14:
        comment ='The COMM_PHONE_NBR column exceded limit 14 - found: ' +  str(max(FULLDF['COMM_PHONE_NBR'].str.len()))
        changeType = 'COMM_PHONE_NBR'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COMM_PHONE_NBR#
#HUB_FLAG#
if ("''" in FULLDF['HUB_FLAG'].values) == False:
    if max(FULLDF['HUB_FLAG'].str.len()) > 1:
        comment ='The HUB_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['HUB_FLAG'].str.len()))
        changeType = 'HUB_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['HUB_FLAG'].values) == True:
    comment ='The HUB_FLAG column contains blank values'
    changeType = 'HUB_FLAG'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['HUB_FLAG'].str.len()) > 1:
        comment ='The HUB_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['HUB_FLAG'].str.len()))
        changeType = 'HUB_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END HUB_FLAG#

#MEGA_HUB_FLAG#
if ("''" in FULLDF['MEGA_HUB_FLAG'].values) == False:
    if max(FULLDF['MEGA_HUB_FLAG'].str.len()) > 1:
        comment ='The MEGA_HUB_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['MEGA_HUB_FLAG'].str.len()))
        changeType = 'MEGA_HUB_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['MEGA_HUB_FLAG'].values) == True:
    comment ='The MEGA_HUB_FLAG column contains blank values'
    changeType = 'MEGA_HUB_FLAG'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['MEGA_HUB_FLAG'].str.len()) > 1:
        comment ='The MEGA_HUB_FLAG column exceded limit 1 - found: ' +  str(max(FULLDF['MEGA_HUB_FLAG'].str.len()))
        changeType = 'MEGA_HUB_FLAG'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END MEGA_HUB_FLAG#
#DIVISION_ID.1#
if ("''" in FULLDF['DIVISION_ID.1'].values) == False:
    if max(FULLDF['DIVISION_ID.1'].str.len()) > 2:
        comment ='The DIVISION_ID.1 column exceded limit 2 - found: ' +  str(max(FULLDF['DIVISION_ID.1'].str.len()))
        changeType = 'DIVISION_ID.1'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DIVISION_ID.1'].values) == True:
    comment ='The DIVISION_ID.1 column contains blank values'
    changeType = 'DIVISION_ID.1'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DIVISION_ID.1'].str.len()) > 2:
        comment ='The DIVISION_ID.1 column exceded limit 2 - found: ' +  str(max(FULLDF['DIVISION_ID.1'].str.len()))
        changeType = 'DIVISION_ID.1'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DIVISION_ID.1#
#DIVISION_VP_EMP_ID#
if ("''" in FULLDF['DIVISION_VP_EMP_ID'].values) == False:
    if max(FULLDF['DIVISION_VP_EMP_ID'].str.len()) > 8:
        comment ='The DIVISION_VP_EMP_ID column exceded limit 8 - found: ' +  str(max(FULLDF['DIVISION_VP_EMP_ID'].str.len()))
        changeType = 'DIVISION_VP_EMP_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DIVISION_VP_EMP_ID'].values) == True:
    comment ='The DIVISION_VP_EMP_ID column contains blank values'
    changeType = 'DIVISION_VP_EMP_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DIVISION_VP_EMP_ID'].str.len()) > 8:
        comment ='The DIVISION_VP_EMP_ID column exceded limit 8 - found: ' +  str(max(FULLDF['DIVISION_VP_EMP_ID'].str.len()))
        changeType = 'DIVISION_VP_EMP_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DIVISION_VP_EMP_ID#
#DIVISION_VP_NAME#
if ("''" in FULLDF['DIVISION_VP_NAME'].values) == False:
    if max(FULLDF['DIVISION_VP_NAME'].str.len()) > 13:
        comment ='The DIVISION_VP_NAME column exceded limit 13 - found: ' +  str(max(FULLDF['DIVISION_VP_NAME'].str.len()))
        changeType = 'DIVISION_VP_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DIVISION_VP_NAME'].values) == True:
    comment ='The DIVISION_VP_NAME column contains blank values'
    changeType = 'DIVISION_VP_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DIVISION_VP_NAME'].str.len()) > 13:
        comment ='The DIVISION_VP_NAME column exceded limit 13 - found: ' +  str(max(FULLDF['DIVISION_VP_NAME'].str.len()))
        changeType = 'DIVISION_VP_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DIVISION_VP_NAME#
#DIVISION_VP_EMAIL_ID#
if ("''" in FULLDF['DIVISION_VP_EMAIL_ID'].values) == False:
    if max(FULLDF['DIVISION_VP_EMAIL_ID'].str.len()) > 26:
        comment ='The DIVISION_VP_EMAIL_ID column exceded limit 26 - found: ' +  str(max(FULLDF['DIVISION_VP_EMAIL_ID'].str.len()))
        changeType = 'DIVISION_VP_EMAIL_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DIVISION_VP_EMAIL_ID'].values) == True:
    comment ='The DIVISION_VP_EMAIL_ID column contains blank values'
    changeType = 'DIVISION_VP_EMAIL_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DIVISION_VP_EMAIL_ID'].str.len()) > 26:
        comment ='The DIVISION_VP_EMAIL_ID column exceded limit 26 - found: ' +  str(max(FULLDF['DIVISION_VP_EMAIL_ID'].str.len()))
        changeType = 'DIVISION_VP_EMAIL_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DIVISION_VP_EMAIL_ID#
#REGION_ID.1#
if ("''" in FULLDF['REGION_ID.1'].values) == False:
    if max(FULLDF['REGION_ID.1'].str.len()) > 2:
        comment ='The REGION_ID.1 column exceded limit 2 - found: ' +  str(max(FULLDF['REGION_ID.1'].str.len()))
        changeType = 'REGION_ID.1'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['REGION_ID.1'].values) == True:
    comment ='The REGION_ID.1 column contains blank values'
    changeType = 'REGION_ID.1'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['REGION_ID.1'].str.len()) > 2:
        comment ='The REGION_ID.1 column exceded limit 2 - found: ' +  str(max(FULLDF['REGION_ID.1'].str.len()))
        changeType = 'REGION_ID.1'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END REGION_ID.1#
#REGION_NAME#
if ("''" in FULLDF['REGION_NAME'].values) == False:
    if max(FULLDF['REGION_NAME'].str.len()) > 12:
        comment ='The REGION_NAME column exceded limit 12 - found: ' +  str(max(FULLDF['REGION_NAME'].str.len()))
        changeType = 'REGION_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['REGION_NAME'].values) == True:
    comment ='The REGION_NAME column contains blank values'
    changeType = 'REGION_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['REGION_NAME'].str.len()) > 12:
        comment ='The REGION_NAME column exceded limit 12 - found: ' +  str(max(FULLDF['REGION_NAME'].str.len()))
        changeType = 'REGION_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END REGION_NAME#
#REGION_MGR_EMP_ID#
if ("''" in FULLDF['REGION_MGR_EMP_ID'].values) == False:
    if max(FULLDF['REGION_MGR_EMP_ID'].str.len()) > 8:
        comment ='The REGION_MGR_EMP_ID column exceded limit 8 - found: ' +  str(max(FULLDF['REGION_MGR_EMP_ID'].str.len()))
        changeType = 'REGION_MGR_EMP_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['REGION_MGR_EMP_ID'].values) == True:
    comment ='The REGION_MGR_EMP_ID column contains blank values'
    changeType = 'REGION_MGR_EMP_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['REGION_MGR_EMP_ID'].str.len()) > 8:
        comment ='The REGION_MGR_EMP_ID column exceded limit 8 - found: ' +  str(max(FULLDF['REGION_MGR_EMP_ID'].str.len()))
        changeType = 'REGION_MGR_EMP_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END REGION_MGR_EMP_ID#
#REGION_MGR_NAME#
if ("''" in FULLDF['REGION_MGR_NAME'].values) == False:
    if max(FULLDF['REGION_MGR_NAME'].str.len()) > 15:
        comment ='The REGION_MGR_NAME column exceded limit 15 - found: ' +  str(max(FULLDF['REGION_MGR_NAME'].str.len()))
        changeType = 'REGION_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['REGION_MGR_NAME'].values) == True:
    comment ='The REGION_MGR_NAME column contains blank values'
    changeType = 'REGION_MGR_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['REGION_MGR_NAME'].str.len()) > 15:
        comment ='The REGION_MGR_NAME column exceded limit 15 - found: ' +  str(max(FULLDF['REGION_MGR_NAME'].str.len()))
        changeType = 'REGION_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END REGION_MGR_NAME#
#REGION_MGR_EMAIL_ID#
if ("''" in FULLDF['REGION_MGR_EMAIL_ID'].values) == False:
    if max(FULLDF['REGION_MGR_EMAIL_ID'].str.len()) > 28:
        comment ='The REGION_MGR_EMAIL_ID column exceded limit 28 - found: ' +  str(max(FULLDF['REGION_MGR_EMAIL_ID'].str.len()))
        changeType = 'REGION_MGR_EMAIL_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['REGION_MGR_EMAIL_ID'].values) == True:
    comment ='The REGION_MGR_EMAIL_ID column contains blank values'
    changeType = 'REGION_MGR_EMAIL_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['REGION_MGR_EMAIL_ID'].str.len()) > 28:
        comment ='The REGION_MGR_EMAIL_ID column exceded limit 28 - found: ' +  str(max(FULLDF['REGION_MGR_EMAIL_ID'].str.len()))
        changeType = 'REGION_MGR_EMAIL_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END REGION_MGR_EMAIL_ID#
#DISTRICT_ID.1#
if ("''" in FULLDF['DISTRICT_ID.1'].values) == False:
    if max(FULLDF['DISTRICT_ID.1'].str.len()) > 3:
        comment ='The DISTRICT_ID.1 column exceded limit 3 - found: ' +  str(max(FULLDF['DISTRICT_ID.1'].str.len()))
        changeType = 'DISTRICT_ID.1'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DISTRICT_ID.1'].values) == True:
    comment ='The DISTRICT_ID.1 column contains blank values'
    changeType = 'DISTRICT_ID.1'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DISTRICT_ID.1'].str.len()) > 3:
        comment ='The DISTRICT_ID.1 column exceded limit 3 - found: ' +  str(max(FULLDF['DISTRICT_ID.1'].str.len()))
        changeType = 'DISTRICT_ID.1'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DISTRICT_ID.1#
#DISTRICT_NAME#
if ("''" in FULLDF['DISTRICT_NAME'].values) == False:
    if max(FULLDF['DISTRICT_NAME'].str.len()) > 21:
        comment ='The DISTRICT_NAME column exceded limit 21 - found: ' +  str(max(FULLDF['DISTRICT_NAME'].str.len()))
        changeType = 'DISTRICT_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DISTRICT_NAME'].values) == True:
    comment ='The DISTRICT_NAME column contains blank values'
    changeType = 'DISTRICT_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DISTRICT_NAME'].str.len()) > 21:
        comment ='The DISTRICT_NAME column exceded limit 21 - found: ' +  str(max(FULLDF['DISTRICT_NAME'].str.len()))
        changeType = 'DISTRICT_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DISTRICT_NAME#
#DISTRICT_MGR_EMP_ID_x#
if ("''" in FULLDF['DISTRICT_MGR_EMP_ID_x'].values) == False:
    if max(FULLDF['DISTRICT_MGR_EMP_ID_x'].str.len()-2) > 8:
        comment ='The DISTRICT_MGR_EMP_ID_x column exceded limit 8 - found: ' +  str(max(FULLDF['DISTRICT_MGR_EMP_ID_x'].str.len()))
        changeType = 'DISTRICT_MGR_EMP_ID_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DISTRICT_MGR_EMP_ID_x'].values) == True:
    comment ='The DISTRICT_MGR_EMP_ID_x column contains blank values'
    changeType = 'DISTRICT_MGR_EMP_ID_x'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DISTRICT_MGR_EMP_ID_x'].str.len()-2) > 8:
        comment ='The DISTRICT_MGR_EMP_ID_x column exceded limit 8 - found: ' +  str(max(FULLDF['DISTRICT_MGR_EMP_ID_x'].str.len()))
        changeType = 'DISTRICT_MGR_EMP_ID_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DISTRICT_MGR_EMP_ID_x#
#DISTRICT_MGR_NAME_x#
if ("''" in FULLDF['DISTRICT_MGR_NAME_x'].values) == False:
    if max(FULLDF['DISTRICT_MGR_NAME_x'].str.len()) > 17:
        comment ='The DISTRICT_MGR_NAME_x column exceded limit 17 - found: ' +  str(max(FULLDF['DISTRICT_MGR_NAME_x'].str.len()))
        changeType = 'DISTRICT_MGR_NAME_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DISTRICT_MGR_NAME_x'].values) == True:
    comment ='The DISTRICT_MGR_NAME_x column contains blank values'
    changeType = 'DISTRICT_MGR_NAME_x'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DISTRICT_MGR_NAME_x'].str.len()) > 17:
        comment ='The DISTRICT_MGR_NAME_x column exceded limit 17 - found: ' +  str(max(FULLDF['DISTRICT_MGR_NAME_x'].str.len()))
        changeType = 'DISTRICT_MGR_NAME_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DISTRICT_MGR_NAME_x#
#DISTRICT_MGR_EMAIL_ID_x#
if ("''" in FULLDF['DISTRICT_MGR_EMAIL_ID_x'].values) == False:
    if max(FULLDF['DISTRICT_MGR_EMAIL_ID_x'].str.len()) > 30:
        comment ='The DISTRICT_MGR_EMAIL_ID_x column exceded limit 30 - found: ' +  str(max(FULLDF['DISTRICT_MGR_EMAIL_ID_x'].str.len()))
        changeType = 'DISTRICT_MGR_EMAIL_ID_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['DISTRICT_MGR_EMAIL_ID_x'].values) == True:
    comment ='The DISTRICT_MGR_EMAIL_ID_x column contains blank values'
    changeType = 'DISTRICT_MGR_EMAIL_ID_x'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['DISTRICT_MGR_EMAIL_ID_x'].str.len()) > 30:
        comment ='The DISTRICT_MGR_EMAIL_ID_x column exceded limit 30 - found: ' +  str(max(FULLDF['DISTRICT_MGR_EMAIL_ID_x'].str.len()))
        changeType = 'DISTRICT_MGR_EMAIL_ID_x'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END DISTRICT_MGR_EMAIL_ID_x#
#SOP_DIV_ID#
if ("''" in FULLDF['SOP_DIV_ID'].values) == False:
    if max(FULLDF['SOP_DIV_ID'].str.len()) > 2:
        comment ='The SOP_DIV_ID column exceded limit 2 - found: ' +  str(max(FULLDF['SOP_DIV_ID'].str.len()))
        changeType = 'SOP_DIV_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_DIV_ID'].values) == True:
    comment ='The SOP_DIV_ID column contains blank values'
    changeType = 'SOP_DIV_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_DIV_ID'].str.len()) > 2:
        comment ='The SOP_DIV_ID column exceded limit 2 - found: ' +  str(max(FULLDF['SOP_DIV_ID'].str.len()))
        changeType = 'SOP_DIV_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_DIV_ID#
#SOP_DIV_NAME#
if ("''" in FULLDF['SOP_DIV_NAME'].values) == False:
    if max(FULLDF['SOP_DIV_NAME'].str.len()) > 6:
        comment ='The SOP_DIV_NAME column exceded limit 6 - found: ' +  str(max(FULLDF['SOP_DIV_NAME'].str.len()))
        changeType = 'SOP_DIV_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_DIV_NAME'].values) == True:
    comment ='The SOP_DIV_NAME column contains blank values'
    changeType = 'SOP_DIV_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_DIV_NAME'].str.len()) > 6:
        comment ='The SOP_DIV_NAME column exceded limit 6 - found: ' +  str(max(FULLDF['SOP_DIV_NAME'].str.len()))
        changeType = 'SOP_DIV_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_DIV_NAME#
#SOP_DIV_MGR_ID#
if ("''" in FULLDF['SOP_DIV_MGR_ID'].values) == False:
    if max(FULLDF['SOP_DIV_MGR_ID'].str.len()-2) > 8:
        comment ='The SOP_DIV_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['SOP_DIV_MGR_ID'].str.len()))
        changeType = 'SOP_DIV_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_DIV_MGR_ID'].values) == True:
    comment ='The SOP_DIV_MGR_ID column contains blank values'
    changeType = 'SOP_DIV_MGR_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_DIV_MGR_ID'].str.len()-2) > 8:
        comment ='The SOP_DIV_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['SOP_DIV_MGR_ID'].str.len()))
        changeType = 'SOP_DIV_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_DIV_MGR_ID#
#SOP_DIV_MGR_NAME#
if ("''" in FULLDF['SOP_DIV_MGR_NAME'].values) == False:
    if max(FULLDF['SOP_DIV_MGR_NAME'].str.len()) > 13:
        comment ='The SOP_DIV_MGR_NAME column exceded limit 13 - found: ' +  str(max(FULLDF['SOP_DIV_MGR_NAME'].str.len()))
        changeType = 'SOP_DIV_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_DIV_MGR_NAME'].values) == True:
    comment ='The SOP_DIV_MGR_NAME column contains blank values'
    changeType = 'SOP_DIV_MGR_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_DIV_MGR_NAME'].str.len()) > 13:
        comment ='The SOP_DIV_MGR_NAME column exceded limit 13 - found: ' +  str(max(FULLDF['SOP_DIV_MGR_NAME'].str.len()))
        changeType = 'SOP_DIV_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_DIV_MGR_NAME#
#SOP_REG_ID#
if ("''" in FULLDF['SOP_REG_ID'].values) == False:
    if max(FULLDF['SOP_REG_ID'].str.len()) > 2:
        comment ='The SOP_REG_ID column exceded limit 2 - found: ' +  str(max(FULLDF['SOP_REG_ID'].str.len()))
        changeType = 'SOP_REG_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_REG_ID'].values) == True:
    comment ='The SOP_REG_ID column contains blank values'
    changeType = 'SOP_REG_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_REG_ID'].str.len()) > 2:
        comment ='The SOP_REG_ID column exceded limit 2 - found: ' +  str(max(FULLDF['SOP_REG_ID'].str.len()))
        changeType = 'SOP_REG_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_REG_ID#
#SOP_REG_NAME#
if ("''" in FULLDF['SOP_REG_NAME'].values) == False:
    if max(FULLDF['SOP_REG_NAME'].str.len()) > 12:
        comment ='The SOP_REG_NAME column exceded limit 12 - found: ' +  str(max(FULLDF['SOP_REG_NAME'].str.len()))
        changeType = 'SOP_REG_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_REG_NAME'].values) == True:
    comment ='The SOP_REG_NAME column contains blank values'
    changeType = 'SOP_REG_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_REG_NAME'].str.len()) > 12:
        comment ='The SOP_REG_NAME column exceded limit 12 - found: ' +  str(max(FULLDF['SOP_REG_NAME'].str.len()))
        changeType = 'SOP_REG_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_REG_NAME#
#SOP_REG_MGR_ID#
if ("''" in FULLDF['SOP_REG_MGR_ID'].values) == False:
    if max(FULLDF['SOP_REG_MGR_ID'].str.len()) > 8:
        comment ='The SOP_REG_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['SOP_REG_MGR_ID'].str.len()))
        changeType = 'SOP_REG_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_REG_MGR_ID'].values) == True:
    comment ='The SOP_REG_MGR_ID column contains blank values'
    changeType = 'SOP_REG_MGR_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_REG_MGR_ID'].str.len()) > 8:
        comment ='The SOP_REG_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['SOP_REG_MGR_ID'].str.len()))
        changeType = 'SOP_REG_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_REG_MGR_ID#
#SOP_REG_MGR_NAME#
if ("''" in FULLDF['SOP_REG_MGR_NAME'].values) == False:
    if max(FULLDF['SOP_REG_MGR_NAME'].str.len()) > 14:
        comment ='The SOP_REG_MGR_NAME column exceded limit 14 - found: ' +  str(max(FULLDF['SOP_REG_MGR_NAME'].str.len()))
        changeType = 'SOP_REG_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_REG_MGR_NAME'].values) == True:
    comment ='The SOP_REG_MGR_NAME column contains blank values'
    changeType = 'SOP_REG_MGR_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_REG_MGR_NAME'].str.len()) > 14:
        comment ='The SOP_REG_MGR_NAME column exceded limit 14 - found: ' +  str(max(FULLDF['SOP_REG_MGR_NAME'].str.len()))
        changeType = 'SOP_REG_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_REG_MGR_NAME#
#SOP_DST_ID#
if ("''" in FULLDF['SOP_DST_ID'].values) == False:
    if max(FULLDF['SOP_DST_ID'].str.len()) > 3:
        comment ='The SOP_DST_ID column exceded limit 3 - found: ' +  str(max(FULLDF['SOP_DST_ID'].str.len()))
        changeType = 'SOP_DST_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_DST_ID'].values) == True:
    comment ='The SOP_DST_ID column contains blank values'
    changeType = 'SOP_DST_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_DST_ID'].str.len()) > 3:
        comment ='The SOP_DST_ID column exceded limit 3 - found: ' +  str(max(FULLDF['SOP_DST_ID'].str.len()))
        changeType = 'SOP_DST_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_DST_ID#
#SOP_DST_NAME#
if ("''" in FULLDF['SOP_DST_NAME'].values) == False:
    if max(FULLDF['SOP_DST_NAME'].str.len()) > 18:
        comment ='The SOP_DST_NAME column exceded limit 18 - found: ' +  str(max(FULLDF['SOP_DST_NAME'].str.len()))
        changeType = 'SOP_DST_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_DST_NAME'].values) == True:
    comment ='The SOP_DST_NAME column contains blank values'
    changeType = 'SOP_DST_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_DST_NAME'].str.len()) > 18:
        comment ='The SOP_DST_NAME column exceded limit 18 - found: ' +  str(max(FULLDF['SOP_DST_NAME'].str.len()))
        changeType = 'SOP_DST_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_DST_NAME#
#SOP_DST_MGR_ID#
if ("''" in FULLDF['SOP_DST_MGR_ID'].values) == False:
    if max(FULLDF['SOP_DST_MGR_ID'].str.len()) > 8:
        comment ='The SOP_DST_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['SOP_DST_MGR_ID'].str.len()))
        changeType = 'SOP_DST_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_DST_MGR_ID'].values) == True:
    comment ='The SOP_DST_MGR_ID column contains blank values'
    changeType = 'SOP_DST_MGR_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_DST_MGR_ID'].str.len()) > 8:
        comment ='The SOP_DST_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['SOP_DST_MGR_ID'].str.len()))
        changeType = 'SOP_DST_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_DST_MGR_ID#
#SOP_DST_MGR_NAME#
if ("''" in FULLDF['SOP_DST_MGR_NAME'].values) == False:
    if max(FULLDF['SOP_DST_MGR_NAME'].str.len()) > 17:
        comment ='The SOP_DST_MGR_NAME column exceded limit 17 - found: ' +  str(max(FULLDF['SOP_DST_MGR_NAME'].str.len()))
        changeType = 'SOP_DST_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['SOP_DST_MGR_NAME'].values) == True:
    comment ='The SOP_DST_MGR_NAME column contains blank values'
    changeType = 'SOP_DST_MGR_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['SOP_DST_MGR_NAME'].str.len()) > 17:
        comment ='The SOP_DST_MGR_NAME column exceded limit 17 - found: ' +  str(max(FULLDF['SOP_DST_MGR_NAME'].str.len()))
        changeType = 'SOP_DST_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END SOP_DST_MGR_NAME#
#COM_DIV_ID#
if ("''" in FULLDF['COM_DIV_ID'].values) == False:
    if max(FULLDF['COM_DIV_ID'].str.len()) > 2:
        comment ='The COM_DIV_ID column exceded limit 2 - found: ' +  str(max(FULLDF['COM_DIV_ID'].str.len()))
        changeType = 'COM_DIV_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_DIV_ID'].values) == True:
    comment ='The COM_DIV_ID column contains blank values'
    changeType = 'COM_DIV_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_DIV_ID'].str.len()) > 2:
        comment ='The COM_DIV_ID column exceded limit 2 - found: ' +  str(max(FULLDF['COM_DIV_ID'].str.len()))
        changeType = 'COM_DIV_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_DIV_ID#
#COM_DIV_NAME#
if ("''" in FULLDF['COM_DIV_NAME'].values) == False:
    if max(FULLDF['COM_DIV_NAME'].str.len()) > 6:
        comment ='The COM_DIV_NAME column exceded limit 6 - found: ' +  str(max(FULLDF['COM_DIV_NAME'].str.len()))
        changeType = 'COM_DIV_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_DIV_NAME'].values) == True:
    comment ='The COM_DIV_NAME column contains blank values'
    changeType = 'COM_DIV_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_DIV_NAME'].str.len()) > 6:
        comment ='The COM_DIV_NAME column exceded limit 6 - found: ' +  str(max(FULLDF['COM_DIV_NAME'].str.len()))
        changeType = 'COM_DIV_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_DIV_NAME#
#COM_DIV_MGR_ID#
if ("''" in FULLDF['COM_DIV_MGR_ID'].values) == False:
    if max(FULLDF['COM_DIV_MGR_ID'].str.len()) > 8:
        comment ='The COM_DIV_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['COM_DIV_MGR_ID'].str.len()))
        changeType = 'COM_DIV_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_DIV_MGR_ID'].values) == True:
    comment ='The COM_DIV_MGR_ID column contains blank values'
    changeType = 'COM_DIV_MGR_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_DIV_MGR_ID'].str.len()) > 8:
        comment ='The COM_DIV_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['COM_DIV_MGR_ID'].str.len()))
        changeType = 'COM_DIV_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_DIV_MGR_ID#
#COM_DIV_MGR_NAME#
if ("''" in FULLDF['COM_DIV_MGR_NAME'].values) == False:
    if max(FULLDF['COM_DIV_MGR_NAME'].str.len()) > 13:
        comment ='The COM_DIV_MGR_NAME column exceded limit 13 - found: ' +  str(max(FULLDF['COM_DIV_MGR_NAME'].str.len()))
        changeType = 'COM_DIV_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_DIV_MGR_NAME'].values) == True:
    comment ='The COM_DIV_MGR_NAME column contains blank values'
    changeType = 'COM_DIV_MGR_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_DIV_MGR_NAME'].str.len()) > 13:
        comment ='The COM_DIV_MGR_NAME column exceded limit 13 - found: ' +  str(max(FULLDF['COM_DIV_MGR_NAME'].str.len()))
        changeType = 'COM_DIV_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_DIV_MGR_NAME#
#COM_REG_ID#
if ("''" in FULLDF['COM_REG_ID'].values) == False:
    if max(FULLDF['COM_REG_ID'].str.len()) > 2:
        comment ='The COM_REG_ID column exceded limit 2 - found: ' +  str(max(FULLDF['COM_REG_ID'].str.len()))
        changeType = 'COM_REG_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_REG_ID'].values) == True:
    comment ='The COM_REG_ID column contains blank values'
    changeType = 'COM_REG_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_REG_ID'].str.len()) > 2:
        comment ='The COM_REG_ID column exceded limit 2 - found: ' +  str(max(FULLDF['COM_REG_ID'].str.len()))
        changeType = 'COM_REG_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_REG_ID#
#COM_REG_NAME#
if ("''" in FULLDF['COM_REG_NAME'].values) == False:
    if max(FULLDF['COM_REG_NAME'].str.len()) > 11:
        comment ='The COM_REG_NAME column exceded limit 11 - found: ' +  str(max(FULLDF['COM_REG_NAME'].str.len()))
        changeType = 'COM_REG_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_REG_NAME'].values) == True:
    comment ='The COM_REG_NAME column contains blank values'
    changeType = 'COM_REG_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_REG_NAME'].str.len()) > 11:
        comment ='The COM_REG_NAME column exceded limit 11 - found: ' +  str(max(FULLDF['COM_REG_NAME'].str.len()))
        changeType = 'COM_REG_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_REG_NAME#
#COM_REG_MGR_ID#
if ("''" in FULLDF['COM_REG_MGR_ID'].values) == False:
    if max(FULLDF['COM_REG_MGR_ID'].str.len()) > 8:
        comment ='The COM_REG_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['COM_REG_MGR_ID'].str.len()))
        changeType = 'COM_REG_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_REG_MGR_ID'].values) == True:
    comment ='The COM_REG_MGR_ID column contains blank values'
    changeType = 'COM_REG_MGR_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_REG_MGR_ID'].str.len()) > 8:
        comment ='The COM_REG_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['COM_REG_MGR_ID'].str.len()))
        changeType = 'COM_REG_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_REG_MGR_ID#
#COM_REG_MGR_NAME#
if ("''" in FULLDF['COM_REG_MGR_NAME'].values) == False:
    if max(FULLDF['COM_REG_MGR_NAME'].str.len()) > 15:
        comment ='The COM_REG_MGR_NAME column exceded limit 15 - found: ' +  str(max(FULLDF['COM_REG_MGR_NAME'].str.len()))
        changeType = 'COM_REG_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_REG_MGR_NAME'].values) == True:
    comment ='The COM_REG_MGR_NAME column contains blank values'
    changeType = 'COM_REG_MGR_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_REG_MGR_NAME'].str.len()) > 15:
        comment ='The COM_REG_MGR_NAME column exceded limit 15 - found: ' +  str(max(FULLDF['COM_REG_MGR_NAME'].str.len()))
        changeType = 'COM_REG_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_REG_MGR_NAME#
#COM_DST_ID#
if ("''" in FULLDF['COM_DST_ID'].values) == False:
    if max(FULLDF['COM_DST_ID'].str.len()) > 3:
        comment ='The COM_DST_ID column exceded limit 3 - found: ' +  str(max(FULLDF['COM_DST_ID'].str.len()))
        changeType = 'COM_DST_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_DST_ID'].values) == True:
    comment ='The COM_DST_ID column contains blank values'
    changeType = 'COM_DST_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_DST_ID'].str.len()) > 3:
        comment ='The COM_DST_ID column exceded limit 3 - found: ' +  str(max(FULLDF['COM_DST_ID'].str.len()))
        changeType = 'COM_DST_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_DST_ID#
#COM_DST_NAME#
if ("''" in FULLDF['COM_DST_NAME'].values) == False:
    if max(FULLDF['COM_DST_NAME'].str.len()) > 21:
        comment ='The COM_DST_NAME column exceded limit 21 - found: ' +  str(max(FULLDF['COM_DST_NAME'].str.len()))
        changeType = 'COM_DST_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_DST_NAME'].values) == True:
    comment ='The COM_DST_NAME column contains blank values'
    changeType = 'COM_DST_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_DST_NAME'].str.len()) > 21:
        comment ='The COM_DST_NAME column exceded limit 21 - found: ' +  str(max(FULLDF['COM_DST_NAME'].str.len()))
        changeType = 'COM_DST_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_DST_NAME#
#COM_DST_MGR_ID#
if ("''" in FULLDF['COM_DST_MGR_ID'].values) == False:
    if max(FULLDF['COM_DST_MGR_ID'].str.len()) > 8:
        comment ='The COM_DST_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['COM_DST_MGR_ID'].str.len()))
        changeType = 'COM_DST_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1

if ("''" in FULLDF['COM_DST_MGR_ID'].values) == True:
    comment ='The COM_DST_MGR_ID column contains blank values'
    changeType = 'COM_DST_MGR_ID'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_DST_MGR_ID'].str.len()) > 8:
        comment ='The COM_DST_MGR_ID column exceded limit 8 - found: ' +  str(max(FULLDF['COM_DST_MGR_ID'].str.len()))
        changeType = 'COM_DST_MGR_ID'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_DST_MGR_ID#
#COM_DST_MGR_NAME#
if ("''" in FULLDF['COM_DST_MGR_NAME'].values) == False:
    if max(FULLDF['COM_DST_MGR_NAME'].str.len()) > 15:
        comment ='The COM_DST_MGR_NAME column exceded limit 15 - found: ' +  str(max(FULLDF['COM_DST_MGR_NAME'].str.len()))
        changeType = 'COM_DST_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1


if ("''" in FULLDF['COM_DST_MGR_NAME'].values) == True:
    comment ='The COM_DST_MGR_NAME column contains blank values'
    changeType = 'COM_DST_MGR_NAME'
    ################
    COMMENTS.at[c, 'Change Type'] = changeType
    COMMENTS.at[c, 'Comment'] = comment
    c += 1
    if max(FULLDF['COM_DST_MGR_NAME'].str.len()) > 15:
        comment ='The COM_DST_MGR_NAME column exceded limit 15 - found: ' +  str(max(FULLDF['COM_DST_MGR_NAME'].str.len()))
        changeType = 'COM_DST_MGR_NAME'
        ################
        COMMENTS.at[c, 'Change Type'] = changeType
        COMMENTS.at[c, 'Comment'] = comment
        c += 1
#END COM_DST_MGR_NAME#

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
FULLDF = FULL
del FULLDF['STORE_COD']
del FULLDF['STORE_NAME_y']
del FULLDF['CITY']
del FULLDF['STORE_OPEN_OR_CLOSE']
del FULLDF['STORE_MGR_EMP_ID_y']
del FULLDF['STORE_MGR_NAME_y']
del FULLDF['DISTRICT_MGR_EMP_ID_y']
del FULLDF['DISTRICT_MGR_NAME_y']
del FULLDF['DISTRICT_MGR_EMAIL_ID_y']
del FULLDF['COMM_SALES_MGR_EMP_ID_y']
del FULLDF['COMM_SALES_MGR_NAME_y']
del FULLDF['CR']
del FULLDF['Loja']
del FULLDF['DM Ignition']
del FULLDF['DM']
del FULLDF['DM E-mail']
del FULLDF['TSM Ignition']
del FULLDF['TSM']
del FULLDF['TSM E-mail']
del FULLDF['COMM_SALES_STORE_FLAG']
CND = COMMENTS.drop_duplicates()
FULLDF.rename(columns={'STORE_NAME_x':'STORE_NAME', 'STORE_MGR_EMP_ID_x': 'STORE_MGR_EMP_ID', 'STORE_MGR_NAME_x': 'STORE_MGR_NAME', 'COMM_SALES_MGR_EMP_ID_x': 'COMM_SALES_MGR_EMP_ID', 'COMM_SALES_MGR_NAME_x': 'COMM_SALES_MGR_NAME'}, inplace=True)
FULLDF.rename(columns={'DISTRICT_MGR_EMP_ID_x': 'DISTRICT_MGR_EMP_ID', 'DISTRICT_MGR_EMAIL_ID_x': 'DISTRICT_MGR_EMAIL_ID', 'DIVISION_ID.1': 'DIVISION_ID', 'REGION_ID.1': 'REGION_ID', 'DISTRICT_ID.1': 'DISTRICT_ID', 'DISTRICT_MGR_NAME_x': 'DISTRICT_MGR_NAME'}, inplace=True)

#END PREPARING FINAL FILES TO SAVE#
# SAVE THE FILES#
final_path = "C:\\Users\\dsegovia\\OneDrive - AutoZone Parts, Inc\\Documents\\Hierarchy tool"
CND.to_csv(final_path + "\\Comments.csv", index=False)
FULLDF.to_csv(final_path + "\\ff_bsto_store_hierarchy_detail.csv", index=False, encoding='ISO-8859-9')
FULLJCF.to_csv(final_path + "\\jobcodefile.csv", index=False)
print("process ended")