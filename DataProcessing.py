'''
Created on Feb 5, 2017

@author: Xing Fugui
'''
import pandas as pd
import numpy as np
from py2neo import Graph, Node, Relationship
from tabulate import tabulate
import jellyfish
import datetime

companies_df = pd.read_csv("/home/morevna/workspace_neo4j3/companies3.csv")
stock_df = pd.read_csv("/home/morevna/workspace_neo4j3/stocks3.csv")
#print(df.iloc[[0],[0,2]])


#print tabulate(df1)
#print (tabulate(df1, headers='keys', tablefmt='psql'))



#df2 = df.groupby(["Parent Company Name"])
#print (tabulate(df2, headers='keys', tablefmt='psql'))

#df3 = df.duplicated("Parent Company Name")
#print (tabulate(df3, headers='keys', tablefmt='psql'))
print (len(companies_df.index))
#print (len(df3.index))
#pd.merge(df,df3,how = 'left',on='x1')
companies_df = companies_df.iloc[:,0:13]


#company_type_public_df = companies_df[(companies_df["Organization Type"].isnull()) & (companies_df["Organization Type"].find('Public') > 0 and companies_df["Organization Type"].find('Company Type') > 0)
def f(x):
    if x.find('Public') > 0 and x.find('Company Type') > 0:
        return x
    return 'no'

company_type_public_df =  companies_df[pd.notnull(companies_df['Organization Type'])]
company_type_public_df['Organization Type'] = company_type_public_df['Organization Type'].map(lambda x: f(x))
print (len(company_type_public_df[company_type_public_df['Organization Type'] != 'no'].index))
print (tabulate(company_type_public_df.head(10), headers='keys', tablefmt='psql'))
company_type_public_df = company_type_public_df[company_type_public_df['Organization Type'] != 'no']

NONE_VIN = (company_type_public_df["Parent Company Name"].isnull()) | (company_type_public_df["Parent Company Name"].apply(lambda x: str(x).isspace()))
df_null = company_type_public_df[NONE_VIN]
df_not_null = company_type_public_df[~NONE_VIN]
print (len(df_null.index))
print (tabulate(df_null.head(10), headers='keys', tablefmt='psql'))

df_duplicated = df_not_null[df_not_null.duplicated("Parent Company Name",keep=False)]
print (len(df_duplicated.index))
print (tabulate(df_duplicated.head(10), headers='keys', tablefmt='psql'))

#df1 = df3.loc[(df3["Company Name"] == df3["Parent Company Name"]) & (df3["Parent Company Name"] == "Henkel KGaA"),["Company Name","Parent Company Name"]]
EQUAL_COND = df_duplicated["Company Name"] == df_duplicated["Parent Company Name"]
df_duplicated_equals = df_duplicated.loc[EQUAL_COND]
#df1 = df_duplicated.loc[df_duplicated["Parent Company Name"] == "Henkel KGaA",["Company Name","Parent Company Name"]]
print (len(df_duplicated_equals.index))
print (tabulate(df_duplicated_equals.head(10), headers='keys', tablefmt='psql'))
#numbers that the name of the Parent Company is redundant but the name of the Parent Company is not the same as the name of the company
df_duplicated_equals_no = df_duplicated[~EQUAL_COND]
print (len(df_duplicated_equals_no.index))
print (tabulate(df_duplicated_equals_no.head(10), headers='keys', tablefmt='psql'))


df_exclude_duplicated = df_not_null[~df_not_null["Parent Company Name"].isin(df_duplicated["Parent Company Name"].values)]
print (len(df_exclude_duplicated.index))
print (tabulate(df_exclude_duplicated.head(1), headers='keys', tablefmt='psql'))

#df_exclude_duplicated_equals = df_exclude_duplicated.loc[df_exclude_duplicated["Company Name"] == df_exclude_duplicated["Parent Company Name"],["Company Name","Parent Company Name"]]
#print (len(df_exclude_duplicated_equals.index))
print (len(stock_df.index))
stock_df = stock_df.iloc[:,0:11]
STOCK_NONE_VIN = (stock_df["Name"].isnull()) | (stock_df["Name"].apply(lambda x: str(x).isspace()))
stock_df_null = stock_df[STOCK_NONE_VIN]
stock_df_not_null = stock_df[~STOCK_NONE_VIN]
print (tabulate(stock_df_null, headers='keys', tablefmt='psql'))

stock_df_duplicated = stock_df_not_null[stock_df_not_null.duplicated("Name",keep=False)]
print (len(stock_df_duplicated.index))
print (tabulate(stock_df_duplicated.head(10), headers='keys', tablefmt='psql'))

established_relationships_nums = 0

list_strings = stock_df_not_null.loc[:,'Name']
g = Graph(password="admin")
highest_jw = 0.88

start = datetime.datetime.now()

isAppendFlag = False
established_relationships_company_df_1 = pd.DataFrame(data=None, columns=df_not_null.columns)
established_relationships_stock_df_1 = pd.DataFrame(data=None, columns=stock_df_not_null.columns)
#the number of companies with 'Company Type: Public' and Parent Company is duplicated
for index,row in df_duplicated_equals.iterrows():
    isAppendFlag = False
    parent_company_name = row["Parent Company Name"]
    OrganizationType = str(row['Organization Type'])
    parent_company_name_split = parent_company_name.split(' ')
    parent_company_first_key = parent_company_name_split[0] 
    
    CompanyName = row['Company Name']
    ParentCompanyName = parent_company_name
    FieldofActivity = row['Field of Activity']
    Country = row['Country']
    KeyIndications = row['Key Indications']
    KeyTargetbasedActions = row['Key Target-based Actions']
    KeyTechnologies = row['Key Technologies']
    NumberofDrugsActiveDevelopment = row['Number of Drugs in Active Development']
    NumberofInactiveDrugs = row['Number of Inactive Drugs']
    NumberofDeals = row['Number of Deals']
    NumberofPatentsasOwner = row['Number of Patents as Owner']
    NumberofPatentsasThirdParty = row['Number of Patents as Third Party']    
    a = Node(CompanyName, Name=CompanyName,ParentCompanyName=ParentCompanyName,OrganizationType=OrganizationType,FieldofActivity=FieldofActivity,Country=Country,KeyIndications=KeyIndications,KeyTargetbasedActions=KeyTargetbasedActions,KeyTechnologies=KeyTechnologies,NumberofDrugsActiveDevelopment=NumberofDrugsActiveDevelopment,NumberofInactiveDrugs=NumberofInactiveDrugs,NumberofDeals=NumberofDeals,NumberofPatentsasOwner=NumberofPatentsasOwner,NumberofPatentsasThirdParty=NumberofPatentsasThirdParty)
    if (OrganizationType.find('Public') > 0 and OrganizationType.find('Company Type') > 0):
        for current_string in list_strings:
            current_score = jellyfish.jaro_winkler(parent_company_name, current_string)
            current_string_split = current_string.split(' ')
#             if len(current_string_split) == len(parent_company_name_split):
#                 highest_jw = 1.0
#             elif  (len(current_string_split) < len(parent_company_name_split)) and  len(current_string_split) >= 2 :
#                 highest_jw = 0.9
            if(current_score >= highest_jw) and (current_string.startswith(parent_company_first_key)):
                #print (current_score)
                #print (current_string+"=="+parent_company_name)
                isAppendFlag = True

                current_stock_df = stock_df_not_null.loc[stock_df_not_null["Name"] == current_string]
                for index2,row2 in current_stock_df.iterrows():
                    StockName = row2['Name']
                    CsiNumber = row2['CsiNumber']
                    Symbol = row2['Symbol']
                    Exchange = row2['Exchange']
                    IsActive = row2['IsActive']
                    StartDate = row2['StartDate']
                    EndDate = row2['EndDate']
                    ConversionFactor = row2['ConversionFactor']
                    SwitchCfDate = row2['SwitchCfDate']
                    SubExchange = row2['SubExchange']
                    #print ("+++++++++++++++++++++++"+StockName+"+++"+parent_company_name)
                    established_relationships_stock_df_1 = established_relationships_stock_df_1.append(row2,ignore_index=True)
                    
                    tx = g.begin()
                    
                    tx.create(a)
                    b = Node(StockName, Name=StockName,ParentCompanyName=ParentCompanyName,Exchange=Exchange,IsActive=IsActive,StartDate=StartDate,EndDate=EndDate,ConversionFactor=ConversionFactor,SwitchCfDate=SwitchCfDate,SubExchange=SubExchange)
                    ab = Relationship(a, "Traded as", b)
                    tx.create(ab)
                    tx.commit()
                    established_relationships_nums = established_relationships_nums + 1                    
                    
    if isAppendFlag :
        #temp_df = pd.DataFrame([row])
        established_relationships_company_df_1 = established_relationships_company_df_1.append(row,ignore_index=True)
                    

    
print (len(established_relationships_company_df_1.index))
#print (tabulate(established_relationships_company_df_1, headers='keys', tablefmt='psql'))

print (len(established_relationships_stock_df_1.index))
#print (tabulate(established_relationships_stock_df_1, headers='keys', tablefmt='psql'))




isAppendFlag = False
established_relationships_company_df_2 = pd.DataFrame(data=None, columns=df_not_null.columns)
established_relationships_stock_df_2 = pd.DataFrame(data=None, columns=stock_df_not_null.columns)
#the number of companies with 'Company Type: Public' and Parent Company is not duplicated
for index,row in df_exclude_duplicated.iterrows():
    isAppendFlag = False
    parent_company_name = row["Parent Company Name"]
    OrganizationType = str(row['Organization Type'])
    parent_company_name_split = parent_company_name.split(' ')
    parent_company_first_key = parent_company_name_split[0] 
    
    CompanyName = row['Company Name']
    ParentCompanyName = parent_company_name
    FieldofActivity = row['Field of Activity']
    Country = row['Country']
    KeyIndications = row['Key Indications']
    KeyTargetbasedActions = row['Key Target-based Actions']
    KeyTechnologies = row['Key Technologies']
    NumberofDrugsActiveDevelopment = row['Number of Drugs in Active Development']
    NumberofInactiveDrugs = row['Number of Inactive Drugs']
    NumberofDeals = row['Number of Deals']
    NumberofPatentsasOwner = row['Number of Patents as Owner']
    NumberofPatentsasThirdParty = row['Number of Patents as Third Party']    
    a = Node(CompanyName, Name=CompanyName,ParentCompanyName=ParentCompanyName,OrganizationType=OrganizationType,FieldofActivity=FieldofActivity,Country=Country,KeyIndications=KeyIndications,KeyTargetbasedActions=KeyTargetbasedActions,KeyTechnologies=KeyTechnologies,NumberofDrugsActiveDevelopment=NumberofDrugsActiveDevelopment,NumberofInactiveDrugs=NumberofInactiveDrugs,NumberofDeals=NumberofDeals,NumberofPatentsasOwner=NumberofPatentsasOwner,NumberofPatentsasThirdParty=NumberofPatentsasThirdParty)
    if (OrganizationType.find('Public') > 0 and OrganizationType.find('Company Type') > 0):
        for current_string in list_strings:
            current_score = jellyfish.jaro_winkler(parent_company_name, current_string)
            current_string_split = current_string.split(' ')
#             if len(current_string_split) == len(parent_company_name_split):
#                 highest_jw = 1.0
#             elif  (len(current_string_split) < len(parent_company_name_split)) and  len(current_string_split) >= 2 :
#                 highest_jw = 0.9
            if(current_score >= highest_jw) and (current_string.startswith(parent_company_first_key)):
                #print (current_score)
                #print (current_string+"=="+parent_company_name)
                isAppendFlag = True

                current_stock_df = stock_df_not_null.loc[stock_df_not_null["Name"] == current_string]
                for index2,row2 in current_stock_df.iterrows():
                    StockName = row2['Name']
                    CsiNumber = row2['CsiNumber']
                    Symbol = row2['Symbol']
                    Exchange = row2['Exchange']
                    IsActive = row2['IsActive']
                    StartDate = row2['StartDate']
                    EndDate = row2['EndDate']
                    ConversionFactor = row2['ConversionFactor']
                    SwitchCfDate = row2['SwitchCfDate']
                    SubExchange = row2['SubExchange']
                    #print ("+++++++++++++++++++++++"+StockName+"+++"+parent_company_name)
                    established_relationships_stock_df_2 = established_relationships_stock_df_2.append(row2,ignore_index=True)
                    
                    tx = g.begin()
                    
                    tx.create(a)
                    b = Node(StockName, Name=StockName,ParentCompanyName=ParentCompanyName,Exchange=Exchange,IsActive=IsActive,StartDate=StartDate,EndDate=EndDate,ConversionFactor=ConversionFactor,SwitchCfDate=SwitchCfDate,SubExchange=SubExchange)
                    ab = Relationship(a, "Traded as", b)
                    tx.create(ab)
                    tx.commit()
                    established_relationships_nums = established_relationships_nums + 1                  
                    
    if isAppendFlag :
        #temp_df = pd.DataFrame([row])
        established_relationships_company_df_2 = established_relationships_company_df_2.append(row,ignore_index=True)
                    

    
print (len(established_relationships_company_df_2.index))
#print (tabulate(established_relationships_company_df_1, headers='keys', tablefmt='psql'))

print (len(established_relationships_stock_df_2.index))
#print (tabulate(established_relationships_stock_df_1, headers='keys', tablefmt='psql'))


isAppendFlag = False
established_relationships_company_df_3 = pd.DataFrame(data=None, columns=df_not_null.columns)
established_relationships_stock_df_3 = pd.DataFrame(data=None, columns=stock_df_not_null.columns)
#the number of companies with 'Company Type: Public'and Parent Company is empty
for index,row in df_null.iterrows():
    isAppendFlag = False
    parent_company_name = row["Company Name"]
    OrganizationType = str(row['Organization Type'])
    parent_company_name_split = parent_company_name.split(' ')
    parent_company_first_key = parent_company_name_split[0] 
    
    CompanyName = row['Company Name']
    ParentCompanyName = parent_company_name
    FieldofActivity = row['Field of Activity']
    Country = row['Country']
    KeyIndications = row['Key Indications']
    KeyTargetbasedActions = row['Key Target-based Actions']
    KeyTechnologies = row['Key Technologies']
    NumberofDrugsActiveDevelopment = row['Number of Drugs in Active Development']
    NumberofInactiveDrugs = row['Number of Inactive Drugs']
    NumberofDeals = row['Number of Deals']
    NumberofPatentsasOwner = row['Number of Patents as Owner']
    NumberofPatentsasThirdParty = row['Number of Patents as Third Party']    
    a = Node(CompanyName, Name=CompanyName,ParentCompanyName=ParentCompanyName,OrganizationType=OrganizationType,FieldofActivity=FieldofActivity,Country=Country,KeyIndications=KeyIndications,KeyTargetbasedActions=KeyTargetbasedActions,KeyTechnologies=KeyTechnologies,NumberofDrugsActiveDevelopment=NumberofDrugsActiveDevelopment,NumberofInactiveDrugs=NumberofInactiveDrugs,NumberofDeals=NumberofDeals,NumberofPatentsasOwner=NumberofPatentsasOwner,NumberofPatentsasThirdParty=NumberofPatentsasThirdParty)
    if (OrganizationType.find('Public') > 0 and OrganizationType.find('Company Type') > 0):
        for current_string in list_strings:
            current_score = jellyfish.jaro_winkler(parent_company_name, current_string)
            current_string_split = current_string.split(' ')
#             if len(current_string_split) == len(parent_company_name_split):
#                 highest_jw = 1.0
#             elif  (len(current_string_split) < len(parent_company_name_split)) and  len(current_string_split) >= 2 :
#                 highest_jw = 0.9
            if(current_score >= highest_jw) and (current_string.startswith(parent_company_first_key)):
                #print (current_score)
                #print (current_string+"=="+parent_company_name)
                isAppendFlag = True

                current_stock_df = stock_df_not_null.loc[stock_df_not_null["Name"] == current_string]
                for index2,row2 in current_stock_df.iterrows():
                    StockName = row2['Name']
                    CsiNumber = row2['CsiNumber']
                    Symbol = row2['Symbol']
                    Exchange = row2['Exchange']
                    IsActive = row2['IsActive']
                    StartDate = row2['StartDate']
                    EndDate = row2['EndDate']
                    ConversionFactor = row2['ConversionFactor']
                    SwitchCfDate = row2['SwitchCfDate']
                    SubExchange = row2['SubExchange']
                    #print ("+++++++++++++++++++++++"+StockName+"+++"+parent_company_name)
                    established_relationships_stock_df_3 = established_relationships_stock_df_3.append(row2,ignore_index=True)
                    
                    tx = g.begin()
                    
                    tx.create(a)
                    b = Node(StockName, Name=StockName,ParentCompanyName=ParentCompanyName,Exchange=Exchange,IsActive=IsActive,StartDate=StartDate,EndDate=EndDate,ConversionFactor=ConversionFactor,SwitchCfDate=SwitchCfDate,SubExchange=SubExchange)
                    ab = Relationship(a, "Traded as", b)
                    tx.create(ab)
                    tx.commit()
                    established_relationships_nums = established_relationships_nums + 1   
                                     
                    
    if isAppendFlag :
        #temp_df = pd.DataFrame([row])
        established_relationships_company_df_3 = established_relationships_company_df_3.append(row,ignore_index=True)
                    

    
print (len(established_relationships_company_df_3.index))
#print (tabulate(established_relationships_company_df_1, headers='keys', tablefmt='psql'))

print (len(established_relationships_stock_df_3.index))
#print (tabulate(established_relationships_stock_df_1, headers='keys', tablefmt='psql'))


established_relationships_company_df = established_relationships_company_df_1.append(established_relationships_company_df_2).append(established_relationships_company_df_3)

end = datetime.datetime.now()

# the total number of relationships that were created by the fuzzywuzzy algorithm
established_relationships_company_df_nums = len(established_relationships_company_df.index)
#the total number of companies with 'Company Type: Public' and  the name of the Parent Company is redundant but the name of the Parent Company is the same as the name of the company
established_should_relationships_company_df_nums = len(company_type_public_df[company_type_public_df['Organization Type'] != 'no'].index) - len(df_duplicated_equals_no.index)
established_no_relationships_company_df_nums = established_should_relationships_company_df_nums - established_relationships_company_df_nums


print ('------------------------------------------')
print (established_relationships_company_df_nums)
print (established_should_relationships_company_df_nums)
print (established_no_relationships_company_df_nums)
print (established_relationships_company_df_nums/established_should_relationships_company_df_nums)
print (established_no_relationships_company_df_nums/established_relationships_company_df_nums)


                    
print (end-start)                    