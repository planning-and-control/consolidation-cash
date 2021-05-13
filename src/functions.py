import pandas as pd
import xlrd
import datetime
from inputs import inputs_all_path
import numpy as np

def read_path(input_all_paths, denomination):
    df = pd.read_excel(input_all_paths, sheet_name="inputs")
    return df[df.denomination==denomination].path.iloc[0]

def excel_to_datetime(excel_date):
    a = xlrd.xldate.xldate_as_tuple(excel_date, 0)
    return datetime.datetime(*a).strftime("%d/%m/%Y")

def cash_column(df, namecol):
    #conditions
    conditional_values = ["1299999999E", "1290000999E"]

    filter_1 =  (df["GL Account"].isin(conditional_values)) & (df["GL Account"].notnull())
    filter_2 = (df["Counter Account"].str.contains("|".join(conditional_values))) & (df["Counter Account"].notnull())
    filter_3 = (df["F16_IS"] == "F16_IS") & (df["F16_IS"].notnull())
    filter_4 = (df["AccountCode"].str.contains("Cash_")) & (df["AccountCode"].notnull())

    condition_countercash = np.where(filter_3, "CounterCash - F16_IS", "CounterCash")
    condition_cash_noncash = np.where(filter_4, "Cash", "Non-Cash")
    condition_np = np.where(filter_2, condition_countercash, condition_cash_noncash)

    df[namecol] = np.where(filter_1, "CashBalance", condition_np)

    return df

def transform_df_cash(df_cash, df_accounts_transformed):
    #transform dates in numeric excel format to string in format d/m/Y
    # df_cash.loc[:,"dataPeriod"] = df_cash.dataPeriod.map(lambda x: excel_to_datetime(x))
    adaptive_version = read_path(inputs_all_path, "adaptiveVersion")
    df_cash["D_SC"] = adaptive_version
    df_cash.rename(columns={"Amount": "LC_Amount"}, inplace=True)

    index_drop = df_cash[df_cash.LevelName=="Blue Canyon I Company"].index
    df_cash.drop(index_drop, inplace=True)
    df_cash.reset_index(drop=True, inplace=True)

    #merging with df_accounts_transformed
    
    assert df_accounts_transformed["Account Code"].duplicated().sum() == 0, "Warning, duplicates found in Account Code"
    df_cash = pd.merge(df_cash, df_accounts_transformed, left_on="AccountCode", right_on="Account Code", how="left")

    df_cash.drop(["Account Code"], axis=1, inplace=True)

    df_cash = cash_column(df_cash, "Cash")

    col_drop = [
        "Level Type",
        "Country_Load",
        "CompanyCode",
        "platformAccount",
        "BSSourceAccount",
        "Currency",
        "Rolls up to",
        "Park",
        "IsCP",
        "IsLinkCalc",
        "OM_Service",
        "GL Account"
        ]
    
    df_cash.drop(col_drop, axis=1, inplace=True)
    df_cash["D_AU"] = "0LIA01"
    df_cash["Period_Level"] = df_cash["LevelName"] + "_" + df_cash["dataPeriod"].dt.strftime("%Y-%m")
    df_cash["Period_Partner"] = df_cash["Partner"] + "_" + df_cash["dataPeriod"].dt.strftime("%Y-%m")
    
    return df_cash

def transform_accounts(df):
    #drop columns
    col_drop = [
        "Unnamed: 0",
        "Account Type Name",
        "Sheet Name"
    ]

    df.drop(col_drop, inplace=True, axis=1)

    #drop nulls for sheet_id
    index_drop = df[df["Sheet ID"].isnull()].index

    if list(index_drop):
        df.drop(index_drop, inplace=True)
        df.reset_index(drop=True, inplace=True)
    else:
        print("No nulls found in Sheet ID column")

    #new column combining Counter Account columns
    df.fillna({"Counter Account " + ext: "" for ext in ["EU", "NA", "OF", "CH"]}, inplace=True)
    df["Counter Account"] = df["Counter Account EU"] + df["Counter Account NA"] + df["Counter Account OF"] + df["Counter Account CH"]

    #column F16_IS
    df["F16_IS"] = np.where((df["Account Code"].notnull()) &
                            (df["Account Code"].str.contains("F16_")) &
                            (df["Account Code"].str.contains("_IS")),
                            "F16_IS",
                            "No F16_IS")

    #change 1264 by 1285 in Sheet ID column
    df.loc[df["Sheet ID"] == 1264, "Sheet ID"] = 1285
    df.loc[df["Sheet ID"] == 1182, "Sheet ID"] = 841
    df.loc[df["Sheet ID"] == 1284, "Sheet ID"] = 1301

    #drop duplicates
    df.drop_duplicates(subset="Account Code", inplace=True, ignore_index=True)

    return df



def transform_0LIA01(df_BP2025_0LIA01, df_accounts_transformed, df_extra_mappings):
    df_BP2025_0LIA01 = df_BP2025_0LIA01.merge(df_accounts_transformed, left_on="AccountCode", right_on="Account Code", how="left")
    df_BP2025_0LIA01.drop("Account Code", axis=1, inplace=True)
    df_BP2025_0LIA01 = cash_column(df_BP2025_0LIA01, "CashAuto")
    
    df_BP2025_0LIA01 = df_BP2025_0LIA01.merge(df_extra_mappings, on="AccountCode", how="left")
    df_BP2025_0LIA01.rename(columns={"CashElement": "CashEl"}, inplace=True)
    df_BP2025_0LIA01["Cash"] = np.where((df_BP2025_0LIA01["CashEl"].notnull()) & (df_BP2025_0LIA01["CashEl"] != ""), df_BP2025_0LIA01["CashEl"], df_BP2025_0LIA01["CashAuto"])
    df_BP2025_0LIA01.drop("CashEl", axis=1, inplace=True)
    df_BP2025_0LIA01.to_csv("../output/0lia01.csv", index=False)
    print("Checking 1")
    df_cash_1 = df_BP2025_0LIA01[df_BP2025_0LIA01.Cash != "Non-Cash"].copy()
    df_cash_2 = df_BP2025_0LIA01[df_BP2025_0LIA01.Cash == "CF_Adj_Mirror"].copy()
    df_cash_2.rename(columns={"AccountCode": "AccountCode_old", "LC_Amount": "LC_Amount_old"}, inplace=True)
    print("Checking 2")
    df_cash_2["AccountCode"] = df_cash_2["AccountCode_old"] + "_M" 
    df_cash_2["LC_Amount"] = df_cash_2["LC_Amount_old"].multiply(-1)
    print("Checking 3")
    df_cash_2.drop(["AccountCode_old", "LC_Amount_old"], axis=1, inplace=True)
    return df_cash_1, df_cash_2



