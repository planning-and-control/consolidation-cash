import pandas as pd
from functions import *
from inputs import *

def main():
    #reading inputs
    path_adaptive_cash = read_path(inputs_all_path, "adaptive_cash")
    path_accounts = read_path(inputs_all_path, "adaptive_accounts")
    path_bp2025_0LIA01 = read_path(inputs_all_path, "bp2025_0LIA01")
    path_extra_mappings = read_path(inputs_all_path, "extra_mappings")
    adaptive_version = read_path(inputs_all_path, "adaptiveVersion")
    #creating dataframes
    print("Creating Dataframes...")

    df_cash=pd.read_csv(path_adaptive_cash, dtype=dtypes_cash, parse_dates=["dataPeriod"])

    df_accounts = pd.read_excel(path_accounts, dtype=dtype_adaptive_accounts, sheet_name="Accounts", skiprows=range(3))

    df_BP2025_0LIA01 = pd.read_csv(path_bp2025_0LIA01, dtype=dtype_BP2025_0LIA01, parse_dates=["dataPeriod"])
    df_BP2025_0LIA01.drop("Unnamed: 0", axis=1, inplace=True)

    df_extra_mappings=pd.read_excel(path_extra_mappings, sheet_name="AdaptiveCashAccount_w_CA_Cash")
    df_extra_mappings.to_csv("../output/extra_mappings.csv", index=False)

    #transforming dataframes
    print("Transforming df_accounts...")
    df_accounts_transformed = transform_accounts(df_accounts)
    selected_cols = [
        'Account Code',
        'Counter Account',
        'DSO',
        'F16_IS',
        'Flow/CounterFlow_Mov',
        'GL Account',
        'LiquidityItem',
        'Sheet ID'
        ]
    df_accounts_transformed = df_accounts_transformed[selected_cols].copy()
    df_accounts_transformed.to_csv("../output/accounts.csv", index=False)
    print("Transforming df_cash...")
    df_cash_transformed = transform_df_cash(df_cash, df_accounts_transformed)
    print("Generating df_cash_1 and df_cash_2...")
    df_cash_1, df_cash_2 = transform_0LIA01(df_BP2025_0LIA01, df_accounts_transformed, df_extra_mappings)
    for i, df in enumerate([df_cash_transformed, df_cash_1, df_cash_2]):
        df.to_csv(f"../output/{i}.csv", index=False)
    df_final = pd.concat([df_cash_transformed, df_cash_1, df_cash_2])
    df_final.reset_index(drop=True, inplace=True)
    df_final.loc[:, "D_SC"] = adaptive_version
    print("Generating CSV...")
    df_final.to_csv("../output/prueba.csv", index=False)


if __name__=="__main__":
    main()