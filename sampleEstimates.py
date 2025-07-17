import pandas as pd
import numpy as np

params = ['V1', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 'V11', 'V12',
          'V13', 'V14', 'V16', 'V17', 'V18', 'V20', 'V21', 'V22', 'V23', 'VA', 'VC11', 'VA1', 'VC111']


# Calculate State RSE
def addStateRSE(current_rse, state_rse):
    # Since state RSE table has only one row, take the first row and add it to the end of current RSE table
    last_row = ['Total'] + state_rse.iloc[0].tolist()[1:]
    df = pd.concat(
        [current_rse, pd.DataFrame([last_row], columns=current_rse.columns)],
        ignore_index=True)
    return df


def addTotalRow(df):
    # Iteratively adds a row at the end of a DataFrame containing the column sums
    df = pd.concat(
        [df, pd.DataFrame([['Total'] + [df[col].sum() for col in df.columns[1:]]], columns=df.columns)],
        ignore_index=True)
    return df


def addCenstoSamp(censusData, sampleData, onCol):
    # Adds missing strata from census data into the sample data and merges values for consistent total estimation.
    for code in censusData[onCol]:
        if code not in sampleData[onCol].values:
            sampleData.loc[len(sampleData)] = [code] + [0 for _ in params]

    added_data = pd.merge(sampleData, censusData, on=[onCol])
    for p in params:
        added_data[p] = added_data[p + '_x'] + added_data[p + '_y']
    added_data = added_data[[onCol] + params]
    added_data = added_data.sort_values(by=onCol).reset_index(drop=True)
    return added_data


# TOTAL ESTIMATE TABLES OF GVA, NNW, WTW, etc AT DISTRICT, NIC 2 DIGIT AND NIC 3 DIGIT LEVEL

def total_estimates(dist_full, nic2dig_full, nic3dig_full, dist_census, nic2dig_census, nic3dig_census):
    n = len(dist_full)

    # District
    dist_est_P = dist_full[0][['distcode', 'mavg']]
    for i in range(1, n):
        dist_est_P = pd.merge(dist_est_P, dist_full[i][['distcode', 'mavg']], on=['distcode'])
        dist_est_P.columns = ['distcode'] + params[:(i + 1)]
    dist_est_P = addCenstoSamp(dist_census, dist_est_P, 'distcode').round(0).replace({np.nan: None})
    dist_est_P = addTotalRow(dist_est_P)

    dist_est_C = dist_full[0][['distcode', 'mavg13']]
    for i in range(1, n):
        dist_est_C = pd.merge(dist_est_C, dist_full[i][['distcode', 'mavg13']], on=['distcode'])
        dist_est_C.columns = ['distcode'] + params[:(i + 1)]
    dist_est_C = addCenstoSamp(dist_census, dist_est_C, 'distcode').round(0).replace({np.nan: None})
    dist_est_C = addTotalRow(dist_est_C)

    dist_est_S = dist_full[0][['distcode', 'mavg24']]
    for i in range(1, n):
        dist_est_S = pd.merge(dist_est_S, dist_full[i][['distcode', 'mavg24']], on=['distcode'])
        dist_est_S.columns = ['distcode'] + params[:(i + 1)]
    dist_est_S = addCenstoSamp(dist_census, dist_est_S, 'distcode').round(0).replace({np.nan: None})
    dist_est_S = addTotalRow(dist_est_S)

    # NIC 2 Digit
    nic2dig_est_P = nic2dig_full[0][['nic2dig', 'mavg']]
    for i in range(1, n):
        nic2dig_est_P = pd.merge(nic2dig_est_P, nic2dig_full[i][['nic2dig', 'mavg']], on=['nic2dig'])
        nic2dig_est_P.columns = ['nic2dig'] + params[:(i + 1)]
    nic2dig_est_P = addCenstoSamp(nic2dig_census, nic2dig_est_P, 'nic2dig').round(0).replace({np.nan: None})
    nic2dig_est_P = addTotalRow(nic2dig_est_P)

    nic2dig_est_C = nic2dig_full[0][['nic2dig', 'mavg13']]
    for i in range(1, n):
        nic2dig_est_C = pd.merge(nic2dig_est_C, nic2dig_full[i][['nic2dig', 'mavg13']], on=['nic2dig'])
        nic2dig_est_C.columns = ['nic2dig'] + params[:(i + 1)]
    nic2dig_est_C = addCenstoSamp(nic2dig_census, nic2dig_est_C, 'nic2dig').round(0).replace({np.nan: None})
    nic2dig_est_C = addTotalRow(nic2dig_est_C)

    nic2dig_est_S = nic2dig_full[0][['nic2dig', 'mavg24']]
    for i in range(1, n):
        nic2dig_est_S = pd.merge(nic2dig_est_S, nic2dig_full[i][['nic2dig', 'mavg24']], on=['nic2dig'])
        nic2dig_est_S.columns = ['nic2dig'] + params[:(i + 1)]
    nic2dig_est_S = addCenstoSamp(nic2dig_census, nic2dig_est_S, 'nic2dig').round(0).replace({np.nan: None})
    nic2dig_est_S = addTotalRow(nic2dig_est_S)

    # NIC 3 Digit
    nic3dig_est_P = nic3dig_full[0][['nic3dig', 'mavg']]
    for i in range(1, n):
        nic3dig_est_P = pd.merge(nic3dig_est_P, nic3dig_full[i][['nic3dig', 'mavg']], on=['nic3dig'])
        nic3dig_est_P.columns = ['nic3dig'] + params[:(i + 1)]
    nic3dig_est_P = addCenstoSamp(nic3dig_census, nic3dig_est_P, 'nic3dig').round(0).replace({np.nan: None})
    nic3dig_est_P = addTotalRow(nic3dig_est_P)

    nic3dig_est_C = nic3dig_full[0][['nic3dig', 'mavg13']]
    for i in range(1, n):
        nic3dig_est_C = pd.merge(nic3dig_est_C, nic3dig_full[i][['nic3dig', 'mavg13']], on=['nic3dig'])
        nic3dig_est_C.columns = ['nic3dig'] + params[:(i + 1)]
    nic3dig_est_C = addCenstoSamp(nic3dig_census, nic3dig_est_C, 'nic3dig').round(0).replace({np.nan: None})
    nic3dig_est_C = addTotalRow(nic3dig_est_C)

    nic3dig_est_S = nic3dig_full[0][['nic3dig', 'mavg24']]
    for i in range(1, n):
        nic3dig_est_S = pd.merge(nic3dig_est_S, nic3dig_full[i][['nic3dig', 'mavg24']], on=['nic3dig'])
        nic3dig_est_S.columns = ['nic3dig'] + params[:(i + 1)]
    nic3dig_est_S = addCenstoSamp(nic3dig_census, nic3dig_est_S, 'nic3dig').round(0).replace({np.nan: None})
    nic3dig_est_S = addTotalRow(nic3dig_est_S)

    return dist_est_P, nic2dig_est_P, nic3dig_est_P, dist_est_C, nic2dig_est_C, nic3dig_est_C, dist_est_S, nic2dig_est_S, nic3dig_est_S


# RSE ESTIMATE TABLES OF GVA, NNW, WTW, etc AT DISTRICT, NIC 2 DIGIT AND NIC 3 DIGIT LEVEL

def rse_estimates(state_full, dist_full, nic2dig_full, nic3dig_full):
    n = len(dist_full)

    # State
    state_rse_P = state_full[0][['statecode', 'rse']]
    for i in range(1, n):
        state_rse_P = pd.merge(state_rse_P, state_full[i][['statecode', 'rse']], on=['statecode'])
        state_rse_P.columns = ['statecode'] + params[:(i + 1)]

    state_rse_C = state_full[0][['statecode', 'rse13']]
    for i in range(1, n):
        state_rse_C = pd.merge(state_rse_C, state_full[i][['statecode', 'rse13']], on=['statecode'])
        state_rse_C.columns = ['statecode'] + params[:(i + 1)]

    state_rse_S = state_full[0][['statecode', 'rse24']]
    for i in range(1, n):
        state_rse_S = pd.merge(state_rse_S, state_full[i][['statecode', 'rse24']], on=['statecode'])
        state_rse_S.columns = ['statecode'] + params[:(i + 1)]

    # District
    dist_rse_P = dist_full[0][['distcode', 'rse']]
    for i in range(1, n):
        dist_rse_P = pd.merge(dist_rse_P, dist_full[i][['distcode', 'rse']], on=['distcode'])
        dist_rse_P.columns = ['distcode'] + params[:(i + 1)]
    dist_rse_P = addStateRSE(dist_rse_P, state_rse_P)

    dist_rse_C = dist_full[0][['distcode', 'rse13']]
    for i in range(1, n):
        dist_rse_C = pd.merge(dist_rse_C, dist_full[i][['distcode', 'rse13']], on=['distcode'])
        dist_rse_C.columns = ['distcode'] + params[:(i + 1)]
    dist_rse_C = addStateRSE(dist_rse_C, state_rse_C)

    dist_rse_S = dist_full[0][['distcode', 'rse24']]
    for i in range(1, n):
        dist_rse_S = pd.merge(dist_rse_S, dist_full[i][['distcode', 'rse24']], on=['distcode'])
        dist_rse_S.columns = ['distcode'] + params[:(i + 1)]
    dist_rse_S = addStateRSE(dist_rse_S, state_rse_S)

    # NIC 2 Digit
    nic2dig_rse_P = nic2dig_full[0][['nic2dig', 'rse']]
    for i in range(1, n):
        nic2dig_rse_P = pd.merge(nic2dig_rse_P, nic2dig_full[i][['nic2dig', 'rse']], on=['nic2dig'])
        nic2dig_rse_P.columns = ['nic2dig'] + params[:(i + 1)]
    nic2dig_rse_P = addStateRSE(nic2dig_rse_P, state_rse_P)

    nic2dig_rse_C = nic2dig_full[0][['nic2dig', 'rse13']]
    for i in range(1, n):
        nic2dig_rse_C = pd.merge(nic2dig_rse_C, nic2dig_full[i][['nic2dig', 'rse13']], on=['nic2dig'])
        nic2dig_rse_C.columns = ['nic2dig'] + params[:(i + 1)]
    nic2dig_rse_C = addStateRSE(nic2dig_rse_C, state_rse_C)

    nic2dig_rse_S = nic2dig_full[0][['nic2dig', 'rse24']]
    for i in range(1, n):
        nic2dig_rse_S = pd.merge(nic2dig_rse_S, nic2dig_full[i][['nic2dig', 'rse24']], on=['nic2dig'])
        nic2dig_rse_S.columns = ['nic2dig'] + params[:(i + 1)]
    nic2dig_rse_S = addStateRSE(nic2dig_rse_S, state_rse_S)

    # NIC 3 Digit
    nic3dig_rse_P = nic3dig_full[0][['nic3dig', 'rse']]
    for i in range(1, n):
        nic3dig_rse_P = pd.merge(nic3dig_rse_P, nic3dig_full[i][['nic3dig', 'rse']], on=['nic3dig'])
        nic3dig_rse_P.columns = ['nic3dig'] + params[:(i + 1)]
    nic3dig_rse_P = addStateRSE(nic3dig_rse_P, state_rse_P)

    nic3dig_rse_C = nic3dig_full[0][['nic3dig', 'rse13']]
    for i in range(1, n):
        nic3dig_rse_C = pd.merge(nic3dig_rse_C, nic3dig_full[i][['nic3dig', 'rse13']], on=['nic3dig'])
        nic3dig_rse_C.columns = ['nic3dig'] + params[:(i + 1)]
    nic3dig_rse_C = addStateRSE(nic3dig_rse_C, state_rse_C)

    nic3dig_rse_S = nic3dig_full[0][['nic3dig', 'rse24']]
    for i in range(1, n):
        nic3dig_rse_S = pd.merge(nic3dig_rse_S, nic3dig_full[i][['nic3dig', 'rse24']], on=['nic3dig'])
        nic3dig_rse_S.columns = ['nic3dig'] + params[:(i + 1)]
    nic3dig_rse_S = addStateRSE(nic3dig_rse_S, state_rse_S)

    return dist_rse_P.round(2).replace({np.nan: None}), nic2dig_rse_P.round(2).replace(
        {np.nan: None}), nic3dig_rse_P.round(2).replace(
        {np.nan: None}), dist_rse_C.round(2).replace(
        {np.nan: None}), nic2dig_rse_C.round(2).replace({np.nan: None}), nic3dig_rse_C.round(2).replace(
        {np.nan: None}), dist_rse_S.round(2).replace(
        {np.nan: None}), nic2dig_rse_S.round(2).replace({np.nan: None}), nic3dig_rse_S.round(2).replace({np.nan: None})


#   IMPORTANT FUNCTIONS TO REDUCE CODE

def merge_sum(blockA, df, slno, col_no, Vname, scale=10 ** 5):
    # Merges a block with aggregated sum of a selected column from another block, applies multiplier,
    # and scales the result.
    blk_subset = df[df[df.columns[1]].isin(slno)].copy()
    result = blk_subset.groupby(df.columns[0]).sum().reset_index()
    result = pd.merge(blockA, result, how="left", left_on='a1', right_on=df.columns[0])
    result = result.fillna(0)
    result[Vname] = (result[df.columns[col_no - 1]] * result['smult']) / scale
    result = result.drop(columns=list(set(df.columns) & set(result.columns)))
    return result


def merge_sum_cols(blockA, df, col_names, Vname, scale=10 ** 5):
    # Merges a block with aggregated sum of multiple specified columns from another block, applies multiplier,
    # and scales the result.
    result = pd.merge(blockA, df, how="left", left_on='a1', right_on=df.columns[0])
    sum_col = pd.Series([0] * blockA.shape[0])
    for colm in col_names:
        sum_col += result[colm]
    result[Vname] = (sum_col * result['smult']) / scale
    result = result.drop(columns=list(set(df.columns) & set(result.columns)))
    return result


def getVarTable(Y_ism, parameter):
    m1 = Y_ism[Y_ism['sscode'] == '1'].groupby('stratum').sum()[parameter]
    m2 = Y_ism[Y_ism['sscode'] == '2'].groupby('stratum').sum()[parameter]
    m3 = Y_ism[Y_ism['sscode'] == '3'].groupby('stratum').sum()[parameter]
    m4 = Y_ism[Y_ism['sscode'] == '4'].groupby('stratum').sum()[parameter]
    var_table = pd.concat([m1, m2, m3, m4], axis=1).reset_index()
    var_table.columns = ['stratum', 'm1', 'm2', 'm3', 'm4']
    var_table['mavg13'] = var_table[['m1', 'm3']].mean(axis=1)
    var_table['mavg24'] = var_table[['m2', 'm4']].mean(axis=1)
    var_table['mavg'] = var_table[['m1', 'm2', 'm3', 'm4']].mean(axis=1)

    var_table['samp_var13'] = var_table[['m1', 'm3']].var(axis=1, ddof=1)
    var_table['samp_var24'] = var_table[['m2', 'm4']].var(axis=1, ddof=1)
    var_table['samp_var'] = var_table[['m1', 'm2', 'm3', 'm4']].var(axis=1, ddof=1)

    var_table['nic2dig'] = var_table['stratum'].str[5:7]
    var_table['nic3dig'] = var_table['stratum'].str[5:8]
    var_table['distcode'] = var_table['stratum'].str[2:4]
    var_table['statecode'] = var_table['stratum'].str[:2]
    return var_table


def getRSEestimates(var_table):
    # State
    state_res = var_table.groupby('statecode').sum().reset_index()[
        ['statecode', 'mavg13', 'mavg24', 'mavg', 'samp_var13', 'samp_var24', 'samp_var']]
    state_res['samp_var'] = state_res['samp_var'] / 4
    state_res['rse'] = (state_res['samp_var'] ** 0.5) / state_res['mavg']

    state_res['samp_var13'] = state_res['samp_var13'] / 2
    state_res['rse13'] = (state_res['samp_var13'] ** 0.5) / state_res['mavg13']

    state_res['samp_var24'] = state_res['samp_var24'] / 2
    state_res['rse24'] = (state_res['samp_var24'] ** 0.5) / state_res['mavg24']

    # District
    dist_res = var_table.groupby('distcode').sum().reset_index()[
        ['distcode', 'mavg13', 'mavg24', 'mavg', 'samp_var13', 'samp_var24', 'samp_var']]
    dist_res['samp_var'] = dist_res['samp_var'] / 4
    dist_res['rse'] = (dist_res['samp_var'] ** 0.5) / dist_res['mavg']

    dist_res['samp_var13'] = dist_res['samp_var13'] / 2
    dist_res['rse13'] = (dist_res['samp_var13'] ** 0.5) / dist_res['mavg13']

    dist_res['samp_var24'] = dist_res['samp_var24'] / 2
    dist_res['rse24'] = (dist_res['samp_var24'] ** 0.5) / dist_res['mavg24']

    # NIC 2 DIgit
    nic2dig_res = var_table.groupby('nic2dig').sum().reset_index()[
        ['nic2dig', 'mavg13', 'mavg24', 'mavg', 'samp_var13', 'samp_var24', 'samp_var']]
    nic2dig_res['samp_var'] = nic2dig_res['samp_var'] / 4
    nic2dig_res['rse'] = (nic2dig_res['samp_var'] ** 0.5) / nic2dig_res['mavg']

    nic2dig_res['samp_var13'] = nic2dig_res['samp_var13'] / 2
    nic2dig_res['rse13'] = (nic2dig_res['samp_var13'] ** 0.5) / nic2dig_res['mavg13']

    nic2dig_res['samp_var24'] = nic2dig_res['samp_var24'] / 2
    nic2dig_res['rse24'] = (nic2dig_res['samp_var24'] ** 0.5) / nic2dig_res['mavg24']

    # NIC 3 Digit
    nic3dig_res = var_table.groupby('nic3dig').sum().reset_index()[
        ['nic3dig', 'mavg13', 'mavg24', 'mavg', 'samp_var13', 'samp_var24', 'samp_var']]
    nic3dig_res['samp_var'] = nic3dig_res['samp_var'] / 4
    nic3dig_res['rse'] = (nic3dig_res['samp_var'] ** 0.5) / nic3dig_res['mavg']

    nic3dig_res['samp_var13'] = nic3dig_res['samp_var13'] / 2
    nic3dig_res['rse13'] = (nic3dig_res['samp_var13'] ** 0.5) / nic3dig_res['mavg13']

    nic3dig_res['samp_var24'] = nic3dig_res['samp_var24'] / 2
    nic3dig_res['rse24'] = (nic3dig_res['samp_var24'] ** 0.5) / nic3dig_res['mavg24']

    return state_res, dist_res, nic2dig_res, nic3dig_res
