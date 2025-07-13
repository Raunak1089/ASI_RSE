from flask import jsonify
import pandas as pd
from sampleEstimates import total_estimates, rse_estimates, getVarTable, getRSEestimates

params = ['V1', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 'V11', 'V12',
          'V13', 'V14', 'V16', 'V17', 'V18', 'V20', 'V21', 'V22', 'V23', 'VA', 'VC11', 'VA1', 'VC111']


def processData(blkA, blkB, blkC, blkD, blkE, blkF, blkG, blkH, blkI, blkJ, sscode, multiplier):
    # Drop unnecessary columns
    blkB = blkB.drop(columns=['yr', 'blk'])
    blkC = blkC.drop(columns=['yr', 'blk'])
    blkD = blkD.drop(columns=['yr', 'blk'])
    blkE = blkE.drop(columns=['yr', 'blk'])
    blkF = blkF.drop(columns=['yr', 'blk'])
    blkG = blkG.drop(columns=['yr', 'blk'])
    blkH = blkH.drop(columns=['yr', 'blk'])
    blkI = blkI.drop(columns=['yr', 'blk'])
    blkJ = blkJ.drop(columns=['yr', 'blk'])

    # Type convert
    blkA['a11'] = blkA['a11'].astype('int64')
    blkA['a12'] = blkA['a12'].astype('int64')
    blkC['c_i1'] = blkC['c_i1'].astype('int64')
    blkC['c_i5'] = blkC['c_i5'].astype('int64')
    blkC['c_i7'] = blkC['c_i7'].astype('int64')
    blkC['c_i9'] = blkC['c_i9'].astype('int64')
    blkC['c_i13'] = blkC['c_i13'].astype('int64')
    blkD['d_i1'] = blkD['d_i1'].astype('int64')
    blkD['d_i4'] = blkD['d_i4'].astype('int64')
    blkE['e_i1'] = blkE['e_i1'].astype('int64')
    blkE['e_i6'] = blkE['e_i6'].astype('int64')
    blkE['e_i8'] = blkE['e_i8'].astype('int64')
    blkJ['j_i1'] = blkJ['j_i1'].astype('int64')
    blkJ['j_i13'] = blkJ['j_i13'].astype('int64')

    gcol = [1, 2, 3, 4, 6, 7, 8, 9, 10, 11]
    for x in gcol:
        blkG['g' + str(x)] = blkG['g' + str(x)].astype('int64')

    blkH['h_i1'] = blkH['h_i1'].astype('int64')
    blkH['h_i6'] = blkH['h_i6'].astype('int64')
    blkI['i_i1'] = blkI['i_i1'].astype('int64')
    blkI['i_i6'] = blkI['i_i6'].astype('int64')

    fcol = [1, "2a", "2b", 3, 4, 6, 7, 8, 9, 10, 11]
    for x in fcol:
        blkF['f' + str(x)] = blkF['f' + str(x)].astype('int64')

    for i in range(1, 5):
        multiplier['mult' + str(i)] = multiplier['mult' + str(i)].astype('float64')

    # Keep Unit Status <= 4 only
    resultC = pd.merge(blkC, blkA[['a1', 'a3', 'a12']], how='left', left_on='ac01', right_on='a1')
    blkC_all = resultC[resultC['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultD = pd.merge(blkD, blkA[['a1', 'a3', 'a12']], how='left', left_on='ad01', right_on='a1')
    blkD_all = resultD[resultD['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultE = pd.merge(blkE, blkA[['a1', 'a3', 'a12']], how='left', left_on='ae01', right_on='a1')
    blkE_all = resultE[resultE['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultF = pd.merge(blkF, blkA[['a1', 'a3', 'a12']], how='left', left_on='af01', right_on='a1')
    blkF_all = resultF[resultF['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultG = pd.merge(blkG, blkA[['a1', 'a3', 'a12']], how='left', left_on='ag01', right_on='a1')
    blkG_all = resultG[resultG['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultH = pd.merge(blkH, blkA[['a1', 'a3', 'a12']], how='left', left_on='ah01', right_on='a1')
    blkH_all = resultH[resultH['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultI = pd.merge(blkI, blkA[['a1', 'a3', 'a12']], how='left', left_on='ai01', right_on='a1')
    blkI_all = resultI[resultI['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultJ = pd.merge(blkJ, blkA[['a1', 'a3', 'a12']], how='left', left_on='aj01', right_on='a1')
    blkJ_all = resultJ[resultJ['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    del resultC, resultD, resultE, resultF, resultG, resultH, resultI, resultJ

    blkA_all = blkA[blkA['a12'] <= 4].copy()

    # Fix district code by ensuring 2-digit format

    blkA_all['a8'] = blkA_all['a8'].str.zfill(2)

    # Incorporate "Other" for NIC 2,3 digit by changing column a5
    allowed_nic2dig = [1, 8] + list(range(10, 34)) + [38, 58]

    def fix_nic_stratum(nic):
        try:
            nic2 = int(str(nic)[5:7])
            if nic2 not in allowed_nic2dig:
                return nic[:5] + 'Oth'
            return nic
        except:
            return nic  # leave it unchanged if error

    def fix_nic_a5(nic):
        try:
            nic2 = int(str(nic)[:2])
            if nic2 not in allowed_nic2dig:
                return 'Oth' + nic[3:]
            return nic
        except:
            return nic  # leave it unchanged if error


    # Merge Block A sample data with sscode to get sector ('bfe') and sscode (subsample code)
    resultA = pd.merge(blkA_all, sscode, how="left", left_on='a1', right_on='dsl')

    # Create 'stratum' identifier by concatenating: sector (a7), district (a8), frame sector (bfe), and first 3
    # digits of NIC code (a5)
    resultA['stratum'] = resultA['a7'] + resultA['a8'] + resultA['bfe'] + resultA['a5'].str[:3]

    # Drop intermediate columns no longer needed after stratum creation
    resultA = resultA.drop(columns=['dsl', 'bfe', 'mult'])

    # Construct the same 'stratum' key in the multiplier data for joining
    multiplier['stratum'] = (
            multiplier['state'] +
            multiplier['distcode'] +
            multiplier['sector'] +
            multiplier['nic03']
    )

    # Merge sample data with relevant multiplier columns based on 'stratum'
    resultmult = pd.merge(
        resultA,
        multiplier[['sstrm', 'mult1', 'mult2', 'mult3', 'mult4', 'stratum', 'cmult']],
        how="left",
        left_on=['a3', 'stratum'],
        right_on=['sstrm', 'stratum']
    )

    resultmult['smult'] = [
        resultmult.loc[i, 'mult' + resultmult.loc[i, 'sscode']]
        if resultmult.loc[i, 'sscode'] in ['1', '2', '3', '4']
        else 1.0
        for i in resultmult.index
    ]

    # Drop the individual multiplier columns now that 'mult' has been finalized
    resultmult = resultmult.drop(columns=['mult1', 'mult2', 'mult3', 'mult4'])

    # resultmult
    # resultmult.to_csv('resultmult',index=False)

    # 1. No. of factories
    resultmult['V1'] = resultmult['a11'] * resultmult['smult']

    # 3. Fixed capital
    # C vals
    blkC_subset = blkC_all[(blkC_all['c_i1'] <= 9) & (blkC_all['c_i1'] != 8)].copy()[
        ['ac01', 'c_i1', 'c_i5', 'c_i9', 'c_i13']]
    resultC = blkC_subset.groupby('ac01').sum().reset_index()
    result = pd.merge(resultmult, resultC, how="left", left_on='a1', right_on='ac01')
    result = result.fillna(0)

    result['V3'] = (result['c_i13'] * result['smult']) / (10 ** 5)
    result = result.drop(columns=['ac01'])

    # 4. Physical Working Capital
    # D vals
    blkD_subset = blkD_all[(blkD_all['d_i1'] <= 6) & (blkD_all['d_i1'] != 4)].copy()[['ad01', 'd_i4']]
    resultD = blkD_subset.groupby('ad01').sum().reset_index()
    result = pd.merge(result, resultD, how="left", left_on='a1', right_on='ad01')
    result = result.fillna(0)

    result['V4'] = (result['d_i4'] * result['smult']) / (10 ** 5)
    result = result.drop(columns=['ad01', 'd_i4'])

    # 5. Working Capital
    # D vals
    blkD_subset = blkD_all[(blkD_all['d_i1'] >= 8) & (blkD_all['d_i1'] <= 10)].copy()[['ad01', 'd_i4']]
    resultD = blkD_subset.groupby('ad01').sum().reset_index()
    result = pd.merge(result, resultD, how="left", left_on='a1', right_on='ad01')
    result = result.fillna(0)

    result['D8910'] = result['d_i4'] * result['smult']
    result = result.drop(columns=['ad01', 'd_i4'])

    blkD_subset = blkD_all[(blkD_all['d_i1'] >= 12) & (blkD_all['d_i1'] <= 14)].copy()[['ad01', 'd_i4']]
    resultD = blkD_subset.groupby('ad01').sum().reset_index()
    result = pd.merge(result, resultD, how="left", left_on='a1', right_on='ad01')
    result = result.fillna(0)

    result['D121314'] = result['d_i4'] * result['smult']
    result = result.drop(columns=['ad01', 'd_i4'])

    result['V5'] = result['V4'] + (result['D8910'] - result['D121314']) / (10 ** 5)
    result = result.drop(columns=['D8910', 'D121314'])

    # 6. Invested Capital
    result['V6'] = result['V3'] + result['V4']

    # 7. Gross Value of additions to fixed capital
    result['V7'] = (result['c_i5'] * result['smult']) / (10 ** 5)

    # 8. Rent paid
    # F vals
    result = pd.merge(result, blkF_all, how="left", left_on='a1', right_on='af01')
    result = result.fillna(0)

    result['V8'] = (result['f9'] * result['smult']) / (10 ** 5)

    # 9. Outstanding Loan
    blkD_subset = blkD_all[blkD_all['d_i1'] == 17].copy()[['ad01', 'd_i4']]
    result = pd.merge(result, resultD, how="left", left_on='a1', right_on='ad01')
    result = result.fillna(0)

    result['V9'] = (result['d_i4'] * result['smult']) / (10 ** 5)

    # 10. Interest paid
    result['V10'] = (result['f10'] * result['smult']) / (10 ** 5)

    # 11. Rent received
    # G vals
    result = pd.merge(result, blkG_all, how="left", left_on='a1', right_on='ag01')
    result = result.fillna(0)
    result['V11'] = (result['g9'] * result['smult']) / (10 ** 5)

    # 12. Interest received
    result['V12'] = (result['g10'] * result['smult']) / (10 ** 5)

    # 13. Gross value of P&M
    blkC_subset = blkC_all[blkC_all['c_i1'] == 3].copy()[['ac01', 'c_i7']]
    result = pd.merge(result, blkC_subset, how="left", left_on='a1', right_on='ac01')
    result = result.fillna(0)
    result['V13'] = (result['c_i7'] * result['smult']) / (10 ** 5)

    # 14. Value of Products & By-products
    # J vals
    blkJ_subset = blkJ_all[blkJ_all['j_i1'] != 12].copy()[['aj01', 'j_i13']]
    resultJ = blkJ_subset.groupby('aj01').sum().reset_index()
    result = pd.merge(result, resultJ, how="left", left_on='a1', right_on='aj01')

    result['V14'] = ((result['j_i13'] + result['g4'] + result['g7']) * result['smult']) / (10 ** 5)

    # 15. Other Output

    g_array = [1, 2, 3, 6, 8, 11]
    g_vals = result['g' + str(g_array[0])]
    for x in g_array[1:]:
        g_vals += result['g' + str(x)]

    result['V15'] = ((g_vals + result['f7']) * result['smult']) / (10 ** 5)

    # 16. Total Output

    result['V16'] = result['V14'] + result['V15']

    # 17. Fuels consumed
    # H vals
    blkH_subset = blkH_all[(blkH_all['h_i1'] >= 16) & (blkH_all['h_i1'] <= 20)].copy()[['ah01', 'h_i6']]
    resultH = blkH_subset.groupby('ah01').sum().reset_index()
    result = pd.merge(result, resultH, how="left", left_on='a1', right_on='ah01')
    result = result.fillna(0)

    result['V17'] = (result['h_i6'] * result['smult']) / (10 ** 5)
    del result['h_i6']

    # 18. Materials consumed for Manufacturing

    # H vals
    blkH_subset = \
    blkH_all[(blkH_all['h_i1'].isin([_ for _ in range(1, 12)] + [13, 14, 21])) | (blkH_all['h_i1'] > 24)].copy()[
        ['ah01', 'h_i6']]
    resultH = blkH_subset.groupby('ah01').sum().reset_index()
    result = pd.merge(result, resultH, how="left", left_on='a1', right_on='ah01')

    # I vals
    blkI_subset = blkI_all[blkI_all['i_i1'] != 7].copy()[['ai01', 'i_i6']]
    resultI = blkI_subset.groupby('ai01').sum().reset_index()
    result = pd.merge(result, resultI, how="left", left_on='a1', right_on='ai01')

    result = result.fillna(0)

    result['V18'] = ((result['h_i6'] + result['i_i6']) * result['smult']) / (10 ** 5)

    # 19. Other Input
    f_array = [1, "2a", "2b", 3, 4, 6, 7, 8, 11]
    f_vals = result['f' + str(f_array[0])]
    for x in f_array[1:]:
        f_vals += result['f' + str(x)]

    result['V19'] = (f_vals * result['smult']) / (10 ** 5)

    # 20. Total Input

    result['V20'] = result['V17'] + result['V18'] + result['V19']

    # 21. GVA

    result['V21'] = result['V16'] - result['V20']

    # 22. Depreciation

    result['V22'] = (result['c_i9'] * result['smult']) / (10 ** 5)

    # 23. NVA

    result['V23'] = result['V21'] - result['V22']

    # A. Employees and C11. Salary to Employees
    # E vals
    blkE_subset = blkE_all[blkE_all['e_i1'].isin([1, 2, 4, 6, 7])].copy()[['ae01', 'e_i6', 'e_i8']]
    resultE = blkE_subset.groupby('ae01').sum().reset_index()
    result = pd.merge(result, resultE, how="left", left_on='a1', right_on='ae01')
    result = result.fillna(0)
    result['VA'] = result['e_i6'] * result['smult']
    result['VC11'] = (result['e_i8'] * result['smult']) / (10 ** 5)
    result = result.drop(columns=['ae01', 'e_i6', 'e_i8'])

    # A1. Workers and C111. Wage to Workers
    # E vals
    blkE_subset = blkE_all[blkE_all['e_i1'].isin([1, 2, 4])].copy()[['ae01', 'e_i6', 'e_i8']]
    resultE = blkE_subset.groupby('ae01').sum().reset_index()
    result = pd.merge(result, resultE, how="left", left_on='a1', right_on='ae01')
    result = result.fillna(0)
    result['VA1'] = result['e_i6'] * result['smult']
    result['VC111'] = (result['e_i8'] * result['smult']) / (10 ** 5)

    result = result[['sscode', 'a1', 'a3', 'a5', 'a8', 'stratum', 'smult'] + params]

    result['stratum'] = result['stratum'].apply(fix_nic_stratum)
    result['a5'] = result['a5'].apply(fix_nic_a5)

    census_result = result[result['a3'] == '1'].copy()
    sample_result = result[result['a3'] == '2'].copy()

    # result.to_csv("result_smult.csv", index=False)

    # Making the final table
    # Y_ismk for each stratum, m = 1,3 and m = 2,4
    y_ismk = sample_result.groupby(['stratum', 'sscode'])
    y_ismk = y_ismk.sum().reset_index()
    y_ismk = y_ismk[['stratum', 'sscode'] + params]

    Y_ism = y_ismk

    state_full = []
    dist_full = []
    nic2dig_full = []
    nic3dig_full = []
    for param in params:
        var_table = getVarTable(Y_ism, param)
        state_res, dist_res, nic2dig_res, nic3dig_res = getRSEestimates(var_table)
        state_full.append(state_res)
        dist_full.append(dist_res)
        nic2dig_full.append(nic2dig_res)
        nic3dig_full.append(nic3dig_res)

    census_result['distcode'] = census_result['a8']
    census_result['nic2dig'] = census_result['a5'].str[:2]
    census_result['nic3dig'] = census_result['a5'].str[:3]
    census_result = census_result[
        ['a1', 'nic2dig', 'nic3dig', 'distcode'] + params]

    # census_result.to_csv("census_result.csv", index=False)

    dist_census = census_result.groupby('distcode').sum().reset_index()[
        ['distcode'] + params]
    nic2dig_census = census_result.groupby('nic2dig').sum().reset_index()[
        ['nic2dig'] + params]
    nic3dig_census = census_result.groupby('nic3dig').sum().reset_index()[
        ['nic3dig'] + params]


    dist_est_P, nic2dig_est_P, nic3dig_est_P, dist_est_C, nic2dig_est_C, nic3dig_est_C, dist_est_S, nic2dig_est_S, nic3dig_est_S = total_estimates(
        dist_full,
        nic2dig_full,
        nic3dig_full,
        dist_census,
        nic2dig_census,
        nic3dig_census)
    dist_rse_P, nic2dig_rse_P, nic3dig_rse_P, dist_rse_C, nic2dig_rse_C, nic3dig_rse_C, dist_rse_S, nic2dig_rse_S, nic3dig_rse_S = rse_estimates(
        state_full,
        dist_full,
        nic2dig_full,
        nic3dig_full)

    new_params = ['NF', 'FC', 'PWC', 'WC', 'IC', 'GVAFC', 'RentP', 'OL', 'IntrP', 'RentR', 'IntrR',
                  'GVPM', 'VPBP', 'TO', 'FUEL', 'MCM', 'TI', 'GVA', 'DEP', 'NVA', 'NE', 'SE', 'NW', 'WTW']

    return jsonify({
        "dist_est_P": [dist_est_P.columns.tolist()[:1] + new_params] + dist_est_P.to_numpy().tolist(),
        "nic2dig_est_P": [nic2dig_est_P.columns.tolist()[:1] + new_params] + nic2dig_est_P.to_numpy().tolist(),
        "nic3dig_est_P": [nic3dig_est_P.columns.tolist()[:1] + new_params] + nic3dig_est_P.to_numpy().tolist(),
        "dist_est_C": [dist_est_C.columns.tolist()[:1] + new_params] + dist_est_C.to_numpy().tolist(),
        "nic2dig_est_C": [nic2dig_est_C.columns.tolist()[:1] + new_params] + nic2dig_est_C.to_numpy().tolist(),
        "nic3dig_est_C": [nic3dig_est_C.columns.tolist()[:1] + new_params] + nic3dig_est_C.to_numpy().tolist(),
        "dist_est_S": [dist_est_S.columns.tolist()[:1] + new_params] + dist_est_S.to_numpy().tolist(),
        "nic2dig_est_S": [nic2dig_est_S.columns.tolist()[:1] + new_params] + nic2dig_est_S.to_numpy().tolist(),
        "nic3dig_est_S": [nic3dig_est_S.columns.tolist()[:1] + new_params] + nic3dig_est_S.to_numpy().tolist(),
        "dist_rse_P": [dist_rse_P.columns.tolist()[:1] + new_params] + dist_rse_P.to_numpy().tolist(),
        "nic2dig_rse_P": [nic2dig_rse_P.columns.tolist()[:1] + new_params] + nic2dig_rse_P.to_numpy().tolist(),
        "nic3dig_rse_P": [nic3dig_rse_P.columns.tolist()[:1] + new_params] + nic3dig_rse_P.to_numpy().tolist(),
        "dist_rse_C": [dist_rse_C.columns.tolist()[:1] + new_params] + dist_rse_C.to_numpy().tolist(),
        "nic2dig_rse_C": [nic2dig_rse_C.columns.tolist()[:1] + new_params] + nic2dig_rse_C.to_numpy().tolist(),
        "nic3dig_rse_C": [nic3dig_rse_C.columns.tolist()[:1] + new_params] + nic3dig_rse_C.to_numpy().tolist(),
        "dist_rse_S": [dist_rse_S.columns.tolist()[:1] + new_params] + dist_rse_S.to_numpy().tolist(),
        "nic2dig_rse_S": [nic2dig_rse_S.columns.tolist()[:1] + new_params] + nic2dig_rse_S.to_numpy().tolist(),
        "nic3dig_rse_S": [nic3dig_rse_S.columns.tolist()[:1] + new_params] + nic3dig_rse_S.to_numpy().tolist(),
    })
