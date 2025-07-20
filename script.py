from flask import jsonify
import pandas as pd
from sampleEstimates import total_estimates, rse_estimates, merge_sum, merge_sum_cols, getVarTable, getRSEestimates

params = ['NF', 'FC', 'PWC', 'WC', 'IC', 'GVAFC', 'RentP', 'OL', 'IntrP', 'RentR', 'IntrR',
         'GVPM', 'VPBP', 'TO', 'FUEL', 'MCM', 'TI', 'GVA', 'DEP', 'NVA', 'NE', 'SE', 'NW', 'WTW']


def exportTables(blkA, blkB, blkC, blkD, blkE, blkF, blkG, blkH, blkI, blkJ, sscode, multiplier):
    try:
        table_results = processData(blkA, blkB, blkC, blkD, blkE, blkF, blkG, blkH, blkI, blkJ, sscode, multiplier)
        return table_results
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)})


def processData(blkA, blkB, blkC, blkD, blkE, blkF, blkG, blkH, blkI, blkJ, sscode, multiplier):
    # Check for discrepancies in data
    blk_dict = {
        'blkA': blkA,
        'blkB': blkB,
        'blkC': blkC,
        'blkD': blkD,
        'blkE': blkE,
        'blkF': blkF,
        'blkG': blkG,
        'blkH': blkH,
        'blkI': blkI,
        'blkJ': blkJ,
    }

    # 1. Check number of columns
    expected_cols = {
        'blkA': 22, 'blkB': 12, 'blkC': 15, 'blkD': 6, 'blkE': 10,
        'blkF': 15, 'blkG': 15, 'blkH': 9, 'blkI': 9, 'blkJ': 15
    }
    for blk_name, expected in expected_cols.items():
        actual = blk_dict[blk_name].shape[1]
        if actual != expected:
            raise Exception(f"{blk_name} should have {expected} columns, found {actual}")

    # 2. Blocks A, B, F, G should not have duplicate DSL (3rd column)
    for blk in ['blkA', 'blkB', 'blkF', 'blkG']:
        dsl_col = blk_dict[blk].columns[2]
        if blk_dict[blk][dsl_col].duplicated().any():
            raise Exception(f"{blk} has duplicate DSL values in column '{dsl_col}'")

    # 3. Multiplier must have exactly these columns
    expected_mult_cols = {'state', 'distcode', 'sector', 'nic03', 'sstrm', 'mult1', 'mult2', 'mult3', 'mult4'}
    if not expected_mult_cols.issubset(multiplier.columns):
        raise Exception(f"Multiplier missing columns: {expected_mult_cols - set(multiplier.columns)}")

    # 4. SScode must contain these columns
    expected_ss_cols = {'dsl', 'sscode', 'bfe'}
    if not expected_ss_cols.issubset(sscode.columns):
        raise Exception(f"SScode missing columns: {expected_ss_cols - set(sscode.columns)}")

    # 5. All blocks except blkA must have DSLs ? blkA
    dslA = set(blk_dict['blkA'].iloc[:, 2])
    for blk in blk_dict:
        if blk == 'blkA':
            continue
        dsl_other = set(blk_dict[blk].iloc[:, 2])
        missing = dsl_other - dslA
        if missing:
            raise Exception(f"{blk} contains unknown DSLs not in blkA: {missing}")

    print("All checks passed successfully.")

    # Rename columns correctly

    names_lists = [
        ['yr', 'blk', 'a1', 'a2', 'a3', 'a4', 'a5', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'bonus', 'pf', 'welfare',
         'mwdays', 'nwdays', 'wdays', 'costop', 'expshare', 'mult'],
        ['yr', 'blk', 'ab01', 'b02', 'b03', 'b04', 'b05', 'b06f', 'b06t', 'b07', 'b08', 'b09'],
        ['yr', 'blk'] + ['c' + str(i) for i in range(1, 14)],
        ['yr', 'blk'] + ['d' + str(i) for i in range(1, 5)],
        ['yr', 'blk'] + ['e' + str(i) for i in range(1, 9)],

        ['yr', 'blk', 'af01', 'f1', 'f2a', 'f2b', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'f10', 'f11'],
        ['yr', 'blk', 'ag01', 'g1', 'g2', 'g3', 'g4', 'g5', 'g6', 'g7', 'g8', 'g9', 'g10', 'g11', 'g12'],

        ['yr', 'blk'] + ['h' + str(i) for i in range(1, 8)],
        ['yr', 'blk'] + ['i' + str(i) for i in range(1, 8)],
        ['yr', 'blk'] + ['j' + str(i) for i in range(1, 14)]
    ]

    for (key, df), colnames in zip(blk_dict.items(), names_lists):
        df.columns = colnames

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

    # Keep Unit Status <= 4 only
    resultC = pd.merge(blkC, blkA[['a1', 'a3', 'a12']], how='left', left_on='c1', right_on='a1')
    blkC_all = resultC[resultC['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultD = pd.merge(blkD, blkA[['a1', 'a3', 'a12']], how='left', left_on='d1', right_on='a1')
    blkD_all = resultD[resultD['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultE = pd.merge(blkE, blkA[['a1', 'a3', 'a12']], how='left', left_on='e1', right_on='a1')
    blkE_all = resultE[resultE['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultF = pd.merge(blkF, blkA[['a1', 'a3', 'a12']], how='left', left_on='af01', right_on='a1')
    blkF_all = resultF[resultF['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultG = pd.merge(blkG, blkA[['a1', 'a3', 'a12']], how='left', left_on='ag01', right_on='a1')
    blkG_all = resultG[resultG['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultH = pd.merge(blkH, blkA[['a1', 'a3', 'a12']], how='left', left_on='h1', right_on='a1')
    blkH_all = resultH[resultH['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultI = pd.merge(blkI, blkA[['a1', 'a3', 'a12']], how='left', left_on='i1', right_on='a1')
    blkI_all = resultI[resultI['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    resultJ = pd.merge(blkJ, blkA[['a1', 'a3', 'a12']], how='left', left_on='j1', right_on='a1')
    blkJ_all = resultJ[resultJ['a12'] <= 4].drop(columns=['a1', 'a3', 'a12'])

    del resultC, resultD, resultE, resultF, resultG, resultH, resultI, resultJ

    blkA_all = blkA[blkA['a12'] <= 4].copy()

    # Fix codes by ensuring 2,3-digit format

    blkA_all['a4'] = blkA_all['a4'].astype('str').str.zfill(4)
    blkA_all['a5'] = blkA_all['a5'].astype('str').str.zfill(5)
    blkA_all['a7'] = blkA_all['a7'].astype('str').str.zfill(2)
    blkA_all['a8'] = blkA_all['a8'].astype('str').str.zfill(2)

    multiplier['state'] = multiplier['state'].astype('str').str.zfill(2)
    multiplier['distcode'] = multiplier['distcode'].astype('str').str.zfill(2)
    multiplier['nic03'] = multiplier['nic03'].astype('str').str.zfill(3)

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

    # Create 'stratum' identifier by concatenating: state (a7), district (a8), sector (bfe),
    # and first 3 digits of NIC code (a5)
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
        multiplier[['sstrm', 'mult1', 'mult2', 'mult3', 'mult4', 'stratum']],
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

    # 1. Number of Factories (NF)
    resultmult['NF'] = resultmult['a11'] * resultmult['smult']

    # 3. Fixed Capital (FC)
    result = merge_sum(resultmult, blkC_all, [1, 2, 3, 4, 5, 6, 7, 9], 13, 'FC')

    # 4. Physical Working Capital (PWC)
    result = merge_sum(result, blkD_all, [1, 2, 3, 5, 6], 4, 'PWC')

    # 5. Working Capital (WC)
    result = merge_sum(result, blkD_all, [8, 9, 10], 4, 'D8910')
    result = merge_sum(result, blkD_all, [12, 13, 14], 4, 'D121314')
    result['WC'] = result['PWC'] + result['D8910'] - result['D121314']
    result = result.drop(columns=['D8910', 'D121314'])

    # 6. Invested Capital (IC) = FC + PWC
    result['IC'] = result['FC'] + result['PWC']

    # 7. Gross Value of Additions to Fixed Capital (GVAFC)
    result = merge_sum(result, blkC_all, [1, 2, 3, 4, 5, 6, 7, 9], 5, 'GVAFC')

    # 8. Rent Paid (RentP)
    result = merge_sum_cols(result, blkF_all, ['f9'], 'RentP')

    # 9. Outstanding Loan (OL)
    result = merge_sum(result, blkD_all, [17], 4, 'OL')

    # 10. Interest Paid (IntrP)
    result = merge_sum_cols(result, blkF_all, ['f10'], 'IntrP')

    # 11. Rent Received (RentR)
    result = merge_sum_cols(result, blkG_all, ['g9'], 'RentR')

    # 12. Interest Received (IntrR)
    result = merge_sum_cols(result, blkG_all, ['g10'], 'IntrR')

    # 13. Gross Value of Plant & Machinery (GVPM)
    result = merge_sum(result, blkC_all, [3], 7, 'GVPM')

    # 14. Value of Products & By-products (VPBP)
    j_sno = list(set(blkJ_all.iloc[:, 1]) - {12})  # j1 != 12
    result = merge_sum(result, blkJ_all, j_sno, 13, 'blkJ13')
    result = merge_sum_cols(result, blkG_all, ['g4'], 'blkG4')
    result = merge_sum_cols(result, blkG_all, ['g7'], 'blkG7')
    result['VPBP'] = result['blkJ13'] + result['blkG4'] + result['blkG7']
    result = result.drop(columns=['blkJ13', 'blkG4', 'blkG7'])

    # 15. Other Output (OO)
    result = merge_sum_cols(result, blkG_all, ['g' + str(i) for i in [1, 2, 3, 6, 8, 11]], 'Gval')
    result = merge_sum_cols(result, blkF_all, ['f7'], 'blkF7')
    result['OO'] = result['Gval'] + result['blkF7']  # OO kept as intermediate calculation
    result = result.drop(columns=['Gval', 'blkF7'])

    # 16. Total Output (TO) = VPBP + OO
    result['TO'] = result['VPBP'] + result['OO']

    # 17. Fuels Consumed (FUEL)
    result = merge_sum(result, blkH_all, [16, 17, 18, 19, 20], 6, 'FUEL')

    # 18. Materials Consumed for Manufacturing (MCM)
    h_sno = []
    for x in list(set(blkH_all.iloc[:, 1])):
        if x in [_ for _ in range(1, 12)] + [13, 14, 21] or x > 24:
            h_sno.append(x)
    result = merge_sum(result, blkH_all, h_sno, 6, 'blkH6')

    i_sno = list(set(blkI_all.iloc[:, 1]) - {7})  # i1 != 7
    result = merge_sum(result, blkI_all, i_sno, 6, 'blkI6')

    result['MCM'] = result['blkH6'] + result['blkI6']
    result = result.drop(columns=['blkH6', 'blkI6'])

    # 19. Other Input (OI)
    result = merge_sum_cols(result, blkF_all, ['f' + str(i) for i in [1, "2a", "2b", 3, 4, 6, 7, 8, 11]], 'OI')

    # 20. Total Input (TI) = FUEL + MCM + OI
    result['TI'] = result['FUEL'] + result['MCM'] + result['OI']

    # 21. Gross Value Added (GVA) = TO - TI
    result['GVA'] = result['TO'] - result['TI']

    # 22. Depreciation (DEP)
    result = merge_sum(result, blkC_all, [1, 2, 3, 4, 5, 6, 7, 9], 9, 'DEP')

    # 23. Net Value Added (NVA) = GVA - DEP
    result['NVA'] = result['GVA'] - result['DEP']

    # Number of Employees (NE)
    result = merge_sum(result, blkE_all, [1, 2, 4, 6, 7], 6, 'NE', scale=1)

    # Salary of Employees (SE)
    result = merge_sum(result, blkE_all, [1, 2, 4, 6, 7], 8, 'SE')

    # Number of Workers (NW)
    result = merge_sum(result, blkE_all, [1, 2, 4], 6, 'NW', scale=1)

    # Wage to Workers (WTW)
    result = merge_sum(result, blkE_all, [1, 2, 4], 8, 'WTW')


    result = result[['sscode', 'a1', 'a3', 'a5', 'a8', 'stratum', 'smult'] + params]
    # result = result[['a3', 'a8'] + params]

    result['stratum'] = result['stratum'].apply(fix_nic_stratum)
    result['a5'] = result['a5'].apply(fix_nic_a5)

    census_result = result[result['a3'] == 1].copy()
    sample_result = result[result['a3'] == 2].copy()

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

    return jsonify({
        "dist_est_P": [dist_est_P.columns.tolist()] + dist_est_P.to_numpy().tolist(),
        "nic2dig_est_P": [nic2dig_est_P.columns.tolist()] + nic2dig_est_P.to_numpy().tolist(),
        "nic3dig_est_P": [nic3dig_est_P.columns.tolist()] + nic3dig_est_P.to_numpy().tolist(),
        "dist_est_C": [dist_est_C.columns.tolist()] + dist_est_C.to_numpy().tolist(),
        "nic2dig_est_C": [nic2dig_est_C.columns.tolist()] + nic2dig_est_C.to_numpy().tolist(),
        "nic3dig_est_C": [nic3dig_est_C.columns.tolist()] + nic3dig_est_C.to_numpy().tolist(),
        "dist_est_S": [dist_est_S.columns.tolist()] + dist_est_S.to_numpy().tolist(),
        "nic2dig_est_S": [nic2dig_est_S.columns.tolist()] + nic2dig_est_S.to_numpy().tolist(),
        "nic3dig_est_S": [nic3dig_est_S.columns.tolist()] + nic3dig_est_S.to_numpy().tolist(),
        "dist_rse_P": [dist_rse_P.columns.tolist()] + dist_rse_P.to_numpy().tolist(),
        "nic2dig_rse_P": [nic2dig_rse_P.columns.tolist()] + nic2dig_rse_P.to_numpy().tolist(),
        "nic3dig_rse_P": [nic3dig_rse_P.columns.tolist()] + nic3dig_rse_P.to_numpy().tolist(),
        "dist_rse_C": [dist_rse_C.columns.tolist()] + dist_rse_C.to_numpy().tolist(),
        "nic2dig_rse_C": [nic2dig_rse_C.columns.tolist()] + nic2dig_rse_C.to_numpy().tolist(),
        "nic3dig_rse_C": [nic3dig_rse_C.columns.tolist()] + nic3dig_rse_C.to_numpy().tolist(),
        "dist_rse_S": [dist_rse_S.columns.tolist()] + dist_rse_S.to_numpy().tolist(),
        "nic2dig_rse_S": [nic2dig_rse_S.columns.tolist()] + nic2dig_rse_S.to_numpy().tolist(),
        "nic3dig_rse_S": [nic3dig_rse_S.columns.tolist()] + nic3dig_rse_S.to_numpy().tolist()
    })
