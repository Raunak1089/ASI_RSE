from flask import Flask, render_template, request, jsonify
import os
import pandas as pd
from script import processData

app = Flask(__name__)
UPLOAD_FOLDER = 'uploaded_files'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Global vars
blkA = blkB = blkC = blkD = blkE = blkF = blkG = blkH = blkI = blkJ = sscode = multiplier = None
sel_n_total = cap_n_total = None


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    global blkA, blkB, blkC, blkD, blkE, blkF, blkG, blkH, blkI, blkJ, sscode, multiplier
    global sel_n_total, cap_n_total

    uploaded_files = request.files.getlist("files[]")
    filenames = [
        'asi_a_v.CSV', 'asi_b_v.CSV', 'asi_c_v.CSV', 'asi_d_v.CSV', 'asi_e_v.CSV',
        'asi_f_v.CSV', 'asi_g_v.CSV', 'asi_h_v.CSV', 'asi_i_v.CSV', 'asi_j_v.CSV',
        'sscode.CSV', 'multiplier.CSV'
    ]

    file_map = {}
    for f in uploaded_files:
        name = f.filename
        if name.lower() in [x.lower() for x in filenames]:
            matched_name = next(orig for orig in filenames if orig.lower() == name.lower())
            filepath = os.path.join(UPLOAD_FOLDER, matched_name)
            f.save(filepath)
            file_map[matched_name] = filepath

    if all(fname in file_map for fname in filenames):
        try:
            blkA = pd.read_csv(file_map['asi_a_v.CSV'], dtype=str)
            blkB = pd.read_csv(file_map['asi_b_v.CSV'], dtype=str)
            blkC = pd.read_csv(file_map['asi_c_v.CSV'], dtype=str)
            blkD = pd.read_csv(file_map['asi_d_v.CSV'], dtype=str)
            blkE = pd.read_csv(file_map['asi_e_v.CSV'], dtype=str)
            blkF = pd.read_csv(file_map['asi_f_v.CSV'], dtype=str)
            blkG = pd.read_csv(file_map['asi_g_v.CSV'], dtype=str)
            blkH = pd.read_csv(file_map['asi_h_v.CSV'], dtype=str)
            blkI = pd.read_csv(file_map['asi_i_v.CSV'], dtype=str)
            blkJ = pd.read_csv(file_map['asi_j_v.CSV'], dtype=str)
            sscode = pd.read_csv(file_map['sscode.CSV'], dtype=str)
            multiplier = pd.read_csv(file_map['multiplier.CSV'], dtype=str)

            return processData(blkA, blkB, blkC, blkD, blkE, blkF, blkG, blkH, blkI, blkJ, sscode, multiplier)
        except Exception as e:
            return jsonify({"error": e})

    return jsonify({"error": "Missing required files"}), 400


app.run()
