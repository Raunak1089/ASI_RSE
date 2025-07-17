from flask import Flask, render_template, request, jsonify
import os
import pandas as pd
from script import exportTables

app = Flask(__name__)
UPLOAD_FOLDER = 'uploaded_files'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Global vars
blkA = blkB = blkC = blkD = blkE = blkF = blkG = blkH = blkI = blkJ = sscode = multiplier = None


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    global blkA, blkB, blkC, blkD, blkE, blkF, blkG, blkH, blkI, blkJ, sscode, multiplier

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
            blkA = pd.read_csv(file_map['asi_a_v.CSV'])
            blkB = pd.read_csv(file_map['asi_b_v.CSV'])
            blkC = pd.read_csv(file_map['asi_c_v.CSV'])
            blkD = pd.read_csv(file_map['asi_d_v.CSV'])
            blkE = pd.read_csv(file_map['asi_e_v.CSV'])
            blkF = pd.read_csv(file_map['asi_f_v.CSV'])
            blkG = pd.read_csv(file_map['asi_g_v.CSV'])
            blkH = pd.read_csv(file_map['asi_h_v.CSV'])
            blkI = pd.read_csv(file_map['asi_i_v.CSV'])
            blkJ = pd.read_csv(file_map['asi_j_v.CSV'])
            sscode = pd.read_csv(file_map['sscode.CSV'])
            multiplier = pd.read_csv(file_map['multiplier.CSV'])

            return exportTables(blkA, blkB, blkC, blkD, blkE, blkF, blkG, blkH, blkI, blkJ, sscode, multiplier)

        except Exception as e:
            return jsonify({"error": e})

    return jsonify({"error": "Missing required files"}), 400


app.run()
