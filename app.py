from flask import Flask, render_template, request, jsonify
import os
import sys
import pandas as pd
from script import exportTables

import threading
import webbrowser
import time

app = Flask(__name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploaded_files')
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


import shutil


@app.route('/delete_uploads', methods=['POST'])
def delete_uploads():
    try:
        if os.path.exists(UPLOAD_FOLDER):
            shutil.rmtree(UPLOAD_FOLDER)
            return jsonify({"status": "success", "message": "Upload folder deleted"})
        else:
            return jsonify({"status": "not_found", "message": "No such folder"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/shutdown', methods=['POST'])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is not None:
        func()
        return 'Server shutting down...'
    else:
        # Fallback for when not using Werkzeug (e.g., PyInstaller .exe)
        shutdown_html = """
        <html><body>
        <h2>Server stopped. Please close this tab.</h2>
        <script>setTimeout(() => { window.close(); }, 1000);</script>
        </body></html>
        """
        # Delay to let the browser get the response
        threading.Timer(1.0, lambda: os._exit(0)).start()
        return shutdown_html


import tkinter as tk
from PIL import Image, ImageTk


def resource_path(relative_path):
    """ Get absolute path to resource, works for PyInstaller and dev """
    try:
        base_path = sys._MEIPASS  # PyInstaller sets this at runtime
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def show_splash_screen():
    splash = tk.Tk()
    splash.overrideredirect(True)
    splash.configure(background='white')

    screen_width = splash.winfo_screenwidth()
    screen_height = splash.winfo_screenheight()

    # Load the image from bundled path
    img_path = resource_path("splash_screen.png")
    img = Image.open(img_path)
    img = img.resize((384, 245))  # Resize as needed
    photo = ImageTk.PhotoImage(img)

    canvas = tk.Canvas(splash, width=384, height=245, highlightthickness=0, bg='white')
    canvas.pack()
    canvas.create_image(192, 122, image=photo)

    x = (screen_width // 2) - 192
    y = (screen_height // 2) - 122
    splash.geometry(f"384x245+{x}+{y}")

    splash.after(2500, splash.destroy)
    splash.mainloop()


def open_browser():
    time.sleep(1)
    webbrowser.open("http://127.0.0.1:5000/")


if __name__ == "__main__":
    show_splash_screen()
    threading.Thread(target=open_browser).start()
    app.run()
