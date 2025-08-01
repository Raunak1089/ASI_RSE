# RSE Calculator
Live demo hoisted [here](https://raunak1089.pythonanywhere.com/asi_rse).
Download [EXE file here](https://drive.google.com/file/d/1VcUufd1B4fzI7wnp0CSFRtOfGUI02x6r/view?usp=sharing) to use the calculator on any platform without any python installation.

A lightweight Flask-based application to compute Relative Standard Error (RSE) for industrial survey parameters. This project is tailored for use with ASI data and includes a simple GUI interface via the browser.

---


## 📁 Project Structure
```
rse_app/
├── Templates/ # HTML template for Webpage
│ └── index.html
│
├── app.py # Main Flask app
├── script.py # python script to return estimate tables
├── sampleEstimates.py # contains functions necessary for calculation in script.py
├── splash_screen.png # Used in native splash (not for HTML)
├── Emblem_of_India.png # Used as the PyInstaller icon
```

---

## 🧪 Running the App

First, install the required dependencies:

```bash
pip install flask pandas numpy
```

Then, to run the app:

```bash
python app.py
```

## ⬇️ Installing the App as Exe file

Install pyinstaller in python:

```bash
pip install pyinstaller
```

Run the following command in the directory containing app.py:
```bash
pyinstaller -w -F ^
    --add-data "Templates;templates" ^
    --add-data "splash_screen.png;." ^
    --hidden-import pandas ^
    --hidden-import numpy ^
    --hidden-import script ^
    --icon=Emblem_of_India.png ^
    --name "RSE Calculator" ^
    app.py
```

### Notes:
- The .exe will appear in the dist/ folder after successful compilation.
- Ensure all external files (splash_screen.png, Templates, etc.) are in the same directory as app.py before compiling.

