from flask import Flask, render_template, redirect
from dashboard import create_dash_app
from request_page import create_request_app
from download_page import create_download_app

server = Flask(__name__)

# Mount both Dash apps
create_dash_app(server)          # Mounts at "/dash/"
create_request_app(server)       # Mounts at "/request/"
create_download_app(server)

@server.route('/')
def home():
    # change this if necesary
    return render_template('home.html', route='home')
    # return redirect('/dash/')

@server.route('/readiness_check')
def readiness_check():
    return "App is running", 200

@server.route('/dashboard')
def dashboard():
    return render_template('home.html', route='dashboard')
    #return redirect('/dash/')

@server.route('/request')
def request():
    return render_template('home.html', route='request')

@server.route('/download')
def download():
    return render_template('home.html', route='download')


filtered_df_cache = {}  # ensure this is global and accessible

@server.route("/download")
def download_filtered_csv():
    df = filtered_df_cache.get("df")
    if df is None or df.empty:
        return "No data available for download.", 400

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    return send_file(
        io.BytesIO(buffer.getvalue().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name="filtered_stock_data.csv"
    )

if __name__ == "__main__":
    server.run(debug=True, host='0.0.0.0', port=8050)
