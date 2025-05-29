from flask import Flask, render_template, redirect
from dashboard import create_dash_app
from request_page import create_request_app

server = Flask(__name__)

# Mount both Dash apps
create_dash_app(server)          # Mounts at "/dash/"
create_request_app(server)       # Mounts at "/request/"

@server.route('/')
def home():
    return render_template('home.html', route='home')

@server.route('/readiness_check')
def readiness_check():
    return "App is running", 200

@server.route('/dashboard')
def dashboard():
    #return render_template('home.html', route='dashboard')
    return redirect('/dash/')

@server.route('/request')
def request():
    return render_template('home.html', route='request')

if __name__ == "__main__":
    server.run(debug=True, host='0.0.0.0', port=8050)
