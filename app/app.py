from flask import Flask, render_template
from dashboard import create_dash_app

server = Flask(__name__)
create_dash_app(server)

@server.route('/')
def home():
    return render_template('home.html')

@server.route('/dashboard')
def dashboard_page():
    return render_template('home.html')  # could use a different template too

if __name__ == "__main__":
    server.run(debug=True)
