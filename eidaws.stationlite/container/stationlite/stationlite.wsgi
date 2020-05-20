# use a virtual environment
activate_this = '/var/www/eidaws-stationlite/venv/bin/activate_this.py'
with open(activate_this) as file_:
    exec(file_.read(), dict(__file__=activate_this))


from eidaws.stationlite.server.wsgi import application
