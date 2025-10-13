# app/process_messages.py
# Back-compat entry so gunicorn target `app.process_messages:app` still works.
from .wsgi import app
