from flask import Flask
from .socketio_instance import socketio
from . import sockets  # This auto-registers all handlers
import logging
from flask_cors import CORS

def create_app(config=None):
    app = Flask(__name__)
    CORS(app)
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize SocketIO with app
    socketio.init_app(app)
    
    # # Optional: Add HTTP routes
    # from . import routes
    # app.register_blueprint(routes.bp)
    
    return app