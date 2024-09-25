from flask import Flask, render_template, request, redirect, url_for, session, send_file, Blueprint
import io
import lms_pb2
import grpc
from grpc_client import stub, handle_grpc_error
from urllib.parse import quote, unquote
import logging

bp = Blueprint('assignments', __name__)
logger = logging.getLogger(__name__)


@bp.route('/download/<path:file_path>')
def download_file(file_path):
    try:
        # Decode the file path to handle special characters and slashes
        file_path = unquote(file_path)
        
        response = stub.Download(lms_pb2.DownloadFileRequest(
            token=session['token'],
            file_path=file_path
        ))

        if response.status == "success":
            return send_file(
                io.BytesIO(response.data),
                download_name =os.path.basename(file_path),
                as_attachment=True
            )
        else:
            return "File not found", 404

    except grpc.RpcError as e:
        return handle_grpc_error(e)
    

def save_assignment(file):
    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config['FILE_STORAGE_DIR'], filename)
    file.save(file_path)

def save_course_material(file):
    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config['FILE_STORAGE_DIR'], filename)
    file.save(file_path)
