from flask import Flask, render_template, request, redirect, url_for, session, send_file
import io
import grpc
import lms_pb2
import lms_pb2_grpc
import logging
import os
from urllib.parse import quote, unquote
from werkzeug.utils import secure_filename
import requests

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'supersecretkey'
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    FILE_STORAGE_DIR='documents'
)

# gRPC Channel Setup
channel = grpc.insecure_channel('lms_server:50051')
stub = lms_pb2_grpc.LMSStub(channel)
logger.info("Client connected to LMS Server")

# Utilities
def check_session():
    if 'token' not in session:
        return False
    return True

def handle_grpc_error(e):
    logger.error(f"gRPC error: {e.code()} - {e.details()}")
    return "An error occurred. Please try again later.", 500

@app.route('/')
def home():
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        role = request.form['role']
        name = request.form.get('name', '')
        try:
            response = stub.Register(lms_pb2.RegisterRequest(username=username, password=password, role=role, name=name))
            if response.status == "Registration successful":
                return redirect(url_for('home'))
            return "Registration Failed: User already exists", 400
        except grpc.RpcError as e:
            return handle_grpc_error(e)
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        try:
            response = stub.Login(lms_pb2.LoginRequest(username=username, password=password))
            if response.status == "Success":
                session['token'] = response.token
                session['role'] = response.role
                session['username'] = username
                session['logged_in'] = True
                return redirect(url_for('dashboard'))
            return render_template('login.html', error="Incorrect username or password"), 401
        except grpc.RpcError as e:
            return handle_grpc_error(e)
    return render_template('login.html')

@app.route('/dashboard')
def dashboard():
    if not check_session():
        return redirect(url_for('home'))
    return render_template('dashboard.html', role=session['role'])

@app.route('/assignments', methods=['GET', 'POST'])
def assignments():
    if not check_session():
        return redirect(url_for('home'))
    if request.method == 'POST':
        return handle_assignments_post()
    return render_assignments_get()

def handle_assignments_post():
    if session['role'] == 'teacher':
        for assignment_id, grade in request.form.items():
            if assignment_id.startswith('grade_'):
                assignment_id_cleaned = assignment_id[len('grade_'):]
                try:
                    # Sending grade
                    response = stub.Post(lms_pb2.PostRequest(
                        token=session['token'],
                        assignment=lms_pb2.AssignmentUpdate(
                            assignment_id=assignment_id_cleaned,
                            grade=grade
                        )
                    ))
                    if response.status != "Assignment grade updated successfully":
                        logger.warning(f"Failed to post grade: {response.status}")
                except grpc.RpcError as e:
                    return handle_grpc_error(e)
        
        feedbacks = {k: v for k, v in request.form.items() if k.startswith('feedback_')}
        for assignment_id, feedback in feedbacks.items():
            assignment_id_cleaned = assignment_id[len('feedback_'):]
            try:
                # Sending feedback
                response = stub.Post(lms_pb2.PostRequest(
                    token=session['token'],
                    assignment=lms_pb2.AssignmentUpdate(
                        assignment_id=assignment_id_cleaned,
                        feedback_text=feedback
                    )
                ))
                if response.status != "Assignment grade updated successfully":
                    logger.warning(f"Failed to post feedback: {response.status}")
            except grpc.RpcError as e:
                return handle_grpc_error(e)
    
    if 'assignment' in request.files:
        uploaded_file = request.files['assignment']
        file_content = uploaded_file.read() 

        if uploaded_file.filename != '':
            file_save_response = stub.Upload(lms_pb2.UploadFileRequest(
                token=session['token'],
                    filename=secure_filename(uploaded_file.filename),
                    data=file_content
            ))

            if file_save_response.status == "success":
                response = stub.Post(lms_pb2.PostRequest(
                        token=session['token'],
                        assignment=lms_pb2.AssignmentData(
                            student_name=session['username'],
                            filename=secure_filename(uploaded_file.filename),
                            file_path=file_save_response.file_path,
                            file_id = str(file_save_response.file_id)
                        )
                    ))
                logger.info(f"Assignment data uploaded successfully: {uploaded_file.filename}")
            else:
                logger.info(f"response.status: {file_save_response.status}")
                logger.error(f"File could not be uploaded successfully: {uploaded_file.filename}")
    
    return redirect(url_for('assignments'))

def render_assignments_get():
    try:
        role = session['role']
        username = session['username']
        
        if role == 'teacher':
            request_data = lms_pb2.AssignmentData(teacher_name=username)
        elif role == 'student':
            request_data = lms_pb2.AssignmentData(student_name=username)
        else:
            return "Unknown role", 400

        response = stub.Get(lms_pb2.GetRequest(
            token=session['token'],
            assignment=request_data
        ))

        assignments = []
        for item in response.assignment_items:
            assignments.append({
                'assignment_id': item.assignment_id,
                'student_name': item.student_name,
                'teacher_name': item.teacher_name,
                'filename': item.filename,
                'file_path': item.file_path,
                'grade': item.grade,
                'feedback': item.feedback_text,
                'submission_date': item.submission_date,
            })

        return render_template('assignments.html', assignments=assignments, role=role)
    except grpc.RpcError as e:
        return handle_grpc_error(e)

@app.route('/download/<path:file_path>')
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

@app.route('/feedback', methods=['GET', 'POST'])
def feedback():
    if not check_session():
        return redirect(url_for('home'))
    if request.method == 'POST':
        feedback_text = request.form['feedback']
        try:
            response = stub.Post(lms_pb2.PostRequest(
                token=session['token'],
                student_feedback=lms_pb2.FeedbackData(
                    feedback_text=feedback_text,
                    student_name=session['username']
                )
            ))
            if response.status == "Success":
                return redirect(url_for('feedback'))
            return "Failed to submit feedback", 400
        except grpc.RpcError as e:
            return handle_grpc_error(e)
    return render_feedback_get()

def render_feedback_get():
    try:
        response = stub.Get(lms_pb2.GetRequest(
            token=session['token'],
            data_type=lms_pb2.GetRequest.student_feedback
        ))
        feedback_items = [{'feedback_id': item.feedback_id, 'feedback_text': item.feedback_text} for item in response.items]
        return render_template('feedback.html', feedback=feedback_items)
    except grpc.RpcError as e:
        return handle_grpc_error(e)

@app.route('/course-material', methods=['GET', 'POST'])
def course_material():
    if not check_session():
        return redirect(url_for('home'))
    if request.method == 'POST':
        if session['role'] == 'teacher':
            uploaded_file = request.files['material']
            if uploaded_file.filename != '':
                save_course_material(uploaded_file)
        return redirect(url_for('course_material'))
    return render_course_material_get()

def render_course_material_get():
    try:
        response = stub.Get(lms_pb2.GetRequest(
            token=session['token'],
            data_type=lms_pb2.GetRequest.course_material
        ))
        materials = [{'filename': item.filename, 'file_path': item.file_path} for item in response.items]
        return render_template('course_material.html', materials=materials, role=session['role'])
    except grpc.RpcError as e:
        return handle_grpc_error(e)
    
@app.route('/logout')
def logout():
    token = session.pop('token', None)
    if token:
        try:
            stub.Logout(lms_pb2.LogoutRequest(token=token))
            session.clear()
        except grpc.RpcError as e:
            handle_grpc_error(e)
    return redirect(url_for('home'))

def save_assignment(file):
    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config['FILE_STORAGE_DIR'], filename)
    file.save(file_path)

def save_course_material(file):
    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config['FILE_STORAGE_DIR'], filename)
    file.save(file_path)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
