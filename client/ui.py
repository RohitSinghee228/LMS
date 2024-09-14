from flask import Flask, render_template, request, redirect, url_for, session, send_file
import grpc
import lms_pb2
import lms_pb2_grpc
import logging
import os
from werkzeug.utils import secure_filename
import io
import base64

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'supersecretkey'
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    FILE_STORAGE_DIR='/path/to/storage'  # Ensure this matches the server's directory
)

# gRPC Channel Setup
channel = grpc.insecure_channel('lms_server:50051')
stub = lms_pb2_grpc.LMSStub(channel)
logger.info("Client connected to LMS Server")

@app.route('/')
def home():
    logger.info("Rendering home page")
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        role = request.form['role']
        logger.info(f"Attempting to register user: {username} as {role}")
        try:
            response = stub.Register(lms_pb2.RegisterRequest(username=username, password=password, role=role))
            if response.status == "Registration successful":
                logger.info(f"Registration successful for user: {username}")
                return redirect(url_for('home'))
            else:
                logger.warning(f"Registration failed for user: {username} - {response.status}")
                return "Registration Failed: User already exists", 400
        except grpc.RpcError as e:
            logger.error(f"gRPC error during registration: {e.code()} - {e.details()}")
            return "An error occurred during registration", 500
    logger.info("Rendering registration page")
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        logger.info(f"Login attempt for user: {username}")
        try:
            response = stub.Login(lms_pb2.LoginRequest(username=username, password=password))
            if response.status == "Success":
                session['token'] = response.token
                session['role'] = response.role
                session["logged_in"] = True
                session["username"] = username
                logger.info(f"Login successful for user: {username}")
                return redirect(url_for('dashboard'))
            else:
                logger.warning(f"Login failed for user: {username}")
                return render_template('login.html', error="Login Failed: Incorrect username or password"), 401
        except grpc.RpcError as e:
            logger.error(f"gRPC error during login: {e.code()} - {e.details()}")
            return render_template('login.html', error="An error occurred. Please try again later."), 500
    logger.info("Rendering login page")
    return render_template('login.html')

@app.route('/dashboard')
def dashboard():
    if 'token' not in session:
        logger.info("Access to dashboard denied - no valid session")
        return redirect(url_for('home'))
    logger.info("Rendering dashboard")
    return render_template('dashboard.html')

@app.route('/assignments', methods=['GET', 'POST'])
def assignments():
    if 'token' not in session:
        logger.info("Access to assignments denied - no valid session")
        return redirect(url_for('home'))

    if request.method == 'POST':
        for assignment_id, grade in request.form.items():
            if assignment_id.startswith('grade_'):
                assignment_id = assignment_id[len('grade_'):]
                logger.info(f"Submitting grade {grade} for assignment ID {assignment_id}")
                try:
                    response = stub.Post(
                        lms_pb2.PostRequest(
                            token=session['token'],
                            grade=lms_pb2.GradeData(
                                student_id="student_id_placeholder",  # Replace with actual student_id
                                assignment_id=assignment_id,
                                grade=grade
                            )
                        )
                    )
                    if response.status != "Success":
                        logger.warning(f"Failed to submit grade: {response.status}")
                except grpc.RpcError as e:
                    logger.error(f"gRPC error during grade submission: {e.code()} - {e.details()}")
                    
        feedbacks = {k: v for k, v in request.form.items() if k.startswith('feedback_')}
        for assignment_id, feedback in feedbacks.items():
            assignment_id = assignment_id[len('feedback_'):]
            logger.info(f"Submitting feedback for assignment ID {assignment_id}")
            try:
                response = stub.Post(
                    lms_pb2.PostRequest(
                        token=session['token'],
                        feedback=lms_pb2.FeedbackData(
                            student_id="student_id_placeholder",  # Replace with actual student_id
                            assignment_id=assignment_id,
                            feedback_text=feedback
                        )
                    )
                )
                if response.status != "Success":
                    logger.warning(f"Failed to submit feedback: {response.status}")
            except grpc.RpcError as e:
                logger.error(f"gRPC error during feedback submission: {e.code()} - {e.details()}")

    try:
        response = stub.Get(
            lms_pb2.GetRequest(
                token=session['token'],
                assignments=lms_pb2.GetAssignmentsRequest(student_id="student_id_placeholder")  # Replace with actual student_id
            )
        )
        assignments = [
            {
                'assignment_id': item.assignment_id,
                'filename': item.filename,
                'data': item.data.encode('utf-8') if isinstance(item.data, str) else item.data
            }
            for item in response.items
        ]
        logger.info("Assignments retrieved successfully")
        return render_template('assignments.html', assignments=assignments, role=session['role'])
    except grpc.RpcError as e:
        logger.error(f"gRPC error on GET: {e.code()} - {e.details()}")
        return "Failed to retrieve assignments", 500

@app.route('/grades')
def grades():
    if 'token' not in session:
        logger.info("Access to grades denied - no valid session")
        return redirect(url_for('home'))
    try:
        response = stub.Get(
            lms_pb2.GetRequest(
                token=session['token'],
                grades=lms_pb2.GetGradesRequest(student_id="student_id_placeholder")  # Replace with actual student_id
            )
        )
        logger.info("Grades retrieved successfully")
        return render_template('grades.html', grades=response.items)
    except grpc.RpcError as e:
        logger.error(f"gRPC error during grade retrieval: {e.code()} - {e.details()}")
        return "Failed to retrieve grades", 500

@app.route('/feedback', methods=['GET', 'POST'])
def feedback():
    if 'token' not in session:
        logger.info("Access to feedback denied - no valid session")
        return redirect(url_for('home'))

    if request.method == 'POST':
        feedback_text = request.form['feedback']
        logger.info("Submitting feedback")
        try:
            response = stub.Post(
                lms_pb2.PostRequest(
                    token=session['token'],
                    feedback=lms_pb2.FeedbackData(
                        student_id="student_id_placeholder",  # Replace with actual student_id
                        assignment_id="assignment_id_placeholder",  # Replace with actual assignment_id if needed
                        feedback_text=feedback_text
                    )
                )
            )
            if response.status == "Success":
                logger.info("Feedback submitted successfully")
                return redirect(url_for('feedback'))
            else:
                logger.warning(f"Failed to submit feedback: {response.status}")
                return "Failed to submit feedback", 400
        except grpc.RpcError as e:
            logger.error(f"gRPC error during feedback submission: {e.code()} - {e.details()}")
            return "Failed to submit feedback", 500

    try:
        response = stub.Get(
            lms_pb2.GetRequest(
                token=session['token'],
                feedback=lms_pb2.GetFeedbackRequest(
                    student_id="student_id_placeholder",  # Replace with actual student_id
                    teacher_id="teacher_id_placeholder"  # Optional: Replace with actual teacher_id if needed
                )
            )
        )
        feedback_items = [
            {'assignment_id': item.assignment_id, 'feedback_text': item.feedback_text}
            for item in response.items
        ]
        logger.info("Feedback retrieved")
        return render_template('feedback.html', feedback=feedback_items)
    except grpc.RpcError as e:
        logger.error(f"gRPC error during feedback retrieval: {e.code()} - {e.details()}")
        return "Failed to retrieve feedback", 500

@app.route('/files/<filename>')
def download_file(filename):
    if 'token' not in session:
        logger.info("Access to file download denied - no valid session")
        return redirect(url_for('home'))

    file_path = os.path.join(app.config['FILE_STORAGE_DIR'], filename)
    if os.path.exists(file_path):
        logger.info(f"Serving file: {filename}")
        return send_file(file_path)
    else:
        logger.warning(f"File not found: {filename}")
        return "File not found", 404

@app.route('/logout')
def logout():
    token = session.pop('token', None)
    if token:
        logger.info(f"Logout request for token: {token}")
        try:
            session.clear()  # Clear all session data
            stub.Logout(lms_pb2.LogoutRequest(token=token))
            logger.info("Logout successful")
        except grpc.RpcError as e:
            logger.error(f"gRPC error during logout: {e.code()} - {e.details()}")
    return redirect(url_for('home'))

@app.template_filter('b64encode')
def b64encode_filter(data):
    if not isinstance(data, bytes):
        data = str(data).encode('utf-8')  # Convert to bytes if it's a string
    return base64.b64encode(data).decode('utf-8')

if __name__ == '__main__':
    logger.info("Starting LMS Client")
    app.run(host='0.0.0.0', port=5000)
