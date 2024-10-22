from config import logger
from flask import Blueprint, request, redirect, url_for, render_template, session
from grpc_client import grpc_client
from routes.auth import check_session
import grpc
import lms_pb2

bp = Blueprint('forum', __name__)

@bp.route('/forum', methods=['GET', 'POST'])
def forum():
    if not check_session():
        return redirect(url_for('auth.home'))
    if request.method == 'POST':
        return handle_queries_post()
    return render_queries_get()

def handle_queries_post():
    if session['role'] == 'student':
        query_type = request.form['query_type']
        query_content = request.form['query']
        context_file_path = request.form['course_material']
        
        # Fetch teachers for error handling
        teachers = grpc_client.fetch_teachers_via_grpc()

        try:
            if query_type == 'teacher':
                selected_teacher = request.form.get('teacher')

                # Ensure a teacher is selected
                if not selected_teacher:
                    logger.warning("No teacher selected.")
                    return render_template('forum.html', error="Please select a teacher.", teachers=teachers)

                logger.info(f"Selected teacher: {selected_teacher}")

                # Post the query for a teacher
                response = grpc_client.stub.Post(lms_pb2.PostRequest(
                    token=session['token'],
                    query=lms_pb2.Query(
                        student_name=session['username'],
                        teacher_name=selected_teacher,
                        query_text=query_content,
                        context_file_path=context_file_path,
                        query_type='teacher',
                        status='open'
                    )
                ))

            else:
                # Post the query for LLM
                logger.info("Posting query to LLM")
                response = grpc_client.stub.Post(lms_pb2.PostRequest(
                    token=session['token'],
                    query=lms_pb2.Query(
                        student_name=session['username'],
                        query_text=query_content,
                        context_file_path=context_file_path,
                        query_type='llm',
                        status='open'
                    )
                ))

            # Check the response and handle accordingly
            if response.status != "success":
                logger.error(f"Failed to submit query: {response.status}")
                return render_template('forum.html', error="Failed to submit query.", teachers=teachers)

        except grpc.RpcError as e:
            logger.error(f"gRPC error during post operation: {e.details()}, code: {e.code()}")
            return grpc_client.handle_grpc_error(e)
        
    elif session['role'] == 'teacher':
        answers = {k: v for k, v in request.form.items() if k.startswith('answer_')}
        for query_id, answer_text in answers.items():
            query_id_cleaned = query_id[len('answer_'):]
            try:
                # Sending answer
                response = grpc_client.stub.Post(lms_pb2.PostRequest(
                    token=session['token'],
                    query=lms_pb2.Query(
                        query_id=query_id_cleaned,
                        answer_text=answer_text,
                        status='answered'
                    )
                ))
                
                # Check the response and handle accordingly
                if response.status != "success":
                    logger.error(f"Failed to update query: {response.status}")
                    return render_template('forum.html', error="Failed to update query.")

            except grpc.RpcError as e:
                return grpc_client.handle_grpc_error(e)

    # Redirect to forum after successful submission
    return redirect(url_for('forum.forum'))


def render_queries_get():
    try:
        role = session['role']
        username = session['username']
        
        if role == 'teacher':
            teachers = []  # No teachers list needed if the user is a teacher
            request_data = lms_pb2.Query()
            query_response = grpc_client.stub.Get(lms_pb2.GetRequest(
                token=session['token'],
                query_teacher=request_data
            ))
        elif role == 'student':
            teachers = grpc_client.fetch_teachers_via_grpc()  # Fetch teachers for the student to select from
            request_data = lms_pb2.Query()
            query_response = grpc_client.stub.Get(lms_pb2.GetRequest(
                token=session['token'],
                query_last=request_data
            ))
        else:
            return "Unknown role", 400

        queries = []
        for item in query_response.query_items:
            if role == 'teacher' and item.teacher_name == username:
                queries.append({
                    'query_id': item.query_id,
                    'content': item.query_text,
                    'date': item.date,
                    'teacher_name': item.teacher_name or 'LLM',
                    'answer_text': item.answer_text or 'Pending',
                    'context_file_path': item.context_file_path,
                    'status': item.status
                })
            elif role == 'student':
                queries.append({
                    'query_id': item.query_id,
                    'content': item.query_text,
                    'date': item.date,
                    'teacher_name': item.teacher_name or 'LLM',
                    'answer_text': item.answer_text or 'Pending',
                    'context_file_path': item.context_file_path,
                    'status': item.status
                })
        
        logger.info(f"Queries fetched: {queries}") 


        # Send the gRPC request to get course_materials
        request_data = lms_pb2.CourseMaterial()
        response = grpc_client.stub.Get(lms_pb2.GetRequest(
            token=session['token'],
            content=request_data
        ))

        course_materials = []
        
        # Loop through the response and filter based on the role
        for item in response.course_items:
            course_materials.append({
                'filename': item.filename,
                'file_path': item.file_path,
            })
            # logger.info(f"course_materials fetched: {course_materials}")
        # else:
        #     logger.warning("No course_materials found in the response.")


    except grpc.RpcError as e:
        return grpc_client.handle_grpc_error(e)

    return render_template('forum.html', queries=queries, teachers=teachers, course_materials=course_materials)

