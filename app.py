from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteApi, SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
import os
from dotenv import load_dotenv
import json
import cloudinary
import cloudinary.uploader
import base64
from io import BytesIO
from datetime import datetime
import dateutil.parser
from zoneinfo import ZoneInfo
import traceback
import requests
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder='static', static_url_path='/static')

# Enable CORS for all routes
CORS(app)

# Load environment variables
load_dotenv()

# InfluxDB configuration
INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'https://us-east-1-1.aws.cloud2.influxdata.com')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', 'nZ49M1MTGbHtRCrc2OJhx-kVIBWuwvereT-o1mcq2COz3urUNuUuIIMjysObK8oOEHn8352w7LKFyrX8PQpdsA==')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'Agri')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'smart_agri')

# Cloudinary configuration
cloudinary.config(
    cloud_name=os.getenv('CLOUDINARY_CLOUD_NAME', 'dnjlsegrq'),
    api_key=os.getenv('CLOUDINARY_API_KEY', '315166364872797'),
    api_secret=os.getenv('CLOUDINARY_API_SECRET', 'xIrcgfB7euQCW-FKi0kd6nWur24'),
    secure=True
)
CLOUDINARY_UPLOAD_PRESET = os.getenv('CLOUDINARY_UPLOAD_PRESET', 'smart_agri_preset')

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_client.query_api()

# Get the Render app URL
RENDER_APP_URL = os.getenv('RENDER_EXTERNAL_URL', 'https://vimal-farm.onrender.com')

# APScheduler setup for keep-alive pings
def ping_self():
    try:
        response = requests.get(f"{RENDER_APP_URL}/healthz", timeout=5)
        if response.status_code == 200:
            print(f"Self-ping successful at {datetime.now().isoformat()}: {response.json()}")
        else:
            print(f"Self-ping failed at {datetime.now().isoformat()}: Status {response.status_code}")
    except Exception as e:
        print(f"Self-ping error at {datetime.now().isoformat()}: {str(e)}")

scheduler = BackgroundScheduler()
scheduler.add_job(ping_self, 'interval', minutes=5)
scheduler.start()

# Expected English questions for validation
EXPECTED_QUESTIONS = {
    'Day 1 - Watering & Health': [
        'Did you water the plants today?',
        'Did it rain today on your field?',
        'Did you spray pesticide or fungicide?',
        'Did you remove weeds today?',
        'Is the plant healthy today (your view)?',
        'Any unusual weather (wind, hail, storm, excess heat)?'
    ],
    'Day 2 - Nutrients & Operations': [
        'Did you apply fertilizer today?',
        'Did you notice any pests or disease symptoms?',
        'Are the leaves showing any issues (spots, yellowing, curling)?',
        'Did you or any labor work in the field today?',
        'Did you face any irrigation or electricity issues?',
        'Did you complete the planned task for today?',
        'Any other field observation or issue today?'
    ],
    'Weekly Review': [
        'What stage is the crop in now?',
        'Is the crop growing as expected?',
        'Is your expected harvest yield still realistic?',
        'Have you planned for harvest storage or sale?',
        'Did any crop support (fence, net, stakes) need fixing this week?',
        'Did you consult anyone for crop advice?',
        'Do you want expert help or callback from our team?',
        'Any wildlife/cattle/animal damage this week?'
    ]
}

# Custom 404 handler
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.route('/')
def serve_index():
    return app.send_static_file('index.html')

@app.route('/static/<path:filename>')
def serve_static(filename):
    return app.send_static_file(filename)

@app.route('/view_data')
def serve_data_view():
    return app.send_static_file('data_viewer.html')

@app.route('/agronomist')
def serve_agronomist():
    """Serve the agronomist assessment page"""
    return send_from_directory('static', 'agronomist.html')

@app.route('/ping', methods=['GET'])
def ping():
    print(f"Ping received at {datetime.now().isoformat()} - Instance is alive!")
    return jsonify({
        'status': 'alive',
        'timestamp': datetime.now().isoformat(),
        'message': 'Farm Tracker API is running'
    }), 200

@app.route('/healthz', methods=['GET'])
def healthz():
    print(f"Health check received at {datetime.now().isoformat()}")
    return jsonify({'status': 'healthy'}), 200

@app.route('/upload_image', methods=['POST'])
def upload_image():
    try:
        data = request.json
        image_data = data.get('image')
        question_id = data.get('question_id')
        timestamp = data.get('timestamp')
        date = data.get('date')  # Added to match index.html expectation

        if not image_data or not question_id or not date:
            return jsonify({'error': 'Missing image, question_id, or date'}), 400

        if ',' in image_data:
            image_data = image_data.split(',')[1]
        else:
            return jsonify({'error': 'Invalid base64 image data'}), 400

        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format, expected YYYY-MM-DD'}), 400

        safe_timestamp = timestamp.replace(':', '-').replace('.', '-')
        public_id = f"smart_agri/{question_id}_{safe_timestamp}"

        try:
            result = cloudinary.uploader.upload(
                f"data:image/jpeg;base64,{image_data}",
                upload_preset=CLOUDINARY_UPLOAD_PRESET,
                public_id=public_id,
                folder="smart_agri"
            )
            image_url = result['secure_url']
            print(f"Image uploaded successfully: question_id={question_id}, date={date}, url={image_url}")
        except Exception as e:
            print(f"Cloudinary upload error: {str(e)}")
            return jsonify({'error': f'Failed to upload to Cloudinary: {str(e)}'}), 500

        point = Point("Vimal_Task") \
            .tag("question_id", question_id) \
            .tag("type", "image") \
            .tag("date", date) \
            .field("image_url", image_url) \
            .time(timestamp, WritePrecision.NS)

        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            print(f"Successfully wrote image record: question_id={question_id}, date={date}, url={image_url}")
        except Exception as e:
            print(f"InfluxDB write error: {str(e)}")
            return jsonify({'error': f'Failed to write to InfluxDB: {str(e)}'}), 500

        return jsonify({'image_url': image_url}), 200
    except Exception as e:
        print(f"Server error in upload_image: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/save_responses', methods=['POST'])
def save_responses():
    try:
        server_date = datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y-%m-%d')
        print(f"Server date (IST): {server_date}")

        data = request.json
        date = data.get('date')
        question_type = data.get('type')
        language = data.get('language', 'hindi')
        responses = data.get('responses')
        timestamp = data.get('timestamp')

        if not responses:
            return jsonify({'error': 'No responses provided'}), 400
        if not question_type:
            print(f"Error: question_type is None or missing")
            return jsonify({'error': 'question_type is missing or invalid'}), 400

        received_questions = list(responses.keys())
        expected_questions = EXPECTED_QUESTIONS.get(question_type, [])
        if len(received_questions) > len(expected_questions):
            received_questions = received_questions[:len(expected_questions)]
        
        missing_questions = [q for q in received_questions if q not in expected_questions]
        if missing_questions:
            print(f"Warning: Some received questions don't match expected: {missing_questions}")

        print(f"Received responses: date={date}, type={question_type}, language={language}, timestamp={timestamp}")
        print(f"Number of responses: {len(responses)}")

        try:
            parsed_time = dateutil.parser.isoparse(timestamp)
            timestamp_ns = int(parsed_time.timestamp() * 1_000_000_000)
        except ValueError as e:
            print(f"Invalid timestamp format: {timestamp}, error: {str(e)}")
            return jsonify({'error': f'Invalid timestamp format: {timestamp}'}), 400

        lines = []
        valid_responses = 0
        
        for index, (question, response) in enumerate(responses.items()):
            if question not in expected_questions:
                print(f"Skipping invalid question: {question}")
                continue
                
            answer = str(response.get('answer', ''))
            followup_text = str(response.get('followup_text', ''))  # Changed to 'followup_text' to match index.html
            photos_list = response.get('photos', [])
            
            if not answer and not followup_text and not photos_list:
                print(f"Skipping question {index + 1}: No meaningful data")
                continue

            valid_responses += 1
            
            def escape_field(value):
                if isinstance(value, str):
                    # Only escape spaces, commas, and equals signs for InfluxDB line protocol
                    return value.replace('\\', '\\\\').replace(',', '\\,').replace('=', '\\=').replace(' ', '\\ ')
                return str(value)
            
            def escape_tag(value):
                if isinstance(value, str):
                    return value.replace('\\', '\\\\').replace(' ', '\\ ').replace(',', '\\,').replace('=', '\\=')
                return str(value)
            
            escaped_question = escape_field(question)
            escaped_answer = escape_field(answer)
            escaped_followup_text = escape_field(followup_text)
            
            photos_urls = [photo.get('url', '') for photo in photos_list if photo.get('url')]
            # Store photos as a JSON string without escaping quotes
            escaped_photos = json.dumps([{'url': url} for url in photos_urls]) if photos_urls else '[]'

            fields = []
            if escaped_answer:
                fields.append(f'answer="{escaped_answer}"')
            if escaped_followup_text:
                fields.append(f'followup_text="{escaped_followup_text}"')
            if photos_urls:
                fields.append(f'photos={escaped_photos}')  # No additional escaping for JSON string
            fields.append(f'question="{escaped_question}"')

            tag_parts = []
            tag_parts.append(f"date={escape_tag(date)}")
            tag_parts.append(f"type={escape_tag(question_type.replace(' ', '_').replace('&', '_'))}")
            tag_parts.append(f"language={escape_tag(language)}")
            tag_parts.append(f"question_id=q{index + 1}")

            line = f'Vimal_Task,{",".join(tag_parts)} {",".join(fields)} {timestamp_ns}'
            lines.append(line)
            print(f"Generated line for q{index + 1}: {line}")

        if not lines:
            print("No valid data points to write to InfluxDB")
            return jsonify({'error': 'No valid responses to save'}), 400

        print(f"Prepared {len(lines)} valid data points for InfluxDB")

        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=lines)
            print(f"Successfully wrote {len(lines)} records to InfluxDB bucket '{INFLUXDB_BUCKET}'")

            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -1m)
                |> filter(fn: (r) => r["_measurement"] == "Vimal_Task")
                |> filter(fn: (r) => r["date"] == "{date}")
                |> limit(n: {len(lines)})
            '''
            tables = query_api.query(query=query, org=INFLUXDB_ORG)
            if not tables:
                print("Verification failed: No records found after write")
                rejection_query = f'''
                from(bucket: "_monitoring")
                    |> range(start: -1h)
                    |> filter(fn: (r) => r["_measurement"] == "rejected_points")
                    |> filter(fn: (r) => r["bucket"] == "{INFLUXDB_BUCKET}")
                    |> limit(n: 10)
                '''
                rejections = query_api.query(query=rejection_query, org=INFLUXDB_ORG)
                if rejections:
                    errors = [record["_value"] for table in rejections for record in table.records]
                    print(f"Rejections found: {errors}")
                    return jsonify({'error': f'Write rejected: {errors}'}), 500
                return jsonify({'error': 'Write succeeded but data not found'}), 500

            print(f"Verified {len(tables)} tables written")
            return jsonify({
                'message': f'Responses saved successfully ({len(lines)} records)',
                'records_written': len(lines)
            }), 200
        except Exception as e:
            print(f"InfluxDB write error: {str(e)}")
            traceback.print_exc()
            return jsonify({'error': f'Failed to write to InfluxDB: {str(e)}'}), 500
            
    except Exception as e:
        print(f"Server error in save_responses: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/save_agronomist_assessment', methods=['POST'])
def save_agronomist_assessment():
    """Save agronomist assessments to the same Vimal_Task measurement"""
    try:
        data = request.json
        date = data.get('date')
        assessment_type = data.get('assessment_type')
        timestamp = data.get('timestamp')
        
        print(f"Received agronomist assessment: date={date}, type={assessment_type}")
        
        if not date or not assessment_type:
            return jsonify({'error': 'Missing required fields: date and assessment_type'}), 400

        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format, expected YYYY-MM-DD'}), 400

        try:
            parsed_time = dateutil.parser.isoparse(timestamp)
            timestamp_ns = int(parsed_time.timestamp() * 1_000_000_000)
        except ValueError as e:
            print(f"Invalid timestamp format: {timestamp}, error: {str(e)}")
            return jsonify({'error': f'Invalid timestamp format: {timestamp}'}), 400

        # Escape function for InfluxDB line protocol
        def escape_field(value):
            if isinstance(value, str):
                return value.replace('\\', '\\\\').replace(',', '\\,').replace('=', '\\=').replace(' ', '\\ ').replace('"', '\\"')
            return str(value)
        
        def escape_tag(value):
            if isinstance(value, str):
                return value.replace('\\', '\\\\').replace(' ', '\\ ').replace(',', '\\,').replace('=', '\\=')
            return str(value)

        # Build the line protocol entry
        tag_parts = []
        tag_parts.append(f"date={escape_tag(date)}")
        tag_parts.append(f"type=agronomist_assessment")
        tag_parts.append(f"assessment_type={escape_tag(assessment_type)}")
        tag_parts.append(f"question_id=agronomist_daily")

        fields = []
        fields.append(f'assessment="{escape_field(assessment_type)}"')
        
        # Add specific notes based on assessment type
        if assessment_type == 'average' and data.get('improvement_notes'):
            improvement_notes = data.get('improvement_notes').strip()
            if improvement_notes:
                fields.append(f'improvement_notes="{escape_field(improvement_notes)}"')
                fields.append(f'question="What needs to be improved?"')
        
        elif assessment_type == 'uncertain' and data.get('uncertainty_notes'):
            uncertainty_notes = data.get('uncertainty_notes').strip()
            if uncertainty_notes:
                fields.append(f'uncertainty_notes="{escape_field(uncertainty_notes)}"')
                fields.append(f'question="Why are you uncertain and what is your view to improve it?"')
        
        elif assessment_type == 'all-good':
            fields.append(f'question="Overall assessment"')
        
        # Add photo analysis if provided
        if data.get('photo_analysis'):
            photo_analysis = data.get('photo_analysis').strip()
            if photo_analysis:
                fields.append(f'photo_analysis="{escape_field(photo_analysis)}"')

        # Add agronomist identifier (you can modify this based on your authentication system)
        fields.append(f'agronomist="system"')  # Replace with actual agronomist ID when you have authentication
        
        line = f'Vimal_Task,{",".join(tag_parts)} {",".join(fields)} {timestamp_ns}'
        print(f"Generated agronomist assessment line: {line}")

        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=line)
            print(f"Successfully wrote agronomist assessment to InfluxDB")
            
            # Verify the write
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -1m)
                |> filter(fn: (r) => r["_measurement"] == "Vimal_Task")
                |> filter(fn: (r) => r["type"] == "agronomist_assessment")
                |> filter(fn: (r) => r["date"] == "{date}")
                |> limit(n: 1)
            '''
            tables = query_api.query(query=query, org=INFLUXDB_ORG)
            if not tables:
                print("Verification failed: Agronomist assessment not found after write")
                return jsonify({'error': 'Assessment saved but verification failed'}), 500
            
            print(f"Verified agronomist assessment written successfully")
            return jsonify({
                'message': 'Agronomist assessment saved successfully',
                'assessment_type': assessment_type,
                'date': date
            }), 200
            
        except Exception as e:
            print(f"InfluxDB write error for agronomist assessment: {str(e)}")
            traceback.print_exc()
            return jsonify({'error': f'Failed to write agronomist assessment to InfluxDB: {str(e)}'}), 500
            
    except Exception as e:
        print(f"Server error in save_agronomist_assessment: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'}), 500

def unescape_influxdb(value):
    """Unescape InfluxDB-escaped strings by removing backslashes before spaces, commas, and equals signs."""
    if isinstance(value, str):
        # Replace escaped characters with their unescaped versions
        return value.replace('\\ ', ' ').replace('\\,', ',').replace('\\=', '=').replace('\\\\', '\\')
    return value

@app.route('/get_data', methods=['POST'])
def get_data():
    try:
        data = request.json
        question_type = data.get('question_type', '')
        date_filter = data.get('date', '')

        print(f"Received request: question_type={question_type}, date_filter={date_filter}")

        if date_filter:
            try:
                datetime.strptime(date_filter, '%Y-%m-%d')
                start_time = f"{date_filter}T00:00:00Z"
                stop_time = f"{date_filter}T23:59:59Z"
            except ValueError:
                return jsonify({'error': 'Invalid date format, expected YYYY-MM-DD'}), 400
        else:
            start_time = '-30d'
            stop_time = 'now()'

        query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: {start_time}, stop: {stop_time})
                |> filter(fn: (r) => r._measurement == "Vimal_Task")
                |> pivot(rowKey: ["_time", "question_id"], columnKey: ["_field"], valueColumn: "_value")
        '''
        if question_type:
            escaped_type = question_type.replace('"', '\\"').replace(' ', '_').replace('&', '_')
            query += f'|> filter(fn: (r) => r.type == "{escaped_type}" or r.type == "image" or r.type == "agronomist_assessment")'
        if date_filter:
            query += f'|> filter(fn: (r) => r.date == "{date_filter}")'

        print(f"Executing Flux query: {query}")
        tables = query_api.query(query, org=INFLUXDB_ORG)

        print(f"Total tables from query: {len(tables)}")
        results = []
        image_urls = {}
        for table in tables:
            for record in table.records:
                print(f"Processing record: time={record.get_time().isoformat()}, type={record.values.get('type')}, date={record.values.get('date')}, question_id={record.values.get('question_id')}")

                date = record.values.get('date', '')
                question_id = record.values.get('question_id', '')
                record_type = record.values.get('type', '')

                if record_type == 'image':
                    image_url = record.values.get('image_url')
                    if image_url:
                        key = f"{date}_{question_id}"
                        if key not in image_urls:
                            image_urls[key] = []
                        image_urls[key].append({
                            'url': image_url,
                            'name': f"image_{question_id}_{record.get_time().isoformat()}"
                        })
                        print(f"Stored image: key={key}, url={image_url}")
                    continue

                # Handle agronomist assessments separately
                if record_type == 'agronomist_assessment':
                    assessment = record.values.get('assessment', '')
                    improvement_notes = record.values.get('improvement_notes', '')
                    uncertainty_notes = record.values.get('uncertainty_notes', '')
                    photo_analysis = record.values.get('photo_analysis', '')
                    question = record.values.get('question', 'Agronomist Assessment')
                    
                    result = {
                        'date': date,
                        'type': 'Agronomist Assessment',
                        'question': question,
                        'answer': assessment,
                        'followup_text': improvement_notes or uncertainty_notes,
                        'photo_analysis': photo_analysis,
                        'photos': [],
                        'timestamp': record.get_time().isoformat(),
                        'question_id': question_id,
                        'assessment_type': record.values.get('assessment_type', ''),
                        'all_fields': {k: v.isoformat() if isinstance(v, datetime) else v for k, v in record.values.items()}
                    }
                    results.append(result)
                    continue

                photos = []
                try:
                    photos_str = record.values.get('photos', '[]')
                    # Attempt to fix malformed JSON by removing extra backslashes
                    photos_str = photos_str.replace('\\"', '"')
                    photos = json.loads(photos_str) if photos_str else []
                    print(f"Parsed photos for question_id {question_id}: {photos}")
                except json.JSONDecodeError as e:
                    print(f"Error decoding photos JSON for question_id {question_id}: {photos_str}, error: {str(e)}")
                    # Try to recover by manually parsing the URL
                    if 'url' in photos_str:
                        try:
                            import re
                            urls = re.findall(r'https?://[^\s"]+', photos_str)
                            photos = [{'url': url} for url in urls]
                            print(f"Recovered photos for question_id {question_id}: {photos}")
                        except Exception as recover_e:
                            print(f"Failed to recover photos for question_id {question_id}: {str(recover_e)}")
                            photos = []

                image_key = f"{date}_{question_id}"
                if image_key in image_urls:
                    photos.extend(image_urls[image_key])
                    print(f"Appended {len(image_urls[image_key])} images to photos for {image_key}")

                # Ensure photos is always a list of dicts with 'url' and 'name'
                photos = [
                    {'url': photo['url'], 'name': photo.get('name', f"image_{question_id}_{i}")}
                    for i, photo in enumerate(photos)
                ]

                # Unescape fields before sending to frontend
                question = unescape_influxdb(record.values.get('question', ''))
                answer = unescape_influxdb(record.values.get('answer', ''))
                followup_text = unescape_influxdb(record.values.get('followup_text', ''))

                serializable_values = {
                    k: v.isoformat() if isinstance(v, datetime) else v
                    for k, v in record.values.items()
                }

                result = {
                    'date': date,
                    'type': record_type.replace('_', ' ').replace(' and ', ' & '),
                    'question': question,
                    'answer': answer,
                    'followup_text': followup_text,
                    'photos': photos,
                    'timestamp': record.get_time().isoformat(),
                    'question_id': question_id,
                    'all_fields': serializable_values
                }
                results.append(result)
                print(f"Added record: question_id={question_id}, photos_count={len(photos)}")

        print(f"Retrieved {len(results)} records with {sum(len(r['photos']) for r in results)} total photos")
        return jsonify(results), 200
    except Exception as e:
        print(f"Error querying InfluxDB: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': f'Failed to query InfluxDB: {str(e)}'}), 500

if __name__ == '__main__':
    print("Starting Farm Tracker API...")
    print(f"Serving static files from: {os.path.abspath('static')}")
    print("Available endpoints:")
    print("  / - Main farm data entry")
    print("  /view_data - Data viewer dashboard")
    print("  /agronomist - Agronomist assessment page")
    port = int(os.environ.get('PORT', 5000))
    try:
        app.run(host='0.0.0.0', port=port, debug=False)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()