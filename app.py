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
from io import BytesIO, StringIO
from datetime import datetime, timedelta
import dateutil.parser
from zoneinfo import ZoneInfo
import traceback
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import csv
import re
import logging
import statistics

app = Flask(__name__, static_folder='static', static_url_path='/static')

# Enable CORS for all routes
CORS(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

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

IST = ZoneInfo('Asia/Kolkata')

def get_rain_status(value):
    if not value or value == "null" or isinstance(value, float) and value != value:  # Check for None, 'null', or NaN
        return "No Rain"
    try:
        value = int(float(value))  # Convert to float first to handle string numbers, then to int
        if value < 1500:
            return "Heavy Rain"
        elif value < 3000:
            return "Light Rain"
        return "No Rain"
    except (ValueError, TypeError):
        return "No Rain"

def fetch_historical_24h_data(date=None):
    """Fetch raw historical data for the specified day from 12:00 AM to 11:59 PM IST"""
    logger.info(f"Fetching raw historical data for date: {date}")
    
    if date:
        try:
            # Set start time to 12:00 AM and end time to 11:59 PM of the specified date in IST
            start_local = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
            end_local = datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
            start_utc = start_local.astimezone(ZoneInfo('UTC')).isoformat()[:-6] + 'Z'
            end_utc = end_local.astimezone(ZoneInfo('UTC')).isoformat()[:-6] + 'Z'
        except ValueError:
            logger.error(f"Invalid date format: {date}")
            return []
    else:
        # For current day, use from 12:00 AM to current time in IST
        end_local = datetime.now(IST)
        start_local = end_local.replace(hour=0, minute=0, second=0, microsecond=0)
        start_utc = start_local.astimezone(ZoneInfo('UTC')).isoformat()[:-6] + 'Z'
        end_utc = end_local.astimezone(ZoneInfo('UTC')).isoformat()[:-6] + 'Z'
    
    # Query raw sensor data without aggregation
    query = f"""
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: {start_utc}, stop: {end_utc})
          |> filter(fn: (r) => r._measurement == "sensor_data" and r.location == "field")
          |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity" or r._field == "soil_moisture" or r._field == "wind_speed" or r._field == "rain_intensity")
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    
    url = f"{INFLUXDB_URL}/api/v2/query?org={INFLUXDB_ORG}"
    
    try:
        response = requests.post(
            url,
            headers={
                "Authorization": f"Token {INFLUXDB_TOKEN}",
                "Content-Type": "application/vnd.flux",
                "Accept": "application/csv",
            },
            data=query,
        )

        if not response.ok:
            logger.error(f"InfluxDB historical request failed: Status {response.status_code} - {response.text}")
            return []

        text = response.text
        if not text.strip():
            logger.warning("No historical data returned from InfluxDB")
            return []

        # Parse CSV data using DictReader
        historical_data = []
        wind_speeds_with_time = []
        csv_reader = csv.DictReader(StringIO(text), skipinitialspace=True)
        
        for row in csv_reader:
            data_point = {}
            try:
                # Parse mandatory fields
                if '_time' in row and row['_time']:
                    data_point['_time'] = row['_time'].strip()
                
                # Parse numeric fields
                for field in ['temperature', 'humidity', 'soil_moisture', 'wind_speed', 'rain_intensity']:
                    if field in row and row[field] and row[field].strip() and row[field] != 'null':
                        try:
                            data_point[field] = float(row[field].strip())
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid {field} value: {row.get(field)} at {row.get('_time')}")
                            continue
                
                # Parse motion_detected if present
                if 'motion_detected' in row and row['motion_detected']:
                    data_point['motion_detected'] = row['motion_detected'].strip()
                
                if data_point and '_time' in data_point:
                    historical_data.append(data_point)
                    if 'wind_speed' in data_point:
                        wind_speeds_with_time.append((data_point['wind_speed'], data_point['_time']))
            
            except Exception as e:
                logger.warning(f"Error parsing row at {row.get('_time')}: {str(e)}")
                continue
        
        # Log top 5 wind speeds for debugging
        wind_speeds_with_time.sort(reverse=True)
        top_winds = wind_speeds_with_time[:5]
        logger.info(f"Top 5 wind speeds: {[(speed, time) for speed, time in top_winds]}")
        if wind_speeds_with_time:
            max_wind = max(wind_speeds_with_time, key=lambda x: x[0])[0]
            max_time = next(t for s, t in wind_speeds_with_time if s == max_wind)
            logger.info(f"Max wind speed: {max_wind} m/s at {max_time} UTC")
        
        logger.info(f"Successfully fetched {len(historical_data)} raw historical data points")
        return historical_data
        
    except Exception as e:
        logger.error(f"Error fetching raw historical data: {str(e)}")
        return []

def analyze_historical_trends(historical_data):
    """Analyze daily trends and patterns with modified wind and rainfall metrics"""
    if not historical_data:
        return "No historical data available for trend analysis."
    
    try:
        # Extract values for analysis
        temps = [d["temperature"] for d in historical_data if "temperature" in d and d["temperature"] is not None]
        humidities = [d["humidity"] for d in historical_data if "humidity" in d and d["humidity"] is not None]
        soil_moistures = [d["soil_moisture"] for d in historical_data if "soil_moisture" in d and d["soil_moisture"] is not None]
        wind_speeds = [d["wind_speed"] for d in historical_data if "wind_speed" in d and d["wind_speed"] is not None]
        rain_intensities = [(d["rain_intensity"], d["_time"]) for d in historical_data if "rain_intensity" in d and d["rain_intensity"] is not None]
        
        trends = []
        
        # Temperature trends
        if len(temps) >= 2:
            temp_trend = "increasing" if temps[-1] > temps[0] else "decreasing" if temps[-1] < temps[0] else "stable"
            avg_temp = statistics.mean(temps)
            max_temp = max(temps)
            min_temp = min(temps)
            trends.append(f"Temperature: {temp_trend} trend, avg {avg_temp:.1f}°C, range {min_temp:.1f}-{max_temp:.1f}°C")
        
        # Humidity trends (avg, min, max)
        if len(humidities) >= 2:
            avg_humidity = statistics.mean(humidities)
            min_humidity = min(humidities)
            max_humidity = max(humidities)
            trends.append(f"Humidity: avg {avg_humidity:.1f}%, min {min_humidity:.1f}%, max {max_humidity:.1f}%")
        
        # Soil moisture trends
        if len(soil_moistures) >= 2:
            soil_trend = "increasing" if soil_moistures[-1] > soil_moistures[0] else "decreasing" if soil_moistures[-1] < soil_moistures[0] else "stable"
            avg_soil = statistics.mean(soil_moistures)
            trends.append(f"Soil moisture: {soil_trend} trend, avg {avg_soil:.1f}%")
        
        # Wind patterns (avg and max with timestamp in IST)
        if len(wind_speeds) >= 2:
            avg_wind = statistics.mean(wind_speeds)
            max_wind = max(wind_speeds)
            max_wind_time = next(d['_time'] for d in historical_data if d.get('wind_speed') == max_wind)
            max_wind_dt = datetime.fromisoformat(max_wind_time.replace('Z', '+00:00')).astimezone(IST)
            max_wind_time_ist = max_wind_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
            trends.append(f"Wind: avg {avg_wind:.1f} m/s, max {max_wind:.1f} m/s at {max_wind_time_ist}")
        
        # Rain patterns (Yes/No with time of first rain event in IST)
        if rain_intensities:
            rain_events = [(get_rain_status(r), t) for r, t in rain_intensities]
            rain_detected = any(status in ["Heavy Rain", "Light Rain"] for status, _ in rain_events)
            if rain_detected:
                # Find the first rain event
                first_rain = next((t for status, t in rain_events if status in ["Heavy Rain", "Light Rain"]), None)
                if first_rain:
                    rain_time_dt = datetime.fromisoformat(first_rain.replace('Z', '+00:00')).astimezone(IST)
                    rain_time_ist = rain_time_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
                    trends.append(f"Rainfall: Yes at {rain_time_ist}")
                    logger.info(f"Rain detected at {rain_time_ist}")
                else:
                    trends.append("Rainfall: Yes")
                    logger.info("Rain detected, but no valid timestamp found")
            else:
                trends.append("Rainfall: No")
                logger.info("No rain detected")
        else:
            trends.append("Rainfall: No")
            logger.info("No rain intensity data available")
        
        return " | ".join(trends) if trends else "Limited historical data available for analysis."
        
    except Exception as e:
        logger.error(f"Error analyzing historical trends: {str(e)}")
        return "Error analyzing historical trends."

def unescape_influxdb(value):
    """Unescape InfluxDB-escaped strings by removing backslashes before spaces, commas, and equals signs."""
    if isinstance(value, str):
        # Replace escaped characters with their unescaped versions
        return value.replace('\\ ', ' ').replace('\\,', ',').replace('\\=', '=').replace('\\\\', '\\')
    return value

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
    """Save agronomist assessments to the Vimal_Task measurement with distinct fields"""
    try:
        data = request.json
        date = data.get('date')
        assessment_type = data.get('assessment_type')
        timestamp = data.get('timestamp')
        
        print(f"Received agronomist assessment: date={date}, assessment_type={assessment_type}")
        
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
        tag_parts.append(f"question_id=agronomist_daily")  # Unique question_id to avoid overlap

        fields = []
        if assessment_type == 'average' and data.get('improvement_notes'):
            improvement_notes = data.get('improvement_notes').strip()
            if improvement_notes:
                fields.append(f'improvement_notes="{escape_field(improvement_notes)}"')
        
        elif assessment_type == 'uncertain' and data.get('uncertainty_notes'):
            uncertainty_notes = data.get('uncertainty_notes').strip()
            if uncertainty_notes:
                fields.append(f'uncertainty_notes="{escape_field(uncertainty_notes)}"')
        
        # Add photo analysis if provided
        if data.get('photo_analysis'):
            photo_analysis = data.get('photo_analysis').strip()
            if photo_analysis:
                fields.append(f'photo_analysis="{escape_field(photo_analysis)}"')

        # Add agronomist identifier (replace with actual ID when authentication is implemented)
        fields.append(f'agronomist="system"')  # Placeholder for agronomist ID
        
        if not fields:
            print("No valid fields provided for agronomist assessment")
            return jsonify({'error': 'No valid fields provided for assessment'}), 400

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
                |> filter(fn: (r) => r["question_id"] == "agronomist_daily")
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

@app.route('/get_data', methods=['POST'])
def get_data():
    try:
        data = request.json
        question_type = data.get('question_type', '')
        date_filter = data.get('date', '')

        logger.info(f"Received request: question_type={question_type}, date_filter={date_filter}")

        # Validate date format if provided
        if date_filter:
            try:
                datetime.strptime(date_filter, '%Y-%m-%d')
                start_time = f"{date_filter}T00:00:00Z"
                stop_time = f"{date_filter}T23:59:59Z"
            except ValueError:
                logger.error(f"Invalid date format: {date_filter}")
                return jsonify({'error': 'Invalid date format, expected YYYY-MM-DD'}), 400
        else:
            start_time = '-30d'
            stop_time = 'now()'

        # Construct the Flux query with fallback for missing question_id
        query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: {start_time}, stop: {stop_time})
                |> filter(fn: (r) => r._measurement == "Vimal_Task")
                |> map(fn: (r) => ({{r with question_id: if exists r.question_id then r.question_id else "unknown"}}))
                |> pivot(rowKey: ["_time", "question_id"], columnKey: ["_field"], valueColumn: "_value")
        '''
        if question_type:
            escaped_type = question_type.replace('"', '\\"').replace(' ', '_').replace('&', '_')
            query += f'|> filter(fn: (r) => r.type == "{escaped_type}" or r.type == "image" or r.type == "agronomist_assessment")'
        if date_filter:
            query += f'|> filter(fn: (r) => r.date == "{date_filter}")'

        # Debug: Check for records without date filter to diagnose missing data
        if date_filter:
            debug_query = f'''
                from(bucket: "{INFLUXDB_BUCKET}")
                    |> range(start: {start_time}, stop: {stop_time})
                    |> filter(fn: (r) => r._measurement == "Vimal_Task")
                    |> limit(n: 10)
            '''
            debug_tables = query_api.query(query=debug_query, org=INFLUXDB_ORG)
            logger.info(f"Debug query for {date_filter} returned {len(debug_tables)} tables")

        logger.info(f"Executing Flux query: {query}")
        tables = query_api.query(query, org=INFLUXDB_ORG)

        logger.info(f"Total tables from query: {len(tables)}")
        results = []
        image_urls = {}

        # Define mandatory fields
        mandatory_fields = ['date', 'type', 'question_id', '_time']

        for table in tables:
            for record in table.records:
                logger.debug(f"Processing record: time={record.get_time().isoformat()}, type={record.values.get('type')}, date={record.values.get('date')}, question_id={record.values.get('question_id')}")

                # Check for mandatory fields
                missing_fields = [field for field in mandatory_fields if field not in record.values or record.values[field] is None]
                if missing_fields:
                    logger.warning(f"Skipping record due to missing mandatory fields: {missing_fields}, record={record.values}")
                    continue

                date = record.values.get('date', '')
                question_id = record.values.get('question_id', 'unknown')
                record_type = record.values.get('type', '')

                # Handle image records
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
                        logger.debug(f"Stored image: key={key}, url={image_url}")
                    continue

                # Initialize result dictionary with mandatory fields
                result = {
                    'date': date,
                    'type': record_type.replace('_', ' ').replace(' and ', ' & '),
                    'question_id': question_id,
                    'timestamp': record.get_time().isoformat(),
                    'question': unescape_influxdb(record.values.get('question', '')),
                    'answer': unescape_influxdb(record.values.get('answer', '')),
                    'followup_text': unescape_influxdb(record.values.get('followup_text', '')),
                    'photos': [],
                    'all_fields': {}
                }

                # Handle agronomist assessments
                if record_type == 'agronomist_assessment':
                    result.update({
                        'assessment_type': unescape_influxdb(record.values.get('assessment_type', '')),
                        'improvement_notes': unescape_influxdb(record.values.get('improvement_notes', '')),
                        'uncertainty_notes': unescape_influxdb(record.values.get('uncertainty_notes', '')),
                        'photo_analysis': unescape_influxdb(record.values.get('photo_analysis', '')),
                        'agronomist': unescape_influxdb(record.values.get('agronomist', ''))
                    })

                # Handle photos
                photos = []
                photos_str = record.values.get('photos', '[]')
                if photos_str and isinstance(photos_str, str):
                    try:
                        # Remove extra backslashes and attempt JSON parsing
                        photos_str = photos_str.replace('\\"', '"').replace('\\ ', ' ')
                        photos = json.loads(photos_str)
                        logger.debug(f"Parsed photos for question_id {question_id}: {photos}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding photos JSON for question_id {question_id}: {photos_str}, error: {str(e)}")
                        # Attempt to recover URLs
                        urls = re.findall(r'https?://[^\s"]+', photos_str)
                        photos = [{'url': url} for url in urls]
                        logger.debug(f"Recovered photos for question_id {question_id}: {photos}")
                else:
                    logger.warning(f"Invalid or missing photos field for question_id {question_id}: {photos_str}")
                    photos = []

                # Append images from image_urls
                image_key = f"{date}_{question_id}"
                if image_key in image_urls:
                    photos.extend(image_urls[image_key])
                    logger.debug(f"Appended {len(image_urls[image_key])} images to photos for {image_key}")

                # Ensure photos is a list of dicts with 'url' and 'name'
                result['photos'] = [
                    {'url': photo['url'], 'name': photo.get('name', f"image_{question_id}_{i}")}
                    for i, photo in enumerate(photos) if isinstance(photo, dict) and 'url' in photo
                ]

                # Include all fields dynamically, excluding internal InfluxDB fields
                result['all_fields'] = {
                    k: v.isoformat() if isinstance(v, datetime) else unescape_influxdb(v)
                    for k, v in record.values.items()
                    if k not in ['_measurement', '_start', '_stop', 'result', 'table']
                }

                results.append(result)
                logger.debug(f"Added record: question_id={question_id}, photos_count={len(result['photos'])}")

        # Fetch weather summary for the specified date
        weather_summary = None
        if date_filter:
            historical_data = fetch_historical_24h_data(date_filter)
            weather_summary = analyze_historical_trends(historical_data)

        logger.info(f"Retrieved {len(results)} records with {sum(len(r['photos']) for r in results)} total photos")
        return jsonify({
            'responses': results,
            'weather_summary': weather_summary,
            'message': f"Retrieved {len(results)} records for date {date_filter}" if date_filter else f"Retrieved {len(results)} records"
        }), 200

    except Exception as e:
        logger.error(f"Error querying InfluxDB: {str(e)}")
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