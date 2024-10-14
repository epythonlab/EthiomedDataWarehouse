import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from logger import Logger

# Initialize the logger
logger = Logger(log_file='../data/database.log')

# Define the class labels
class_labels = [
    'cosmotic',
    'food-package',
    'supliment',
    'formula-milk',
    'lotion-moisturizer',
    'medicine',
    'mini-drop',
    'pregnancy',
]

def connect_to_db():
    try:
        # Load environment variables
        load_dotenv()
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        logger.info("Connection to the database established successfully.")
        print("Connection to the database established successfully.")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        print(f"Error connecting to database: {e}")
        raise

def store_detection_results(engine, detection_folder):
    try:
        # Define the metadata
        metadata = MetaData()
        detections = Table('detections', metadata,
                           Column('id', Integer, primary_key=True),
                           Column('filename', String),
                           Column('x_min', Float),
                           Column('y_min', Float),
                           Column('x_max', Float),
                           Column('y_max', Float),
                           Column('confidence', Float),
                           Column('class_label', String),
                           Column('class_string_label', String)
                           )
        
        # Create the table if it doesn't exist
        metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Iterate through detection results
        for filename in os.listdir(detection_folder):
            if filename.endswith('.txt'):
                filepath = os.path.join(detection_folder, filename)
                with open(filepath, 'r') as file:
                    lines = file.readlines()
                    for line in lines:
                        parts = line.strip().split()
                        if len(parts) == 6:  # Now expecting 6 parts: class_id, center_x, center_y, width, height, confidence
                            try:
                                class_id = int(parts[0])  # Class ID
                                center_x = float(parts[1])
                                center_y = float(parts[2])
                                width = float(parts[3])
                                height = float(parts[4])
                                confidence = float(parts[5])  # Get confidence score

                                # Convert center coordinates and dimensions to bounding box
                                x_min = center_x - (width / 2)
                                y_min = center_y - (height / 2)
                                x_max = center_x + (width / 2)
                                y_max = center_y + (height / 2)

                                # Retrieve class string label based on class_id
                                class_string_label = class_labels[class_id] if 0 <= class_id < len(class_labels) else None
                                
                                detection_record = {
                                    'filename': filename.replace('.txt', '.jpg'),  # Convert .txt to .jpg
                                    'x_min': x_min,
                                    'y_min': y_min,
                                    'x_max': x_max,
                                    'y_max': y_max,
                                    'confidence': confidence,
                                    'class_label': str(class_id),  # Store class ID as a string
                                    'class_string_label': class_string_label
                                }
                                # Insert into the database
                                session.execute(detections.insert().values(detection_record))
                            except ValueError as ve:
                                logger.warning(f"ValueError in {filename}: {ve} - Line: {line.strip()}")
                                print(f"ValueError in {filename}: {ve} - Line: {line.strip()}")
                        else:
                            logger.warning(f"Skipping line in {filename}: unexpected format. Found {len(parts)} parts.")
                            print(f"Skipping line in {filename}: unexpected format. Found {len(parts)} parts.")

        # Commit the transaction
        session.commit()
        logger.info("Detection results stored successfully.")
        print("Detection results stored successfully.")
    except Exception as e:
        logger.error(f"Error storing detection results: {e}")
        print(f"Error storing detection results: {e}")
    finally:
        session.close()

# Call the functions
if __name__ == "__main__":
    engine = connect_to_db()
    detection_results_path = '../yolov5/runs/detect/predictions9/labels'  # Path to your labels
    store_detection_results(engine, detection_results_path)
