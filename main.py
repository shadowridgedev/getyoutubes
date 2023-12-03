import os
import re
import moviepy.editor as mp
import speech_recognition as sr
from multiprocessing import Pool
import mysql.connector

# Function to sanitize filenames
def sanitize_filename(title):
    return re.sub(r'[\\/:*?"<>|\|\t\n\r]', "_", title)

# Function to extract audio from video (if it's a video file)
def extract_audio(video_path, download_path):
    if video_path.endswith((".mp4", ".avi", ".mkv")):
        print(f"Extracting audio from {video_path}")
        video_clip = mp.VideoFileClip(video_path)
        audio_clip = video_clip.audio
        audio_file_name = os.path.basename(video_path).replace('.mp4', '.wav')
        audio_full_path = os.path.join(download_path, audio_file_name)
        audio_clip.write_audiofile(audio_full_path)
        audio_clip.close()
        print("Audio extracted successfully.")
        return audio_full_path
    else:
        return None  # Not a video file, return None

# Function to transcribe audio
def transcribe_audio(segment):
    audio_path, start_time, duration = segment
    print(f"Transcribing audio from {start_time} to {start_time + duration} seconds...")
    recognizer = sr.Recognizer()
    with sr.AudioFile(audio_path) as source:
        audio_data = recognizer.record(source, duration=duration, offset=start_time)
        try:
            transcription = recognizer.recognize_google(audio_data)
            print(f"Transcribed segment: {transcription[:50]}...")
            return transcription
        except sr.UnknownValueError:
            print("Audio segment is inaudible.")
            return "[Inaudible]"
        except sr.RequestError as e:
            print(f"Error in speech recognition: {e}")
            return f"[Error: {e}]"

# Function to handle missing words in transcriptions
def handle_missing_words(full_transcriptions, three_second_transcriptions, overlap):
    corrected_transcripts = []
    for i in range(len(full_transcriptions) - 1):
        current_segment_words = full_transcriptions[i].split()
        next_segment_first_word = three_second_transcriptions[i].split()[0]
        if current_segment_words[-overlap:] != next_segment_first_word:
            if next_segment_first_word not in current_segment_words[-overlap:]:
                corrected_segment = " ".join(current_segment_words) + " " + next_segment_first_word
            else:
                corrected_segment = " ".join(current_segment_words)
        else:
            corrected_segment = " ".join(current_segment_words)
        corrected_transcripts.append(corrected_segment)
    corrected_transcripts.append(full_transcriptions[-1])  # Append the last segment without modification
    return corrected_transcripts

# Function to create a database and table
def create_database_and_table(db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS youtube")
    cursor.execute("USE youtube")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS YoutubeData (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_name TEXT,
            file_path TEXT,
            audio_path TEXT,
            transcript TEXT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

# Function to store data in database
def store_data(db_config, file_name, file_path, audio_path, transcript):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("USE youtube")
    cursor.execute("""
        INSERT INTO YoutubeData (file_name, file_path, audio_path, transcript)
        VALUES (%s, %s, %s, %s)
    """, (file_name, file_path, audio_path, transcript))
    conn.commit()
    cursor.close()
    conn.close()

# Function to process files in a directory
def process_files(directory):
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            audio_path = extract_audio(filepath, download_path)
            if audio_path:
                segment_duration = 60
                overlap = 3
                total_duration = mp.VideoFileClip(filepath).duration

                full_segments = [(audio_path, i * segment_duration, segment_duration + overlap) for i in
                                range(int(total_duration / segment_duration))]
                three_second_segments = [(audio_path, i * segment_duration, overlap) for i in
                                        range(1, int(total_duration / segment_duration))]

                with Pool() as pool:
                    full_transcriptions = pool.map(transcribe_audio, full_segments)
                    three_second_transcriptions = pool.map(transcribe_audio, three_second_segments)

                corrected_transcripts = handle_missing_words(full_transcriptions, three_second_transcriptions, overlap)

                full_transcript = "\n".join(corrected_transcripts)
                store_data(db_config, filename, filepath, audio_path, full_transcript)
            else:
                print(f"Skipping {filename} - Not a supported video format")


if __name__ == "__main__":
    download_path = "audio_files"  # Directory to store extracted audio files
    os.makedirs(download_path, exist_ok=True)

    directory_to_scan = "/path/to/your/files"  # Replace with the directory containing your files
    db_config = {
        'host': os.environ.get('host', 'localhost'),
        'port': int(os.environ.get('port', 3306)),
        'user': os.environ.get('user'),
        'password': os.environ.get('password', 'Rs232x25'),
        'database': os.environ.get('database', '')
    }

    create_database_and_table(db_config)
    process_files(directory_to_scan)

