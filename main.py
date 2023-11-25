import sys
import os
import re
import moviepy.editor as mp
import speechrecognition as sr
from pytube import YouTube
from multiprocessing import Pool
import mysql.connector


## Function to adjust file paths for different operating systems
def adjust_for_os(path):
    return path.replace('/', os.sep) if os.name == 'nt' else path


# Function to sanitize filenames
def sanitize_filename(title):
    return re.sub(r'[\\/:*?"<>|\|\t\n\r]', "_", title)


# Function to download videos from YouTube
def download_video(video_url, download_path):
    print(f"Downloading video from {video_url}...")
    yt = YouTube(video_url)
    video_title = sanitize_filename(yt.title)
    file_name = f"{video_title}.mp4"
    video = yt.streams.filter(file_extension='mp4').first()
    video.download(output_path=download_path, filename=file_name)
    print("Video downloaded successfully.")
    return os.path.join(download_path, file_name)


# Function to extract audio from video
def extract_audio(video_path, download_path):
    print(f"Extracting audio from {video_path}")
    video_clip = mp.VideoFileClip(video_path)
    audio_clip = video_clip.audio
    audio_file_name = os.path.basename(video_path).replace('.mp4', '.wav')
    audio_full_path = os.path.join(download_path, audio_file_name)
    audio_clip.write_audiofile(audio_full_path)
    audio_clip.close()
    print("Audio extracted successfully.")
    return audio_full_path


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
            video_url TEXT,
            video_path TEXT,
            audio_path TEXT,
            transcript TEXT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()


# Function to store data in database
def store_data(db_config, video_url, video_path, audio_path, transcript):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("USE youtube")
    cursor.execute("""
        INSERT INTO YoutubeData (video_url, video_path, audio_path, transcript)
        VALUES (%s, %s, %s, %s)
    """, (video_url, video_path, audio_path, transcript))
    conn.commit()
    cursor.close()
    conn.close()


# Function to read YouTube URLs from a file
def read_urls_from_file(file_path):
    with open(file_path, 'r') as file:
        urls = file.read().splitlines()
    return urls


# Main function
def main():
    youtube_urls_file = adjust_for_os("path_to_your_file.txt")  # Replace with your file path
    youtube_urls = read_urls_from_file(youtube_urls_file)

    download_path = adjust_for_os("E:/scratch")  # Replace with your download path

    db_config = {
        'host': 'localhost',
        'port': 4306,
        'user': 'newuser',
        'password': 'Rs232x25',
        'database': 'getyoutubevideos'
    }

    create_database_and_table(db_config)

    for video_url in youtube_urls:
        try:
            video_path = download_video(video_url, download_path)
            audio_full_path = extract_audio(video_path, download_path)

            segment_duration = 60
            overlap = 3
            total_duration = mp.VideoFileClip(video_path).duration

            full_segments = [(audio_full_path, i * segment_duration, segment_duration + overlap) for i in
                             range(int(total_duration / segment_duration))]
            three_second_segments = [(audio_full_path, i * segment_duration, overlap) for i in
                                     range(1, int(total_duration / segment_duration))]

            with Pool() as pool:
                full_transcriptions = pool.map(transcribe_audio, full_segments)
                three_second_transcriptions = pool.map(transcribe_audio, three_second_segments)

            corrected_transcripts = handle_missing_words(full_transcriptions, three_second_transcriptions, overlap)

            full_transcript = "\n".join(corrected_transcripts)
            store_data(db_config, video_url, video_path, audio_full_path, full_transcript)
        except Exception as e:
            print(f"Error processing {video_url}: {e}")


if __name__ == "__main__":
    main()
