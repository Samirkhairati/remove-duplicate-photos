from PIL import Image
from PIL.ExifTags import TAGS
import os
import datetime

def get_original_date(image_path):
    try:
        image = Image.open(image_path)
        exif_data = image._getexif()

        if exif_data:
            for tag, value in exif_data.items():
                tag_name = TAGS.get(tag, tag)
                if tag_name == "DateTimeOriginal":
                    return f"Original Date (EXIF): {value}"
        
        return "No EXIF Date Found"
    
    except Exception as e:
        return f"Error reading EXIF: {e}"

def get_file_timestamps(image_path):
    try:
        created_time = os.path.getctime(image_path)  # Creation time (may not be original)
        modified_time = os.path.getmtime(image_path)  # Last modified time
        return (
            f"File Created: {datetime.datetime.fromtimestamp(created_time)}\n"
            f"File Modified: {datetime.datetime.fromtimestamp(modified_time)}"
        )
    except Exception as e:
        return f"Error getting file timestamps: {e}"

def main(image_path):
    original_date = get_original_date(image_path)
    file_dates = get_file_timestamps(image_path)

    print(original_date)
    print(file_dates)

if __name__ == "__main__":
    # image_path = input("Enter the image file path: ").strip()
    # if os.path.exists(image_path):
    #     main("./test.jpeg")
    # else:
    #     print("File not found. Please check the path.")
    main("./test.jpg") 
