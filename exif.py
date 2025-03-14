from PIL import Image
from PIL.ExifTags import TAGS

def get_exif_date(image_path):
    img = Image.open(image_path)
    exif_data = img._getexif()
    if not exif_data:
        return "No EXIF data found"

    for tag, value in exif_data.items():
        if TAGS.get(tag) == "DateTimeOriginal":
            return f"Original Date: {value}"

    return "No DateTimeOriginal found"

print(get_exif_date("./B/test.jpg"))
