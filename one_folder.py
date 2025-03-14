import os
import hashlib
import shutil
import concurrent.futures
from PIL import Image
from pillow_heif import register_heif_opener
from tqdm import tqdm

# Register HEIF support (to handle HEIC files)
register_heif_opener()

def clean_up_videos(folder, video_output_folder):
    """Remove MP4s if a matching HEIC exists. Move standalone MP4s & MOVs to `videos/`."""
    heic_files = {os.path.splitext(entry.name)[0] for entry in os.scandir(folder) if entry.name.lower().endswith('.heic')}
    
    os.makedirs(video_output_folder, exist_ok=True)
    files = list(os.scandir(folder))  # Convert to list for tqdm

    for entry in tqdm(files, desc=f"🔍 Cleaning {folder}", unit="file"):
        file_path = entry.path
        file_base, file_ext = os.path.splitext(entry.name)

        if file_ext.lower() == ".mp4":
            if file_base in heic_files:
                os.remove(file_path)
            else:
                shutil.move(file_path, os.path.join(video_output_folder, entry.name))

        elif file_ext.lower() == ".mov":
            shutil.move(file_path, os.path.join(video_output_folder, entry.name))

def get_image_hash(image_path):
    """Compute a SHA-256 hash of an image's pixel data (ignoring metadata)."""
    try:
        with Image.open(image_path) as img:
            img = img.convert("RGB")
            return hashlib.sha256(img.tobytes()).hexdigest()
    except Exception:
        return None

def process_images(folder):
    """Scan & hash images with a progress bar (multithreading for speed)."""
    image_hashes = {}
    files = [entry for entry in os.scandir(folder) if entry.name.lower().endswith(('.heic', '.jpg', '.png'))]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(get_image_hash, entry.path): entry.path for entry in tqdm(files, desc=f"🔍 Hashing {folder}", unit="file")}

        for future in concurrent.futures.as_completed(futures):
            file_path = futures[future]
            img_hash = future.result()
            if img_hash:
                image_hashes[img_hash] = file_path

    return image_hashes

def copy_image(src, dest_folder):
    """Copy an image with progress tracking."""
    os.makedirs(dest_folder, exist_ok=True)
    shutil.copy2(src, os.path.join(dest_folder, os.path.basename(src)))

def remove_duplicates_and_store_unique(folder_c, output_folder):
    """Find duplicates within a single folder and store only unique images in `output2/`."""
    images_c = process_images(folder_c)

    folders = {
        "Unique": os.path.join(output_folder, "Unique"),
        "Videos": os.path.join(output_folder, "videos"),
    }

    for folder in folders.values():
        os.makedirs(folder, exist_ok=True)

    # Copy unique images
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for h in tqdm(images_c, desc="📂 Copying Unique Images", unit="file"):
            futures.append(executor.submit(copy_image, images_c[h], folders["Unique"]))

        for future in concurrent.futures.as_completed(futures):
            future.result()  # Wait for all tasks to complete

    print("\n✅ Duplicate removal complete! Unique images stored in `output2/`.")

if __name__ == "__main__":
    folder_c = "./C"
    output_folder = "output2"

    if os.path.exists(folder_c):
        print("\n🔍 Step 1: Cleaning up videos (MP4 & MOV files)...")
        clean_up_videos(folder_c, os.path.join(output_folder, "videos"))

        print("\n🔍 Step 2: Detecting duplicates and storing unique images...")
        remove_duplicates_and_store_unique(folder_c, output_folder)
    else:
        print("Error: Folder `C` does not exist.")
