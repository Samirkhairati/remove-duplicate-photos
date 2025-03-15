import os
import hashlib
import shutil
import concurrent.futures
from PIL import Image
from pillow_heif import register_heif_opener
from tqdm import tqdm

# Register HEIF support
register_heif_opener()

# Optimized thread count
MAX_THREADS = min(32, os.cpu_count() * 2)

def clean_up_videos(folder, video_output_folder):
    """Deletes MP4s if a matching HEIC exists & moves standalone MP4s/MOVs to videos/."""
    heic_files = {os.path.splitext(entry.name)[0] for entry in os.scandir(folder) if entry.name.lower().endswith('.heic')}
    
    os.makedirs(video_output_folder, exist_ok=True)
    files = list(os.scandir(folder))

    for idx, entry in enumerate(tqdm(files, desc=f"🔍 Cleaning {folder}", unit="file")):
        file_path = entry.path
        file_base, file_ext = os.path.splitext(entry.name)

        if file_ext.lower() == ".mp4":
            if file_base in heic_files:
                os.remove(file_path)
                print(f"[{idx+1}/{len(files)}] 🗑️ Deleted {entry.name} (HEIC exists)")
            else:
                shutil.move(file_path, os.path.join(video_output_folder, entry.name))
                print(f"[{idx+1}/{len(files)}] 📂 Moved {entry.name} → {video_output_folder}")

        elif file_ext.lower() == ".mov":
            shutil.move(file_path, os.path.join(video_output_folder, entry.name))
            print(f"[{idx+1}/{len(files)}] 📂 Moved {entry.name} → {video_output_folder}")

def get_image_hash(image_path):
    """Compute a SHA-256 hash of an image's pixel data (ignoring metadata)."""
    try:
        with Image.open(image_path) as img:
            img = img.convert("RGB")
            return hashlib.sha256(img.tobytes()).hexdigest()
    except Exception:
        return None

def process_images(folder):
    """Scans & hashes images with multithreading and progress tracking."""
    image_hashes = {}
    files = [entry for entry in os.scandir(folder) if entry.name.lower().endswith(('.heic', '.jpg', '.png'))]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(get_image_hash, entry.path): entry.path for entry in files}

        for idx, future in enumerate(tqdm(concurrent.futures.as_completed(futures), desc=f"🔍 Hashing {folder}", total=len(files), unit="file")):
            file_path = futures[future]
            img_hash = future.result()
            if img_hash:
                image_hashes[img_hash] = file_path
            print(f"[{idx+1}/{len(files)}] 🖼️ Hashed {os.path.basename(file_path)}")

    return image_hashes, len(files)

def copy_image(src, dest_folder, idx, total):
    """Copies an image with progress tracking."""
    os.makedirs(dest_folder, exist_ok=True)
    shutil.copy2(src, os.path.join(dest_folder, os.path.basename(src)))
    print(f"[{idx+1}/{total}] ✅ Copied {os.path.basename(src)} → {dest_folder}")

def remove_duplicates_and_store_unique(folder_c, output_folder):
    """Find duplicates within a single folder and store only unique images in `output2/`."""
    print("\n🔍 Step 2: Detecting duplicates and storing unique images...")
    
    images_c, total_files_before = process_images(folder_c)
    unique_files = len(images_c)
    duplicate_files = total_files_before - unique_files  # Duplicates found

    folders = {
        "Unique": os.path.join(output_folder, "Unique"),
        "Videos": os.path.join(output_folder, "videos"),
    }

    for folder in folders.values():
        os.makedirs(folder, exist_ok=True)

    files_to_copy = list(images_c.values())
    total_files_after = len(files_to_copy)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(copy_image, file_path, folders["Unique"], idx, total_files_after) for idx, file_path in enumerate(files_to_copy)}

        for future in concurrent.futures.as_completed(futures):
            future.result()  # Wait for all tasks to complete

    # Print summary
    print("\n✅ Duplicate removal complete!")
    print(f"📂 Total files before processing: {total_files_before}")
    print(f"📂 Total unique files after processing: {total_files_after}")
    print(f"❌ Total duplicate files removed: {duplicate_files}")

if __name__ == "__main__":
    folder_c = "./C"
    output_folder = "output2"

    if os.path.exists(folder_c):
        print("\n🔍 Step 1: Cleaning up videos (MP4 & MOV files)...")
        clean_up_videos(folder_c, os.path.join(output_folder, "videos"))

        remove_duplicates_and_store_unique(folder_c, output_folder)
    else:
        print("Error: Folder `C` does not exist.")
