import os
import hashlib
import shutil
import concurrent.futures
from PIL import Image
from pillow_heif import register_heif_opener
from tqdm import tqdm  # Import tqdm for progress bars

# Register HEIF support (to handle HEIC files)
register_heif_opener()

def clean_up_videos(folder, video_output_folder):
    """Delete MP4s if a HEIC exists, move standalone MP4s & MOVs to 'videos/'"""
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

def compare_images_and_sort(folder_a, folder_b, output_folder):
    """Find matching and non-matching images between two folders and sort them."""
    images_a = process_images(folder_a)
    images_b = process_images(folder_b)

    intersection = {h for h in images_a if h in images_b}
    only_in_a = {h for h in images_a if h not in images_b}
    only_in_b = {h for h in images_b if h not in images_a}
    union = images_a.keys() | images_b.keys()

    folders = {
        "A-B": os.path.join(output_folder, "A-B"),
        "B-A": os.path.join(output_folder, "B-A"),
        "A∩B": os.path.join(output_folder, "A_intersection_B"),
        "A∪B": os.path.join(output_folder, "A_union_B"),
        "Videos": os.path.join(output_folder, "A_intersection_B", "videos"),
    }

    for folder in folders.values():
        os.makedirs(folder, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for h in tqdm(only_in_a, desc="📂 Copying A-B", unit="file"):
            futures.append(executor.submit(copy_image, images_a[h], folders["A-B"]))

        for h in tqdm(only_in_b, desc="📂 Copying B-A", unit="file"):
            futures.append(executor.submit(copy_image, images_b[h], folders["B-A"]))

        for h in tqdm(intersection, desc="📂 Copying A∩B", unit="file"):
            futures.append(executor.submit(copy_image, images_a[h], folders["A∩B"]))

        for h in tqdm(union, desc="📂 Copying A∪B", unit="file"):
            if h in images_a:
                futures.append(executor.submit(copy_image, images_a[h], folders["A∪B"]))
            if h in images_b and h not in images_a:
                futures.append(executor.submit(copy_image, images_b[h], folders["A∪B"]))

        for future in concurrent.futures.as_completed(futures):
            future.result()  # Wait for all tasks to complete

    print("\n✅ Sorting complete!")

if __name__ == "__main__":
    folder_a = "./A"
    folder_b = "./B"
    output_folder = "output"

    if os.path.exists(folder_a) and os.path.exists(folder_b):
        print("\n🔍 Step 1: Cleaning up videos (MP4 & MOV files)...")
        clean_up_videos(folder_a, os.path.join(output_folder, "A_intersection_B", "videos"))
        clean_up_videos(folder_b, os.path.join(output_folder, "A_intersection_B", "videos"))

        print("\n🔍 Step 2: Comparing and categorizing images...")
        compare_images_and_sort(folder_a, folder_b, output_folder)
    else:
        print("Error: One or both folders do not exist.")
