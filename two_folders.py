import os
import hashlib
import shutil
import concurrent.futures
from PIL import Image
from pillow_heif import register_heif_opener
from tqdm import tqdm

# Register HEIF support
register_heif_opener()

# Optimized thread count: Uses min(32, CPU cores * 2)
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

    return image_hashes

def copy_image(src, dest_folder, idx, total):
    """Copies an image with progress tracking."""
    os.makedirs(dest_folder, exist_ok=True)
    shutil.copy2(src, os.path.join(dest_folder, os.path.basename(src)))
    print(f"[{idx+1}/{total}] ✅ Copied {os.path.basename(src)} → {dest_folder}")

def compare_images_and_sort(folder_a, folder_b, output_folder):
    """Finds matching & unique images between two folders & sorts them."""
    print("\n🔍 Hashing images in Folder A...")
    images_a = process_images(folder_a)
    
    print("\n🔍 Hashing images in Folder B...")
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

    tasks = [
        (only_in_a, images_a, folders["A-B"]),
        (only_in_b, images_b, folders["B-A"]),
        (intersection, images_a, folders["A∩B"]),
        (union, {**images_a, **images_b}, folders["A∪B"]),
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for label, dataset, target_folder in tasks:
            futures = {executor.submit(copy_image, dataset[h], target_folder, idx, len(dataset)) for idx, h in enumerate(label)}
            for future in concurrent.futures.as_completed(futures):
                future.result()

    print("\n✅ Sorting complete! Images have been copied.")

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
