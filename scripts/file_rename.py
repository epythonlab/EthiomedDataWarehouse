import os

# Directory containing the images you want to rename
directory = '../data/val/images'

# Loop through each file in the directory
for filename in os.listdir(directory):
    if filename.endswith(('.jpg', '.png')):
        # Store the modified filename separately
        modified_filename = filename
        
        # Replace the leading hyphen '-' with '@' if present
        if modified_filename.startswith('-'):
            modified_filename = '@' + modified_filename[1:]
        
        # Find the position of '_jpg' in the modified filename
        position = modified_filename.find('_jpg')
        
        # If '_jpg' is found, create the new filename up to that position
        if position != -1:
            new_filename = modified_filename[:position] + '.jpg'
            
            # Get full paths
            old_path = os.path.join(directory, filename)
            new_path = os.path.join(directory, new_filename)
            
            # Rename the file
            os.rename(old_path, new_path)

print("Files have been renamed successfully!")