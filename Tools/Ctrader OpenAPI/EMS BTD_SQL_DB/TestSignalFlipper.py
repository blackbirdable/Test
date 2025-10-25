import os

# File path to the TEST_SIGNAL.py
file_path = r'C:\Users\Administrator\Desktop\Sonixen\Logs\TEST_SIGNAL.py'

def switch_action_in_signal(file_path):
    """Reads the signal file and switches the Action between BUY and SELL."""
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    # Read the contents of the file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Identify and modify the 'Action' line
    modified_lines = []
    for line in lines:
        if line.startswith("Action"):
            if "'BUY'" in line:
                modified_line = line.replace("'BUY'", "'SELL'")
                print("Changing Action from BUY to SELL")
            elif "'SELL'" in line:
                modified_line = line.replace("'SELL'", "'BUY'")
                print("Changing Action from SELL to BUY")
            else:
                modified_line = line
            modified_lines.append(modified_line)
        else:
            modified_lines.append(line)

    # Write the modified content back to the file
    with open(file_path, 'w') as file:
        file.writelines(modified_lines)

    print("Action has been updated successfully.")

if __name__ == "__main__":
    switch_action_in_signal(file_path)
