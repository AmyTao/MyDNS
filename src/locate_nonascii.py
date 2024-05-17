import os

def scan_directory(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.go', '.sh', '.py')):
                filepath = os.path.join(root, file)
                check_file(filepath)

def check_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            found = False
            for line_number, line in enumerate(lines, start=1):
                if not line.isascii():
                    if found:
                        print("    (More non-ASCII lines found...)\n")
                        break
                    found = True
                    print(f"Non-ASCII character found in {filepath} at line {line_number}")
                    print(line, end='')
                    
    except Exception as e:
        print(f"Error reading {filepath}: {e}")

if __name__ == "__main__":
    directory_to_scan = input("Enter the directory path to scan: ")
    scan_directory(directory_to_scan)
