import os

directory = "history_log"

if os.access(directory, os.W_OK):
    print(f"You have write permission on the directory '{directory}'.")
else:
    print(f"You do not have write permission on the directory '{directory}'.")
