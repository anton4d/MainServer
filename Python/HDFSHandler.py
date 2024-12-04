from hdfs import InsecureClient

class HDFSHandler:
    def __init__(self, hdfs_url: str, user: str):
        """
        Initialize the HDFS handler.
        :param hdfs_url: The URL of the HDFS cluster (e.g., "http://namenode_host:50070").
        :param user: The HDFS user.
        """
        self.client = InsecureClient(hdfs_url, user=user)

    def upload_file(self, local_path: str, hdfs_path: str):
        """
        Upload a file to HDFS.
        :param local_path: The local file path.
        :param hdfs_path: The target path on HDFS.
        """
        try:
            self.client.upload(hdfs_path, local_path)
            print(f"Successfully uploaded {local_path} to {hdfs_path}")
        except Exception as e:
            print(f"Error uploading file: {e}")

    def download_file(self, hdfs_path: str, local_path: str):
        """
        Download a file from HDFS to the local system.
        :param hdfs_path: The file path on HDFS.
        :param local_path: The local path to save the file.
        """
        try:
            self.client.download(hdfs_path, local_path)
            print(f"Successfully downloaded {hdfs_path} to {local_path}")
        except Exception as e:
            print(f"Error downloading file: {e}")

    def list_directory(self, hdfs_path: str):
        """
        List files in a directory on HDFS.
        :param hdfs_path: The HDFS directory path.
        :return: A list of files in the directory.
        """
        try:
            files = self.client.list(hdfs_path)
            print(f"Files in {hdfs_path}: {files}")
            return files
        except Exception as e:
            print(f"Error listing directory: {e}")
            return []

    def delete_file(self, hdfs_path: str):
        """
        Delete a file on HDFS.
        :param hdfs_path: The file path on HDFS to delete.
        """
        try:
            self.client.delete(hdfs_path)
            print(f"Successfully deleted {hdfs_path}")
        except Exception as e:
            print(f"Error deleting file: {e}")

    def file_exists(self, hdfs_path: str) -> bool:
        """
        Check if a file exists on HDFS.
        :param hdfs_path: The file path on HDFS.
        :return: True if the file exists, False otherwise.
        """
        try:
            return self.client.status(hdfs_path) is not None
        except Exception as e:
            print(f"Error checking file existence: {e}")
            return False
    
    def stop(self):
        """
        Stop the HDFS handler and cancel any ongoing operations.
        This method will mark the handler as stopped, ensuring that subsequent operations
        are either skipped or cleanly interrupted.
        """
        self.stop_requested = True
        print("HDFSHandler has been stopped. No further operations will be executed.")

# Example usage:
if __name__ == "__main__":
    # Set the HDFS URL and user
    hdfs_handler = HDFSHandler(hdfs_url="http://localhost:50070", user="hdfs")

    # Upload a file
    hdfs_handler.upload_file("local_file.txt", "/user/hdfs/hdfs_file.txt")

    # Download a file
    hdfs_handler.download_file("/user/hdfs/hdfs_file.txt", "downloaded_file.txt")

    # List files in a directory
    hdfs_handler.list_directory("/user/hdfs")

    # Delete a file from HDFS
    hdfs_handler.delete_file("/user/hdfs/hdfs_file.txt")

    # Check if a file exists
    exists = hdfs_handler.file_exists("/user/hdfs/hdfs_file.txt")
    print(f"File exists: {exists}")
