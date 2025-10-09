import os

class FileManager:
    def __init__(self, base_dir, lakehouse_s3_path):
        self.base_dir = base_dir
        self.lakehouse_s3_path = lakehouse_s3_path

    def get_local_file_path(self, filename, filetype):
        full_filename = f"{filename}.{filetype}"
        return os.path.join(self.base_dir, full_filename)

    def get_s3_path(self, business_unit, dataset_name):
        return os.path.join(self.base_dir, business_unit, dataset_name)
