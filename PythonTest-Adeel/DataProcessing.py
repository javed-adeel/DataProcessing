import csv
from zipfile import ZipFile
import os.path
from os import path

class DataProcessing:
    def __init__(self, file="", path = ""):
        self.combined_data = {}
        if file:
            self.file = self.check_file(path, file)
        else:
            self.file = file

        if self.file:
            self.read_combined_file(self.file[0])

    def read_combined_file(self, file):
        if file:
            self.combined_data = self.read_file(file)
        else:
            print('Please pass a valid file')

        print('combined file: source_ip: ', self.combined_data.keys())
        print('combined env: ', self.combined_data.values())

    def read_file(self, file, header=True):
        """#Logic to read the new file"""
        print('processing file: ', file)

        result = {}

        with open(file, "r") as f:
            try:
                reader = csv.reader(f, delimiter=",")
                env = ((f.name).split('\\')[-1]).split('.')[0]
                env = ''.join([i for i in env if not i.isdigit()])
                print('env: ', env)
                if header:
                    next(reader)
                for row in reader:
                    if row:
                        result[str(row[0])] = env
            finally:
                f.close()
        return result

    def insert_data(self, file, data):
        with open(file[0], mode='a', newline='') as f:
            writer = csv.writer(f)
            for key, value in data.items():
                print('inserting ', key, ' --> ', value)
                writer.writerow([key, value])
        f.close()

    def check_file(self, path, file_name):
        result = []
        for root, dirs, files in os.walk(path):
            if file_name in files:
                result.append(os.path.join(root, file_name))
        print('result:', result)
        return result

    def get_all_csv(self, path, combined_file):
        result = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".csv"):
                    result.append(os.path.join(root, file))
        result = [s for s in result if str(combined_file) not in s]

        return result

    def process_file(self, current_path):
        print('Getting All CSV files')
        csv_list = self.get_all_csv(current_path, self.file)
        print('Found : ', len(csv_list), ' CSV files')
        print(csv_list)
        for file in csv_list:
            if (str(self.file).split('\\')[-1]).split('\'')[0] in file:
                csv_list.remove(file)

        print('Found : ', len(csv_list), ' CSV files')
        for file in csv_list:
            print('Processing file: ', file)
            result_new = self.read_file(file)
            print('List of source_ip: ', result_new.keys())
            print('List of env: ', result_new.values())

            updated_result = {}
            for key, value in result_new.items():
                if key not in self.combined_data.keys():
                    print(key, ' NOT In ', self.combined_data.keys(), ' -----> value: ', value)
                    updated_result[key] = value
                else:
                    print(key, ' FOUND in ', self.combined_data.keys())
            del result_new
            print('updated source_ip_new: ', updated_result.keys())
            print('updated env: ', updated_result.values())

            if updated_result:
                self.insert_data(self.file, updated_result)


def extract_zip_file(directory, file_name):
    file_name = directory + "\\" + file_name
    with ZipFile(file_name, 'r') as zip:
        # # printing all the contents of the zip file
        # zip.printdir()
        # extracting all the files
        print('Extracting all the files now...')
        zip.extractall()
        print('Done!')


def main():

    directory = 'c:\\My Docs\\TCS'

    file_name = 'Engineering Test.zip'
    current_path = os.path.dirname(os.path.realpath(__file__)) + '\\' + file_name.split(".")[0]
    combined_file = 'Combined.csv'

    extract_zip_file(directory, file_name)
    dp = DataProcessing(combined_file, current_path)

    combined_file = dp.check_file(current_path, combined_file)
    if combined_file:
        print(combined_file, ' exists')
    else:
        print(combined_file, ' does not exist')

    combined_data = dp.read_file(combined_file[0])
    print('combined file: source_ip: ', combined_data.keys())
    print('combined env: ', combined_data.values())

    dp.process_file(current_path)


if __name__ == "__main__":
    main()
