import os
import argparse

import wget
import yaml


class Data():

    def __init__(self, grounds_list, sounds_list):

        self.abs_path = os.path.abspath(os.path.join(__file__, os.pardir))

        # Parse yaml file
        with open("data.yaml") as stream:
            try:
                self.get_data_yaml = yaml.load(stream)
            except yaml.YAMLError as e:
                print("Something is wrong with the data.yaml.")
                raise

        self.db_file = os.path.join(self.abs_path, self.get_data_yaml['db_name'])

        self.grounds_list = grounds_list or []
        self.sounds_list = sounds_list or []

        print("Args: {0}, {1}, {2}".format(self.db_file, self.grounds_list, self.sounds_list))

    def get_ground(self):
        print("Get ground station data...")
        from ground_station.txt_to_db import main as read_ground

        try:
            groundings = self.get_data_yaml['ground']
        except:
            groundings = []

        for ground in groundings:
            url = ground['url']
            name = ground['name']
            if name in self.grounds_list:
                path_to_parse = os.path.join(self.abs_path, 'ground_station', os.path.basename(url))
                print("Downloading {0} to {1} for {2}".format(url, path_to_parse, name))
                wget.download(url, out=path_to_parse)
                print("\n")
                read_ground(path_to_parse, self.db_file)

    def get_sounds(self):
        print("Get soundings data...")
        from soundings.txt_to_db import main as read_sound

        try:
            soundings = self.get_data_yaml['soundings']
        except:
            soundings = []

        for sound in soundings:
            url = sound['url']
            name = sound['name']
            if name in self.sounds_list:
                path_to_parse = os.path.join(self.abs_path, 'soundings', os.path.basename(url))
                print("Downloading {0} to {1} for {2}".format(url, path_to_parse, name))
                #wget.download(url, path_to_parse)
                print("\n")
                read_sound(path_to_parse, self.db_file)


def main(grounds_list=None, sounds_list=None):

    print("Enter data.py")
    print("Command line: grounds=[{0}], sounds=[{1}]".format(grounds_list, sounds_list))

    data = Data(grounds_list, sounds_list)

    data.get_ground()
    data.get_sounds()

    print("Exit data.py")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Refactor into subcommands
    parser.add_argument("--grounds", help="Comma list of ground_station short names to be downloaded.")
    parser.add_argument("--sounds", help="Comma list of soundings short names to be downloaded.")
    args = parser.parse_args()

    print(args)

    main(args.grounds, args.sounds)
