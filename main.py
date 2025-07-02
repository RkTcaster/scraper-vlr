import json
from functions import link_extractor, process_match

def load_json(path):
    with open(path) as json_file:
        config = json.load(json_file)
    return config

def main():
    config = load_json("config.json")
    folder = config["folder"]
    encoding = config["encoding"]
    processed_url = config["processed_url"]
    urls = []

    for key in config["url"].keys():
        for url in config["url"][key]:
            urls.append(url)

    for matches_page_url in urls:
        if matches_page_url not in processed_url:
            print(matches_page_url)
            matches_links = link_extractor(matches_page_url)
            for match_url in matches_links:
                process_match(match_url, folder, encoding)

            config["processed_url"].append(matches_page_url)
            with open("config.json", "w") as outfile:
                json.dump(config, outfile,indent=4)

    print("Done processing")

main()

