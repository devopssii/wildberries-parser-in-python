import json
from datetime import date
from os import path
import pandas as pd
import requests
from chromadb import ChromaDB
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

class WildBerriesParser:
    """
    A parser object for extracting data from wildberries.ru.

    Attributes:
        headers (dict): HTTP headers for the parser.
        run_date (datetime.date): The date when the parser is run.
        product_cards (list): A list to store the parsed product cards.
        directory (str): The directory path where the script is located.
        chroma_db (ChromaDB): An instance of ChromaDB for database operations.
    """

    def __init__(self):
        """
        Initialize a new instance of the WildBerriesParser class.

        This constructor sets up the parser object with default values
        for its attributes.

        Args:
            None

        Returns:
            None
        """
        self.headers = {'Accept': "*/*", 'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
        self.run_date = date.today()
        self.product_cards = []
        self.directory = path.dirname(path.abspath(__file__))
        self.chroma_db = ChromaDB()

    def download_current_catalogue(self):
        """
        Download the current catalogue from wildberries.ru.

        This function sends an HTTP GET request to wildberries.ru
        to download the current catalogue as a JSON file.

        Returns:
            dict: The parsed JSON catalogue.

        Raises:
            requests.exceptions.RequestException: If the GET request fails.
        """
        catalogue_url = 'https://search.wb.ru/catalog.json'
        response = requests.get(catalogue_url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def process_catalogue(self, catalogue):
        """
        Process the catalogue to extract the category names and URLs.

        This function receives the parsed catalogue as a dictionary
        and extracts the category names and URLs.

        Args:
            catalogue (dict): The parsed JSON catalogue.

        Returns:
            list: A list of dictionaries containing the category names and URLs.
        """
        flattened_catalogue = []

        def traverse_json(category, parent_name=""):
            if 'children' in category:
                for child in category['children']:
                    traverse_json(child, parent_name + category['name'] + ' -> ')
            else:
                flattened_catalogue.append({'name': parent_name + category['name'], 'url': category['url']})

        traverse_json(catalogue)

        return flattened_catalogue

    def parse_product_cards(self):
        """
        Parse the product cards for each category in the catalogue.

        This function loops through the categories in the catalogue,
        retrieves the product cards for each category, and stores them
        in the product_cards list.

        Returns:
            None
        """
        catalogue = self.process_catalogue(self.download_current_catalogue())
        for category in catalogue:
            category_name = category['name']
            category_url = category['url']
            product_cards_url = f'https://search.wb.ru/catalog/{category_url}'
            response = requests.get(product_cards_url, headers=self.headers).json()
            self.product_cards.extend(response['products'])

    def write_to_database(self):
        """
        Write the parsed product cards to the ChromaDB database.

        This function iterates over the product_cards list and inserts
        each product card into the ChromaDB database.

        Returns:
            None
        """
        for product_card in self.product_cards:
            self.chroma_db.insert_product_card(product_card)

    def write_to_excel(self):
        """
        Write the parsed product cards to an Excel file.

        This function creates an Excel workbook, adds a sheet, and writes
        the parsed product cards to the sheet.

        Returns:
            None
        """
        wb = Workbook()
        ws = wb.active

        df = pd.DataFrame(self.product_cards)
        for row in dataframe_to_rows(df, index=False, header=True):
            ws.append(row)

        excel_file_path = path.join(self.directory, 'wb_product_cards.xlsx')
        wb.save(excel_file_path)

        print(f"Product cards written to {excel_file_path}")

    def main(self):
        """
        The main function of the WildBerriesParser class.

        This function orchestrates the execution of the parser by calling
        the necessary methods in the correct order.

        Returns:
            None
        """
        self.parse_product_cards()
        self.write_to_database()
        self.write_to_excel()

if __name__ == "__main__":
    parser = WildBerriesParser()
    parser.main()
