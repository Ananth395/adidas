from adidas.etl.raw_data import RawData
import click


@click.command()
@click.option("--url", help="url of the file to be downloaded.")
def get_raw_data(url):
    RawData(url=url).download()


if __name__ == "__main__":
    get_raw_data()
