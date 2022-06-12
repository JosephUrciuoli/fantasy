import click
from pprint import pprint
from stages.collect.golf import create_golf_dataset


@click.command()
@click.option("--run_collect", default=False, help="Run the collect stage.", type=bool)
@click.option(
    "--use_existing_csvs", default=False, help="Use existing csvs.", type=bool
)
def main(run_collect, use_existing_csvs):
    if run_collect:
        collect(use_existing_csvs)


def collect(use_existing_csvs: bool):
    create_golf_dataset(use_existing_csvs)


def model():
    pass


if __name__ == "__main__":
    main()
