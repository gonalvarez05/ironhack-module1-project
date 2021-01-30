
import argparse
import p_acquisition.m_acquisition as mac
import p_wrangling.m_wrangling as maw



def argument_parser():
    """
     parsear argumentos
    """

    parser = argparse.ArgumentParser(description='parse arguments')

    parser.add_argument("-p", "--path", help="specify the path of the database", type=str, required=True)

    args = parser.parse_args()

    return args


def main(arguments):
    print('Starting process...')

    path = arguments.path

    df_data, jobs_table, countries_table= mac.acquire(path)

    final_df= maw.final_wrangle(df_data, jobs_table, countries_table)

    print('Finished process...')


if __name__ == '__main__':
    arguments = argument_parser()
    main(arguments)










