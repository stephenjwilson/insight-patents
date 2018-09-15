"""
The main run script. Made for dev only on a local machine currently
"""
from download import download


def main():
    """
    The main body of the python part of the pipeline. Limited functionality right now.
    :return:
    """
    # TODO: run only if needed
    download(2011, 2011)


if __name__ == '__main__':
    main()
