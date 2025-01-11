from endureio.fit import read_fit


def test_read_fit_running_file():
    file_path = "tests/data/running.fit"

    data = read_fit(file_path)