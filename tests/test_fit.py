from endureio.fit import read_fit


def test_read_fit_running_file():
    file_path = "tests/data/short-run.fit"

    data = read_fit(file_path)

    assert "power" in data.columns
    assert len(data) == 1344