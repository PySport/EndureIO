.PHONY: test, build, publish

test:
	uv run python -m pytest ${pytestargs}

build:
	rm -rf dist
	uvx --from build pyproject-build --installer uv

publish: build
	uvx twine upload dist/*