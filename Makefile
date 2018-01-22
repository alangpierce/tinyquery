build:
	python setup.py sdist        # builds source distribution
	python setup.py bdist_wheel  # builds wheel

release: build
	pip install -U twine   # TODO(benkraft): dev-only requirements file?
	twine upload dist/*
