build:
	git submodule update --init --recursive
	sass static/styles.sass static/styles.css --style compressed
