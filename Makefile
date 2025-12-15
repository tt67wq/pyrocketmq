PYTHON=python3
TWINE=twine
UV=uv
UVX=uvx
PYTHON_PATH=src/pyrocketmq
DIST_DIR=dist
BUILD_DIR=build
REMOTE_HUB=Python
PYTHON_FILES=$(sort $(wildcard $(PYTHON_PATH)/*.py))


# all: clean check-tw build upload
all: help

help:
	@echo "make build - build the package"
	@echo "make upload - upload the package to registry"
	@echo "make clean - clean the build directory"
	@echo "make clean-dist - clean the dist directory"
	@echo "make help - show"
	@echo "make release - clean, check-tw, build, upload"

release: clean check-tw build upload

build: $(PYTHON_FILES)
	$(UV) build
	@echo "Build complete."

upload: $(DIST_DIR)
	$(UVX) $(TWINE) upload -r $(REMOTE_HUB) dist/*

$(DIST_DIR): $(PYTHON_FILES)
$(BUILD_DIR): $(PYTHON_FILES)


check-tw:
	@hash twine 2>/dev/null || ( \
	echo '`twine` seem not to be installed or in your PATH.' && \
	echo 'Maybe you need to install it first.' && \
	exit 1)

clean-dist:
	rm -rf $(DIST_DIR)

clean-build:
	rm -rf $(BUILD_DIR)

clean: clean-dist clean-build


.PHONY: upload check-tw clean-dist clean-build
