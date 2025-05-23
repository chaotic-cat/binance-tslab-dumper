# Variables
APP_NAME = binancedumper
GO = go
BUILD_DIR = ./bin
BINARY = $(BUILD_DIR)/$(APP_NAME)

# Default target
.PHONY: all
all: build

.PHONY: build-arm
build-arm:
	@echo "Building $(APP_NAME) for Linux x64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o $(BINARY)-linux-x64 .
	@echo "Binary built at $(BINARY)"

.PHONY: clean
clean:
	@echo "Cleaning up..."
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete"

.PHONY: release
release:
	goreleaser release --clean