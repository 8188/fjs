TARGET = utils
BUILD_DIR = build

INC = /usr/local/include/python3.9
LIBS = /usr/local/bin
LIB = python3.9

NUMPY_INCLUDE = /usr/local/lib/python3.9/site-packages/numpy/core/include

all: clean init $(BUILD_DIR)/$(TARGET).so

$(BUILD_DIR)/$(TARGET).so: $(BUILD_DIR)/$(TARGET).o
	gcc -shared -o $(BUILD_DIR)/$(TARGET).so $(BUILD_DIR)/$(TARGET).o -L $(LIBS) -l $(LIB)

$(BUILD_DIR)/$(TARGET).o: $(BUILD_DIR)/$(TARGET).c
	gcc -O3 -o $(BUILD_DIR)/$(TARGET).o -c $(BUILD_DIR)/$(TARGET).c -I $(INC) -I $(NUMPY_INCLUDE) -fPIC

$(BUILD_DIR)/$(TARGET).c: $(TARGET).pyx
	cython -3 $(TARGET).pyx -o $(BUILD_DIR)/$(TARGET).c

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

.PHONY: init
init:
	@if [ ! -d "$(BUILD_DIR)" ]; then mkdir -p $(BUILD_DIR); fi