CC = g++
CFLAGS = -pthread -std=c++17 -I..
LIBS = -lredis++ -lhiredis -lpaho-mqttpp3

MQTT_LIB = $(shell ./detect_mqtt.sh)

all: utils

utils: utils.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LIBS) $(MQTT_LIB)

debug: CFLAGS += -g
debug: utils

clean:
	rm -f utils

.PHONY: debug clean