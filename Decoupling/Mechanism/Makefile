CC = g++
CFLAGS = -pthread -std=c++17 -I..
LIBS = -lredis++ -lhiredis -lpaho-mqttpp3 -lpaho-mqtt3a -lpaho-mqtt3as

all: utils

utils: utils.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LIBS)

debug: CFLAGS += -g
debug: utils

clean:
	rm -f utils

.PHONY: debug clean