CC = g++
CFLAGS = -pthread
LIBS = -lredis++ -lhiredis -lpaho-mqttpp3 -lpaho-mqtt3a

all: utils

utils: utils.cpp
	$(CC) $(CFLAGS) $^ -o $@ $(LIBS)

debug: CFLAGS += -g
debug: utils

clean:
	rm -f utils

.PHONY: debug