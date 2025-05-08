CC = clang++
CFLAGS = -g -std=c++17 -Wall
LDFLAGS = -pthread
TARGET = bin


SOURCES = $(wildcard *.cpp)

OBJECTS = $(SOURCES:.cpp=.o)


UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    LDFLAGS += -lrt
endif

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $^ -o $@ $(LDFLAGS)

%.o: %.cpp
	$(CC) -c $< -o $@ $(CFLAGS)

clean:
	$(RM) $(TARGET) $(OBJECTS)

.PHONY: all clean