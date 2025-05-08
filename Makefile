CC = clang++
CFLAGS = -g -std=c++17 -Iinclude
LDFLAGS = -lraylib
TARGET = bin

# Detect operating system
UNAME_S := $(shell uname -s)

# Mac-specific settings
ifeq ($(UNAME_S),Darwin)
    # Use homebrew paths if available
    BREW_PREFIX := $(shell command -v brew >/dev/null 2>&1 && brew --prefix || echo "/usr/local")
    CFLAGS += -I$(BREW_PREFIX)/include -I$(BREW_PREFIX)/include/raylib
    LDFLAGS += -L$(BREW_PREFIX)/lib -framework OpenGL -framework Cocoa -framework IOKit -framework CoreAudio -framework CoreVideo
endif

# Linux-specific settings
ifeq ($(UNAME_S),Linux)
    CFLAGS += -I/usr/include -I/usr/local/include
    LDFLAGS += -L/usr/lib -L/usr/local/lib -lGL -lm -lpthread -ldl -lrt -lX11
endif

# Use wildcard to get all .cpp files
SOURCES = $(wildcard *.cpp)
# Create object files list
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

# Compile the target from object files
$(TARGET): $(OBJECTS)
	$(CC) $^ -o $@ $(LDFLAGS)

# Pattern rule for object files
%.o: %.cpp
	$(CC) -c $< -o $@ $(CFLAGS)

clean:
	$(RM) $(TARGET) $(OBJECTS)

.PHONY: all clean