CC = clang++
# Split into CFLAGS (compilation flags) and LDFLAGS (linker flags)
CFLAGS = -g -I$(shell brew --prefix raylib)/include -I$(shell brew --prefix glm)/include -Iinclude
LDFLAGS = -L$(shell brew --prefix raylib)/lib -lraylib -framework OpenGL -framework Cocoa -framework IOKit -framework CoreAudio -framework CoreVideo
TARGET = bin 

# Use wildcard to get all .cpp files
SOURCES = $(wildcard *.cpp)
# Create object files list
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

# Compile the target from object files
$(TARGET): $(OBJECTS)
	$(CC) $^ -o $@ $(LDFLAGS)

# Pattern rule for object files - use only CFLAGS here
%.o: %.cpp
	$(CC) -c $< -o $@ $(CFLAGS)

clean:
	$(RM) $(TARGET) $(OBJECTS)

