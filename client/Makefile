CC = g++
CFLAGS = -Wall -Wextra -std=c++17 -pthread -O3

TARGET = client
SRCS = client.cpp ../common/sha1.cpp
OBJS = $(SRCS:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

%.o: %.cpp
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) $(OBJS) client_*_files.txt
