CC = g++
CFLAGS = -Wall -Wextra -std=c++17 -pthread -O3

TARGET = tracker
SRCS = tracker.cpp ../common/sha1.cpp
OBJS = $(SRCS:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

%.o: %.cpp
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) $(OBJS) tracker*_state.txt tracker*_pending_sync.txt
