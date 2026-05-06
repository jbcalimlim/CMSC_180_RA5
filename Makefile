CC = gcc

LDFLAGS = -pthread -lm -lrt

TARGET = main
SRC = code.c

all:
	$(CC) $(SRC) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)