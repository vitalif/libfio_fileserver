CXXFLAGS := -g -O3 -Wall -Wno-sign-compare -Wno-comment -Wno-parentheses -Wno-pointer-arith -fPIC -fdiagnostics-color=always
all: libfio_fileserver.so
clean:
	rm libfio_fileserver.so
libfio_fileserver.so: fio_fileserver.cpp
	g++ $(CXXFLAGS) -shared -o $@ $<
