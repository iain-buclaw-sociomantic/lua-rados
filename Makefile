##### Build defaults #####
LUA_VERSION =       5.1
TARGET =            rados.so
PREFIX =            /usr/local
LIBDIR =            $(PREFIX)/lib
#CFLAGS =            -g -Wall -pedantic -fno-inline
CFLAGS =            -O3 -Wall -pedantic -DNDEBUG
RADOS_CFLAGS =      -fpic
RADOS_LDFLAGS =     -shared -lrados
LUA_INCLUDE_DIR =   $(PREFIX)/include
LUA_CMODULE_DIR =   $(LIBDIR)/lua/$(LUA_VERSION)

##### Platform overrides #####
##
## Tweak one of the platform sections below to suit your situation.
##
## See http://lua-users.org/wiki/BuildingModules for further platform
## specific details.

## Linux

## FreeBSD
#LUA_INCLUDE_DIR =   $(PREFIX)/include/lua51

## MacOSX (Macports)
#PREFIX =            /opt/local
#RADOS_LDFLAGS =     -bundle -undefined dynamic_lookup

## Solaris
#CC           =      gcc
#RADOS_CFLAGS =      -fpic -DUSE_INTERNAL_ISINF

## Windows (MinGW)
#TARGET =            rados.dll
#PREFIX =            /home/user/opt
#RADOS_CFLAGS =      -DDISABLE_INVALID_NUMBERS
#RADOS_LDFLAGS =     -shared -L$(PREFIX)/lib -llua51
#LUA_BIN_SUFFIX =    .lua

##### End customisable sections #####

EXECPERM =          755

BUILD_CFLAGS =      -I$(LUA_INCLUDE_DIR) $(RADOS_CFLAGS)
OBJS =              lua_rados.o

.PHONY: all clean install

.c.o:
	$(CC) -o $@ $< -c $(CFLAGS) $(CPPFLAGS) $(BUILD_CFLAGS)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS) $(RADOS_LDFLAGS)

install: $(TARGET)
	mkdir -p $(DESTDIR)/$(LUA_CMODULE_DIR)
	cp $(TARGET) $(DESTDIR)/$(LUA_CMODULE_DIR)
	chmod $(EXECPERM) $(DESTDIR)/$(LUA_CMODULE_DIR)/$(TARGET)

clean:
	rm -f *.o $(TARGET)
