
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -W -Wall -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_CPPFLAGS ?= -W -Wall -fno-common -g -ggdb -O2
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_CPPFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -O2
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

JSON_PATH = thirdparty/json-c
JSON_LIB = $(JSON_PATH)/.libs
CFLAGS += -I$(JSON_PATH)
CPPFLAGS += -I$(JSON_PATH)
SHOBJ_LDFLAGS += -L$(JSON_LIB) 
LIBS += -ljson-c

SOURCEDIR = .
CC_SOURCES = $(wildcard $(SOURCEDIR)/*.cc)
CC_OBJECTS = $(patsubst $(SOURCEDIR)/%.cc, $(SOURCEDIR)/%.xo, $(CC_SOURCES))

.SUFFIXES: .c .cc .so .xo .o

all: reji.so

$(JSON_LIB)/libjson-c.a:
	cd $(JSON_PATH); ./autogen.sh; ./configure --with-pic; make; cd -

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

$(SOURCEDIR)/%.xo: $(SOURCEDIR)/%.cc
	g++ -I. $(CPPFLAGS) $(SHOBJ_CPPFLAGS) -fPIC -c $< -o $@

reji_main.xo: ../redismodule.h

reji.so: $(JSON_LIB)/libjson-c.a reji_main.xo $(CC_OBJECTS)
	$(LD) -o $@ reji_main.xo $(CC_OBJECTS) $(JSON_LIB)/libjson-c.a $(SHOBJ_LDFLAGS) -lc

clean:
	rm -rf *.xo *.so
