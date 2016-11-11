
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -W -Wall -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

JSON_PATH = thirdparty/json-c
JSON_LIB = $(JSON_PATH)/.libs
CFLAGS += -I$(JSON_PATH)
SHOBJ_LDFLAGS += -L$(JSON_LIB) 
LIBS += -ljson-c

.SUFFIXES: .c .so .xo .o

all: reji.so

$(JSON_LIB)/libjson-c.a:
	cd $(JSON_PATH); ./autogen.sh; ./configure; make; cd -

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

reji_main.xo: ../redismodule.h

reji.so: $(JSON_LIB)/libjson-c.a reji_main.xo
	$(LD) -o $@ reji_main.xo $(SHOBJ_LDFLAGS) $(LIBS) -lc

clean:
	rm -rf *.xo *.so
