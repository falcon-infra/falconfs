# contrib/falcon/Makefile

PG_CFLAGS = -Wno-declaration-after-statement

MODULE_big = falcon
SUBDIRS = . metadb utils distributed_backend transaction control dir_path_shmem connection_pool plugin

falcon_srcdir = .
CXX_OBJS += \
	$(patsubst $(falcon_srcdir)/%.cpp,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(falcon_srcdir)/$(dir)/*.cpp))))

# Plugin sources are included in SUBDIRS above
OBJS += \
	$(patsubst $(falcon_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(falcon_srcdir)/$(dir)/*.c))))
OBJS += $(CXX_OBJS)

REMOTE_CONNECTION_DEF_DIR = $(falcon_srcdir)/../../../../remote_connection_def
CONNECTION_POOL_DIR = $(falcon_srcdir)/connection_pool

PROTO_DIR = $(shell find $(REMOTE_CONNECTION_DEF_DIR) -type d | grep -E '/proto$$')
PROTO_FILE = $(wildcard $(PROTO_DIR)/*.proto)
PROTO_SRC = $(patsubst $(REMOTE_CONNECTION_DEF_DIR)/%.proto, $(CONNECTION_POOL_DIR)/%.pb.cc, $(PROTO_FILE))
PROTO_HEAD = $(patsubst %.cc, %.h, $(PROTO_SRC))
PROTO_OBJS = $(patsubst %.cc, %.o, $(PROTO_SRC))
OBJS += $(PROTO_OBJS)

FBS_DIR = $(shell find $(REMOTE_CONNECTION_DEF_DIR) -type d | grep -E '/fbs$$')
FBS_FILE = $(wildcard $(FBS_DIR)/*.fbs)
FBS_HEAD = $(patsubst $(REMOTE_CONNECTION_DEF_DIR)/%.fbs, $(CONNECTION_POOL_DIR)/%_generated.h, $(FBS_FILE))

all: $(FBS_HEAD) $(PROTO_OBJS) $(OBJS)

$(OBJS): $(FBS_HEAD) $(PROTO_OBJS)

$(CXX_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -faligned-new -c $< -o $@


$(PROTO_SRC): $(PROTO_FILE)
	mkdir -p $(CONNECTION_POOL_DIR)/proto
	protoc --cpp_out=$(CONNECTION_POOL_DIR)/proto/ -I $(PROTO_DIR) $(notdir $(PROTO_FILE))

$(FBS_HEAD): $(FBS_FILE)
	mkdir -p $(CONNECTION_POOL_DIR)/fbs
	flatc --cpp -o $(CONNECTION_POOL_DIR)/fbs/ $(FBS_FILE)


PGFILEDESC = "falcon"

LDFLAGS_PROTO = -Wl,-Bdynamic -lprotobuf
LDFLAGS_BRPC = -lbrpc
LDFLAGS_DEPENDENCIES = -L../../../../falcon/third_party/glog/lib -lpthread -lgflags -lleveldb -lcrypto -lssl -ldl -lz -lglog

PG_CPPFLAGS = -I$(libpq_srcdir) -I./include -I./connection_pool/proto -I./connection_pool/fbs \
			  -I$(REMOTE_CONNECTION_DEF_DIR) \
			  -I/usr/local/include \
			  -I$(falcon_srcdir)/../../../../falcon/third_party/glog/include
SHLIB_LINK_INTERNAL =  -L../../src/interfaces/libpq -Wl,--no-as-needed -lpq -lstdc++ \
					-lz -Wl,--as-needed \
					   $(LDFLAGS_BRPC) $(LDFLAGS_PROTO) $(LDFLAGS_DEPENDENCIES)

EXTENSION = falcon
DATA = falcon--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/falcon
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
