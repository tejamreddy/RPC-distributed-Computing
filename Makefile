CC=gcc
CLIENT = client
SERVER = server

SOURCES_CLNT.c = 
SOURCES_CLNT.h = 
SOURCES_SVC.c = 
SOURCES_SVC.h = 
SOURCES.x = replicator.x

TARGETS_SVC.c = replicator_svc.c server.c replicator_xdr.c
TARGETS_CLNT.c = replicator_clnt.c client.c replicator_xdr.c
TARGETS = replicator.h replicator_clnt.c replicator_svc.c replicator_xdr.c

OBJECTS_CLNT = $(SOURCES_CLNT.c:%.c=%.o) $(TARGETS_CLNT.c:%.c=%.o)
OBJECTS_SVC = $(SOURCES_SVC.c:%.c=%.o) $(TARGETS_SVC.c:%.c=%.o)

CFLAGS += -O2 -Wno-unused-result
LDLIBS +=
RPCGENFLAGS =

all : $(CLIENT) $(SERVER)

$(TARGETS) : $(SOURCES.x) 
	rpcgen $(RPCGENFLAGS) $(SOURCES.x)

$(OBJECTS_CLNT) : $(SOURCES_CLNT.c) $(SOURCES_CLNT.h) $(TARGETS_CLNT.c) 

$(OBJECTS_SVC) : $(SOURCES_SVC.c) $(SOURCES_SVC.h) $(TARGETS_SVC.c) 

$(CLIENT) : $(OBJECTS_CLNT) 
	$(CC) -o $(CLIENT) $(OBJECTS_CLNT) $(LDLIBS) 

$(SERVER) : $(OBJECTS_SVC) 
	$(CC) -o $(SERVER) $(OBJECTS_SVC) $(LDLIBS)

 clean:
	 $(RM) $(TARGETS) $(OBJECTS_SVC) $(CLIENT) $(SERVER) *.o
