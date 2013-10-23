%% Copyright
-module(server).
-author("Matthias Stellmann and Grzegorz Markiewicz").

%% API
-export([start/0]).

%% starting server and queueMgmt
start() ->
  {ok, ConfigList} = file:consult("server.cfg"),
  {ok, ClientLifeTime} = werkzeug:get_config_value(clientlifetime, ConfigList),
  {ok, ServerLifetime} = werkzeug:get_config_value(lifetime, ConfigList),
  {ok, DlqLimit} = werkzeug:get_config_value(dlqlimit, ConfigList),
  {ok, Servername} = werkzeug:get_config_value(servername, ConfigList),

  %% starting the QueueMgmt
  QueueMgmtPID = serverQueueMgmt:start(DlqLimit, ClientLifeTime),
  {ok, TimerRef} = timer:send_after(ServerLifetime, endOfLife),
  ServerPID = erlang:spawn(fun() -> dispatcher(1, ServerLifetime, TimerRef, DlqLimit, QueueMgmtPID) end),
  register(Servername, ServerPID),
  {ServerPID, node(ServerPID)}
.

%% receive loop
dispatcher(MessageCounter, ServerLifetime, ServerLifetimeTimer, DLQLimit, QueueMgmtPID) ->
  receive
  %% if endOfLife is received, the server exits
    endOfLife -> exitServer(QueueMgmtPID);

  %% request of clients: next messagenumber
    {getmsgid, ClientPID} ->
      werkzeug:reset_timer(ServerLifetimeTimer, ServerLifetime, endOfLife),
      messagenumberMgmt(ClientPID, MessageCounter, ServerLifetime, ServerLifetimeTimer, DLQLimit, QueueMgmtPID);

  %% request of clients: dropping a new message
    {dropmessage, {Message, Number}} ->
      io:format("Message: ~p ---- Number: ~p~n", [Message, Number]),

      werkzeug:reset_timer(ServerLifetimeTimer, ServerLifetime, endOfLife),
      QueueMgmtPID ! {dropmessage, {Message, Number}},
      dispatcher(MessageCounter, ServerLifetime, ServerLifetimeTimer, DLQLimit, QueueMgmtPID);

  %% request of clients: getting messages
    {getmessages, ClientPID} ->
      werkzeug:reset_timer(ServerLifetimeTimer, ServerLifetime, endOfLife),
      QueueMgmtPID ! {getmessages, ClientPID},
      dispatcher(MessageCounter, ServerLifetime, ServerLifetimeTimer, DLQLimit, QueueMgmtPID);

  %% something unknown received
    Any ->
      logToFile(lists:concat(["received anything not understandable: ", Any, "~n"])),
      dispatcher(MessageCounter, ServerLifetime, ServerLifetimeTimer, DLQLimit, QueueMgmtPID)
  end
.

%% sends the next msg number to the editor client
messagenumberMgmt(ClientPID, MessageCounter, ServerLifetime, ServerLifetimeTimer, DLQLimit, QueueMgmtPID) ->
  ClientPID ! {nid, MessageCounter},
  dispatcher(MessageCounter + 1, ServerLifetime, ServerLifetimeTimer, DLQLimit, QueueMgmtPID)
.

%% terminates the server and sends the queueMngt process the endOfLife msg
exitServer(QueueMgmtPID) ->
  io:format("INFO: Server with PID: ~p ended at ~p~n", [self(), werkzeug:timeMilliSecond()]),
  QueueMgmtPID ! endOfLife,
  erlang:exit("EndOfLife")
.


%% -- helping classes --
getHostname() ->
  {ok, Hostname} = inet:gethostname(),
  Hostname
.

logToFile(Message) ->
  Filename = lists:concat(["Server: ", "@", getHostname(), ".log"]),
  werkzeug:logging(Filename, lists:concat(["[", werkzeug:timeMilliSecond(), "] ", ": ", Message, "\n"]))
.